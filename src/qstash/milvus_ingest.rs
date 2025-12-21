use crate::app_state::AppState;
use crate::milvus::{self, Client as MilvusClient};
use anyhow::{Context, Result};
use axum::{extract::State, http::StatusCode, Json};
use futures::stream::{self, StreamExt};
use google_cloud_bigquery::http::job::get_query_results::GetQueryResultsRequest;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::http::tabledata::insert_all::{InsertAllRequest, Row};
use google_cloud_bigquery::http::tabledata::list::Value;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use tracing::instrument;
use utoipa::ToSchema;

/// Performance metrics for tracking operation timings
#[cfg(not(feature = "local-bin"))]
#[derive(Debug, Default)]
struct OperationMetrics {
    redis_check_times: Vec<u128>,     // microseconds
    milvus_search_times: Vec<u128>,   // microseconds
    milvus_insert_times: Vec<u128>,   // microseconds
    redis_insert_times: Vec<u128>,    // microseconds
    bigquery_insert_times: Vec<u128>, // microseconds
}

/// Metrics collector that can be enabled or disabled to avoid memory bloat
#[cfg(not(feature = "local-bin"))]
#[derive(Debug)]
enum MetricsCollector {
    Enabled(OperationMetrics),
    Disabled,
}

#[cfg(not(feature = "local-bin"))]
impl MetricsCollector {
    fn record_redis_check(&mut self, duration: u128) {
        if let MetricsCollector::Enabled(m) = self {
            m.redis_check_times.push(duration);
        }
    }

    fn record_milvus_search(&mut self, duration: u128) {
        if let MetricsCollector::Enabled(m) = self {
            m.milvus_search_times.push(duration);
        }
    }

    fn record_milvus_insert(&mut self, duration: u128) {
        if let MetricsCollector::Enabled(m) = self {
            m.milvus_insert_times.push(duration);
        }
    }

    fn record_redis_insert(&mut self, duration: u128) {
        if let MetricsCollector::Enabled(m) = self {
            m.redis_insert_times.push(duration);
        }
    }

    fn record_bigquery_insert(&mut self, duration: u128) {
        if let MetricsCollector::Enabled(m) = self {
            m.bigquery_insert_times.push(duration);
        }
    }

    fn merge(&mut self, other: MetricsCollector) {
        if let (MetricsCollector::Enabled(m1), MetricsCollector::Enabled(m2)) = (self, other) {
            m1.redis_check_times.extend(m2.redis_check_times);
            m1.milvus_search_times.extend(m2.milvus_search_times);
            m1.milvus_insert_times.extend(m2.milvus_insert_times);
            m1.redis_insert_times.extend(m2.redis_insert_times);
            m1.bigquery_insert_times.extend(m2.bigquery_insert_times);
        }
    }

    fn log_summary(&self) {
        if let MetricsCollector::Enabled(m) = self {
            m.log_summary();
        }
    }
}

#[cfg(not(feature = "local-bin"))]
impl OperationMetrics {
    fn log_summary(&self) {
        log::info!("📊 Performance Metrics (all times in microseconds):");

        if !self.redis_check_times.is_empty() {
            let total: u128 = self.redis_check_times.iter().sum();
            let avg = total / self.redis_check_times.len() as u128;
            let min = *self.redis_check_times.iter().min().unwrap();
            let max = *self.redis_check_times.iter().max().unwrap();
            log::info!(
                "  Redis Check: count={}, total={}µs ({:.2}ms), avg={}µs, min={}µs, max={}µs",
                self.redis_check_times.len(),
                total,
                total as f64 / 1000.0,
                avg,
                min,
                max
            );
        }

        if !self.milvus_search_times.is_empty() {
            let total: u128 = self.milvus_search_times.iter().sum();
            let avg = total / self.milvus_search_times.len() as u128;
            let min = *self.milvus_search_times.iter().min().unwrap();
            let max = *self.milvus_search_times.iter().max().unwrap();
            log::info!(
                "  Milvus Search: count={}, total={}µs ({:.2}ms), avg={}µs, min={}µs, max={}µs",
                self.milvus_search_times.len(),
                total,
                total as f64 / 1000.0,
                avg,
                min,
                max
            );
        }

        if !self.milvus_insert_times.is_empty() {
            let total: u128 = self.milvus_insert_times.iter().sum();
            let avg = total / self.milvus_insert_times.len() as u128;
            let min = *self.milvus_insert_times.iter().min().unwrap();
            let max = *self.milvus_insert_times.iter().max().unwrap();
            log::info!(
                "  Milvus Insert: count={}, total={}µs ({:.2}ms), avg={}µs, min={}µs, max={}µs",
                self.milvus_insert_times.len(),
                total,
                total as f64 / 1000.0,
                avg,
                min,
                max
            );
        }

        if !self.redis_insert_times.is_empty() {
            let total: u128 = self.redis_insert_times.iter().sum();
            let avg = total / self.redis_insert_times.len() as u128;
            let min = *self.redis_insert_times.iter().min().unwrap();
            let max = *self.redis_insert_times.iter().max().unwrap();
            log::info!(
                "  Redis Insert: count={}, total={}µs ({:.2}ms), avg={}µs, min={}µs, max={}µs",
                self.redis_insert_times.len(),
                total,
                total as f64 / 1000.0,
                avg,
                min,
                max
            );
        }

        if !self.bigquery_insert_times.is_empty() {
            let total: u128 = self.bigquery_insert_times.iter().sum();
            let avg = total / self.bigquery_insert_times.len() as u128;
            let min = *self.bigquery_insert_times.iter().min().unwrap();
            let max = *self.bigquery_insert_times.iter().max().unwrap();
            log::info!(
                "  BigQuery Insert: count={}, total={}µs ({:.2}ms), avg={}µs, min={}µs, max={}µs",
                self.bigquery_insert_times.len(),
                total,
                total as f64 / 1000.0,
                avg,
                min,
                max
            );
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct IngestPhashRequest {
    #[serde(default = "default_limit")]
    pub limit: u32,

    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    #[serde(default = "default_collect_metrics")]
    pub collect_metrics: bool,
}

fn default_limit() -> u32 {
    1000
}

fn default_concurrency() -> usize {
    10 // Process 10 videos concurrently by default
}

fn default_collect_metrics() -> bool {
    false // Disabled by default to avoid memory bloat
}

fn default_batch_size() -> usize {
    1000
}

fn default_hamming_distance() -> u32 {
    30
}

/// Get BigQuery table name for given hamming distance threshold
fn get_bigquery_table(hamming_distance: u32) -> String {
    format!("video_dedup_status_HAM{}", hamming_distance)
}

/// Get Redis key for phash (same for all thresholds)
fn get_redis_key(phash: &str) -> String {
    format!("video_phash:{}", phash)
}

#[derive(Debug, Serialize, ToSchema)]
pub struct IngestPhashResponse {
    pub total_processed: u32,
    pub unique_count: u32,
    pub duplicate_count: u32,
    pub failed: u32,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct BulkIngestRequest {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct BulkIngestResponse {
    pub total_inserted: u32,
    pub failed: u32,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct DeduplicateRequest {
    #[serde(default = "default_limit")]
    pub limit: u32,

    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    #[serde(default = "default_hamming_distance")]
    pub hamming_distance: u32, // 10, 20, 30, 40, 50

    #[serde(default = "default_collect_metrics")]
    pub collect_metrics: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DeduplicateResponse {
    pub total_processed: u32,
    pub unique_count: u32,
    pub duplicate_count: u32,
    pub failed: u32,
}

/// QStash handler to ingest video phashes from BigQuery into Milvus
/// Checks for near-duplicates (Hamming distance < 10) and records status
/// Spawns background task and returns immediately
#[utoipa::path(
    post,
    path = "/qstash/milvus/ingest_phash",
    request_body = IngestPhashRequest,
    responses(
        (status = 202, description = "Batch ingestion started"),
        (status = 500, description = "Internal server error")
    ),
    tag = "qstash"
)]
#[instrument(skip(state))]
pub async fn ingest_phash_to_milvus_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<IngestPhashRequest>,
) -> Result<StatusCode, StatusCode> {
    log::info!(
        "Starting Milvus ingestion (async): limit={}, concurrency={}",
        req.limit,
        req.concurrency
    );

    #[cfg(feature = "local-bin")]
    {
        log::warn!("Milvus ingestion not available in local-bin mode");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    #[cfg(not(feature = "local-bin"))]
    {
        // Check if Milvus client is available
        let milvus_client = match &state.milvus_client {
            Some(client) => client.clone(),
            None => {
                log::error!("Milvus client not initialized");
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
        };

        // Spawn background task
        tokio::spawn(async move {
            log::info!("Background task started for Milvus ingestion");
            match process_batch(&state, &milvus_client, &req).await {
                Ok(response) => {
                    log::info!(
                        "✅ Batch completed: processed={}, unique={}, duplicate={}, failed={}",
                        response.total_processed,
                        response.unique_count,
                        response.duplicate_count,
                        response.failed
                    );
                }
                Err(e) => {
                    log::error!("❌ Batch processing failed: {}", e);
                }
            }
        });

        Ok(StatusCode::ACCEPTED)
    }
}

/// QStash handler to backfill video phashes from video_unique table into Milvus
/// Only processes videos that are in video_unique (pre-filtered unique videos)
/// Spawns background task and returns immediately
#[utoipa::path(
    post,
    path = "/qstash/milvus/backfill_unique_videos",
    request_body = IngestPhashRequest,
    responses(
        (status = 202, description = "Backfill started"),
        (status = 500, description = "Internal server error")
    ),
    tag = "qstash"
)]
#[instrument(skip(state))]
pub async fn backfill_unique_videos_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<IngestPhashRequest>,
) -> Result<StatusCode, StatusCode> {
    log::info!(
        "Starting backfill from video_unique table (async): limit={}, concurrency={}",
        req.limit,
        req.concurrency
    );

    #[cfg(feature = "local-bin")]
    {
        log::warn!("Milvus backfill not available in local-bin mode");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    #[cfg(not(feature = "local-bin"))]
    {
        // Check if Milvus client is available
        let milvus_client = match &state.milvus_client {
            Some(client) => client.clone(),
            None => {
                log::error!("Milvus client not initialized");
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
        };

        // Spawn background task
        tokio::spawn(async move {
            log::info!("Background task started for video_unique backfill");
            match process_backfill_batch(&state, &milvus_client, &req).await {
                Ok(response) => {
                    log::info!(
                        "✅ Backfill completed: processed={}, unique={}, duplicate={}, failed={}",
                        response.total_processed,
                        response.unique_count,
                        response.duplicate_count,
                        response.failed
                    );
                }
                Err(e) => {
                    log::error!("❌ Backfill processing failed: {}", e);
                }
            }
        });

        Ok(StatusCode::ACCEPTED)
    }
}

#[cfg(not(feature = "local-bin"))]
async fn process_batch(
    state: &AppState,
    milvus_client: &MilvusClient,
    req: &IngestPhashRequest,
) -> Result<IngestPhashResponse> {
    // 1. Fetch unprocessed videos from BigQuery
    let videos = fetch_unprocessed_videos(state, req).await?;

    if videos.is_empty() {
        log::info!("No unprocessed videos found");
        return Ok(IngestPhashResponse {
            total_processed: 0,
            unique_count: 0,
            duplicate_count: 0,
            failed: 0,
        });
    }

    log::info!(
        "Processing {} videos with concurrency={}",
        videos.len(),
        req.concurrency
    );

    // 2. Process videos concurrently
    let collect_metrics = req.collect_metrics;
    let results: Vec<_> = stream::iter(videos)
        .map(|(video_id, phash)| {
            let state = state.clone();
            let milvus_client = milvus_client.clone();

            async move {
                let mut task_metrics = if collect_metrics {
                    MetricsCollector::Enabled(OperationMetrics::default())
                } else {
                    MetricsCollector::Disabled
                };
                let result = process_single_video(
                    &state,
                    &milvus_client,
                    &video_id,
                    &phash,
                    &mut task_metrics,
                )
                .await;
                (result, task_metrics)
            }
        })
        .buffer_unordered(req.concurrency)
        .collect()
        .await;

    // 3. Aggregate results
    let mut metrics = if req.collect_metrics {
        MetricsCollector::Enabled(OperationMetrics::default())
    } else {
        MetricsCollector::Disabled
    };
    let mut unique_count = 0;
    let mut duplicate_count = 0;
    let mut failed = 0;

    for (result, task_metrics) in results {
        metrics.merge(task_metrics);
        match result {
            Ok(true) => unique_count += 1,
            Ok(false) => duplicate_count += 1,
            Err(e) => {
                log::error!("Failed to process video: {}", e);
                failed += 1;
            }
        }
    }

    // Log summary statistics
    log::info!(
        "📊 Batch Processing Summary: Total={}, Unique={}, Duplicates={}, Failed={}",
        unique_count + duplicate_count + failed,
        unique_count,
        duplicate_count,
        failed
    );

    if duplicate_count > 0 {
        log::info!(
            "   Deduplication rate: {:.1}% of processed videos were duplicates",
            (duplicate_count as f64 / (unique_count + duplicate_count) as f64) * 100.0
        );
    }

    // Log performance metrics
    metrics.log_summary();

    Ok(IngestPhashResponse {
        total_processed: (unique_count + duplicate_count + failed),
        unique_count,
        duplicate_count,
        failed,
    })
}

#[cfg(not(feature = "local-bin"))]
async fn process_backfill_batch(
    state: &AppState,
    milvus_client: &MilvusClient,
    req: &IngestPhashRequest,
) -> Result<IngestPhashResponse> {
    // 1. Fetch unique videos from BigQuery (video_unique JOIN videohash_phash)
    let videos = fetch_unique_videos_for_backfill(state, req).await?;

    if videos.is_empty() {
        log::info!("No unique videos found for backfill");
        return Ok(IngestPhashResponse {
            total_processed: 0,
            unique_count: 0,
            duplicate_count: 0,
            failed: 0,
        });
    }

    log::info!(
        "Backfilling {} unique videos with concurrency={}",
        videos.len(),
        req.concurrency
    );

    // 2. Process videos concurrently
    let collect_metrics = req.collect_metrics;
    let results: Vec<_> = stream::iter(videos)
        .map(|(video_id, phash)| {
            let state = state.clone();
            let milvus_client = milvus_client.clone();

            async move {
                let mut task_metrics = if collect_metrics {
                    MetricsCollector::Enabled(OperationMetrics::default())
                } else {
                    MetricsCollector::Disabled
                };
                let result = process_single_video(
                    &state,
                    &milvus_client,
                    &video_id,
                    &phash,
                    &mut task_metrics,
                )
                .await;
                (result, task_metrics)
            }
        })
        .buffer_unordered(req.concurrency)
        .collect()
        .await;

    // 3. Aggregate results
    let mut metrics = if req.collect_metrics {
        MetricsCollector::Enabled(OperationMetrics::default())
    } else {
        MetricsCollector::Disabled
    };
    let mut unique_count = 0;
    let mut duplicate_count = 0;
    let mut failed = 0;

    for (result, task_metrics) in results {
        metrics.merge(task_metrics);
        match result {
            Ok(true) => unique_count += 1,
            Ok(false) => duplicate_count += 1,
            Err(e) => {
                log::error!("Failed to process video: {}", e);
                failed += 1;
            }
        }
    }

    // Log summary statistics
    log::info!(
        "📊 Backfill Summary: Total={}, Unique={}, Duplicates={}, Failed={}",
        unique_count + duplicate_count + failed,
        unique_count,
        duplicate_count,
        failed
    );

    if duplicate_count > 0 {
        log::info!(
            "   Deduplication rate: {:.1}% of backfilled videos were duplicates",
            (duplicate_count as f64 / (unique_count + duplicate_count) as f64) * 100.0
        );
    }

    // Log performance metrics
    metrics.log_summary();

    Ok(IngestPhashResponse {
        total_processed: (unique_count + duplicate_count + failed),
        unique_count,
        duplicate_count,
        failed,
    })
}

#[cfg(not(feature = "local-bin"))]
async fn fetch_unprocessed_videos(
    state: &AppState,
    req: &IngestPhashRequest,
) -> Result<Vec<(String, String)>> {
    let query = format!(
        "SELECT video_id, phash
         FROM `hot-or-not-feed-intelligence.yral_ds.videohash_phash`
         WHERE video_id NOT IN (
           SELECT video_id
           FROM `hot-or-not-feed-intelligence.yral_ds.video_dedup_status`
         )
         ORDER BY created_at ASC
         LIMIT {}",
        req.limit
    );

    log::debug!("Executing BigQuery query: {}", query);

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    let response = state
        .bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await
        .context("Failed to execute BigQuery query")?;

    let mut videos = Vec::new();

    if let Some(rows) = response.rows.as_ref() {
        for row in rows {
            if row.f.len() < 2 {
                continue;
            }

            let video_id = match &row.f[0].v {
                Value::String(s) => s.clone(),
                _ => continue,
            };

            let phash = match &row.f[1].v {
                Value::String(s) => s.clone(),
                _ => continue,
            };

            videos.push((video_id, phash));
        }
    }

    log::info!("Fetched {} videos from BigQuery", videos.len());
    Ok(videos)
}

#[cfg(not(feature = "local-bin"))]
async fn fetch_unique_videos_for_backfill(
    state: &AppState,
    req: &IngestPhashRequest,
) -> Result<Vec<(String, String)>> {
    let query = format!(
        "SELECT video_id, phash
         FROM
         (
          SELECT
              DISTINCT t1.video_id,
              t2.phash
            FROM
              `hot-or-not-feed-intelligence`.`yral_ds`.`video_unique` AS t1
            INNER JOIN
              `hot-or-not-feed-intelligence`.`yral_ds`.`videohash_phash` AS t2
            ON
              t1.video_id = t2.video_id
         )
         WHERE video_id NOT IN (
           SELECT video_id
           FROM `hot-or-not-feed-intelligence.yral_ds.video_dedup_status`
         )
         LIMIT {}",
        req.limit
    );

    log::debug!("Executing BigQuery backfill query: {}", query);

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    let response = state
        .bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await
        .context("Failed to execute BigQuery backfill query")?;

    let mut videos = Vec::new();

    if let Some(rows) = response.rows.as_ref() {
        for row in rows {
            if row.f.len() < 2 {
                continue;
            }

            let video_id = match &row.f[0].v {
                Value::String(s) => s.clone(),
                _ => continue,
            };

            let phash = match &row.f[1].v {
                Value::String(s) => s.clone(),
                _ => continue,
            };

            videos.push((video_id, phash));
        }
    }

    log::info!(
        "Fetched {} unique videos from BigQuery for backfill",
        videos.len()
    );
    Ok(videos)
}

#[cfg(not(feature = "local-bin"))]
async fn has_any_processed_videos(state: &AppState) -> Result<bool> {
    let query = "SELECT COUNT(*) as count FROM `hot-or-not-feed-intelligence.yral_ds.video_dedup_status` LIMIT 1";

    let request = QueryRequest {
        query: query.to_string(),
        ..Default::default()
    };

    let response = state
        .bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await
        .context("Failed to check video_dedup_status count")?;

    if let Some(rows) = response.rows.as_ref() {
        if let Some(row) = rows.first() {
            if let Some(field) = row.f.first() {
                if let Value::String(count_str) = &field.v {
                    if let Ok(count) = count_str.parse::<i64>() {
                        return Ok(count > 0);
                    }
                }
            }
        }
    }

    // Default to false if we can't parse the result
    Ok(false)
}

#[cfg(not(feature = "local-bin"))]
async fn check_exact_duplicate_in_redis(
    redis_pool: &bb8::Pool<bb8_redis::RedisConnectionManager>,
    phash: &str,
) -> Result<Option<String>> {
    let mut conn = redis_pool
        .get()
        .await
        .context("Failed to get Redis connection")?;

    let key = format!("video_phash:{}", phash);
    let result: Option<String> = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut *conn)
        .await
        .context("Failed to query Redis for phash")?;

    Ok(result)
}

#[cfg(not(feature = "local-bin"))]
async fn store_unique_phash_in_redis(
    redis_pool: &bb8::Pool<bb8_redis::RedisConnectionManager>,
    phash: &str,
    video_id: &str,
) -> Result<()> {
    let mut conn = redis_pool
        .get()
        .await
        .context("Failed to get Redis connection")?;

    let key = format!("video_phash:{}", phash);
    redis::cmd("SET")
        .arg(&key)
        .arg(video_id)
        .query_async::<()>(&mut *conn)
        .await
        .context("Failed to store phash in Redis")?;

    Ok(())
}

#[cfg(not(feature = "local-bin"))]
async fn process_single_video(
    state: &AppState,
    milvus_client: &MilvusClient,
    video_id: &str,
    phash: &str,
    metrics: &mut MetricsCollector,
) -> Result<bool> {
    const HAMMING_THRESHOLD: u32 = 20;

    // TIER 1: Check Redis for exact match (FAST - <1ms)
    log::debug!("Tier 1: Checking Redis for exact phash match");
    let start = Instant::now();
    let redis_result = check_exact_duplicate_in_redis(&state.leaderboard_redis_pool, phash).await?;
    metrics.record_redis_check(start.elapsed().as_micros());

    if let Some(existing_video_id) = redis_result {
        log::info!(
            "⚡ EXACT DUPLICATE (Redis): Video {} has identical phash to {}",
            video_id,
            existing_video_id
        );

        // Record in BigQuery as exact duplicate (distance = 0)
        let start = Instant::now();
        store_dedup_status(
            state,
            video_id,
            phash,
            false, // is_unique = false
            Some(existing_video_id),
            Some(0), // hamming_distance = 0 (exact match)
        )
        .await
        .context("Failed to store dedup status")?;
        metrics.record_bigquery_insert(start.elapsed().as_micros());

        return Ok(false); // Not unique (is duplicate)
    }

    log::debug!("Tier 1: No exact match in Redis");

    // TIER 2: Check Milvus for similar matches (SLOWER - 10-50ms)
    log::debug!(
        "Tier 2: Checking Milvus for similar videos (Hamming distance < {})",
        HAMMING_THRESHOLD
    );

    // Check if collection has any data (to avoid SDK panic on empty collection)
    let collection_has_data = has_any_processed_videos(state).await.unwrap_or(false);

    let start = Instant::now();
    let similar_videos = if collection_has_data {
        milvus::search_similar_videos(milvus_client, phash, HAMMING_THRESHOLD)
            .await
            .context("Failed to search in Milvus")?
    } else {
        log::info!(
            "Skipping Milvus search for video {} (empty collection)",
            video_id
        );
        Vec::new()
    };
    metrics.record_milvus_search(start.elapsed().as_micros());

    let is_duplicate = !similar_videos.is_empty();
    let (duplicate_of, hamming_distance) = if is_duplicate {
        let closest = &similar_videos[0];

        log::info!(
            "🔍 SIMILAR DUPLICATE (Milvus): Video {} matches {} with Hamming distance {}",
            video_id,
            closest.video_id,
            closest.hamming_distance
        );

        (
            Some(closest.video_id.clone()),
            Some(closest.hamming_distance),
        )
    } else {
        (None, None)
    };

    // 3. Store in Redis + Milvus (ONLY if unique)
    if !is_duplicate {
        log::info!(
            "✨ UNIQUE: Video {} has a unique phash, storing in Redis + Milvus",
            video_id
        );

        let created_at = chrono::Utc::now().timestamp();

        // Store in Milvus (only unique hashes)
        let start = Instant::now();
        milvus::insert_video_hash(milvus_client, video_id, phash, created_at)
            .await
            .context("Failed to insert into Milvus")?;
        metrics.record_milvus_insert(start.elapsed().as_micros());

        // Store in Redis (only unique hashes)
        let start = Instant::now();
        store_unique_phash_in_redis(&state.leaderboard_redis_pool, phash, video_id)
            .await
            .context("Failed to store phash in Redis")?;
        metrics.record_redis_insert(start.elapsed().as_micros());
    } else {
        log::debug!(
            "Video {} is a duplicate, NOT storing in Redis/Milvus",
            video_id
        );
    }

    // 4. Record status in BigQuery (ALL videos - both unique and duplicates)
    let start = Instant::now();
    store_dedup_status(
        state,
        video_id,
        phash,
        !is_duplicate,
        duplicate_of.clone(),
        hamming_distance,
    )
    .await
    .context("Failed to store dedup status")?;
    metrics.record_bigquery_insert(start.elapsed().as_micros());

    log::debug!(
        "Processed video {}: is_unique={}, duplicate_of={:?}, distance={:?}",
        video_id,
        !is_duplicate,
        duplicate_of,
        hamming_distance
    );

    Ok(!is_duplicate)
}

#[cfg(not(feature = "local-bin"))]
async fn store_dedup_status(
    state: &AppState,
    video_id: &str,
    phash: &str,
    is_unique: bool,
    duplicate_of: Option<String>,
    hamming_distance: Option<u32>,
) -> Result<()> {
    let row_data = json!({
        "video_id": video_id,
        "phash": phash,
        "is_duplicate": !is_unique,
        "duplicate_of": duplicate_of,
        "hamming_distance": hamming_distance,
        "ingested_at": chrono::Utc::now().to_rfc3339(),
    });

    let request = InsertAllRequest {
        rows: vec![Row {
            insert_id: Some(format!(
                "dedup_status_{}_{}",
                video_id,
                chrono::Utc::now().timestamp_millis()
            )),
            json: row_data.clone(),
        }],
        ignore_unknown_values: Some(false),
        skip_invalid_rows: Some(false),
        ..Default::default()
    };

    let result = state
        .bigquery_client
        .tabledata()
        .insert(
            "hot-or-not-feed-intelligence",
            "yral_ds",
            "video_dedup_status",
            &request,
        )
        .await
        .context("Failed to insert into BigQuery")?;

    if let Some(errors) = result.insert_errors {
        if !errors.is_empty() {
            anyhow::bail!("BigQuery insert errors: {:?}", errors);
        }
    }

    // Also push to kvrocks
    if let Some(ref kvrocks) = state.kvrocks_client {
        if let Err(e) = kvrocks.store_video_dedup_status(video_id, &row_data).await {
            log::error!("Error pushing dedup_status to kvrocks: {}", e);
        }
    }

    Ok(())
}

/// QStash handler to bulk ingest ALL unique videos from video_unique table into Milvus + Redis
/// Phase 1: Index all unique videos (no deduplication logic)
#[cfg(not(feature = "local-bin"))]
#[utoipa::path(
    post,
    path = "/qstash/milvus/bulk_ingest_unique_hashes",
    request_body = BulkIngestRequest,
    responses(
        (status = 202, description = "Bulk ingestion started"),
        (status = 500, description = "Internal server error")
    ),
    tag = "qstash"
)]
#[instrument(skip(state))]
pub async fn bulk_ingest_unique_hashes_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BulkIngestRequest>,
) -> Result<StatusCode, StatusCode> {
    log::info!(
        "Starting bulk ingestion of unique videos (async): batch_size={}",
        req.batch_size
    );

    // Check if Milvus client is available
    let milvus_client = match &state.milvus_client {
        Some(client) => client.clone(),
        None => {
            log::error!("Milvus client not initialized");
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    // Spawn background task
    tokio::spawn(async move {
        log::info!("Background task started for bulk ingestion");
        match process_bulk_ingest(&state, &milvus_client, &req).await {
            Ok(response) => {
                log::info!(
                    "✅ Bulk ingestion completed: total_inserted={}, failed={}",
                    response.total_inserted,
                    response.failed
                );
            }
            Err(e) => {
                log::error!("❌ Bulk ingestion failed: {}", e);
            }
        }
    });

    Ok(StatusCode::ACCEPTED)
}

#[cfg(not(feature = "local-bin"))]
async fn process_bulk_ingest(
    state: &AppState,
    milvus_client: &MilvusClient,
    req: &BulkIngestRequest,
) -> Result<BulkIngestResponse> {
    log::info!("Fetching ALL unique videos from video_unique table");

    // 1. Fetch ALL unique videos from BigQuery (video_unique JOIN videohash_phash)
    let videos = fetch_all_unique_videos(state).await?;

    if videos.is_empty() {
        log::info!("No unique videos found");
        return Ok(BulkIngestResponse {
            total_inserted: 0,
            failed: 0,
        });
    }

    log::info!(
        "Found {} unique videos, starting bulk insert with batch_size={}",
        videos.len(),
        req.batch_size
    );

    // 2. Bulk insert into Milvus and Redis in batches
    let mut total_inserted = 0;
    let mut failed = 0;

    for chunk in videos.chunks(req.batch_size) {
        match bulk_insert_to_milvus_and_redis(state, milvus_client, chunk).await {
            Ok(count) => {
                total_inserted += count;
                log::info!(
                    "Inserted batch of {} videos (total: {})",
                    count,
                    total_inserted
                );
            }
            Err(e) => {
                log::error!("Failed to insert batch: {}", e);
                failed += chunk.len() as u32;
            }
        }
    }

    log::info!(
        "📊 Bulk Ingestion Summary: Total={}, Failed={}",
        total_inserted,
        failed
    );

    Ok(BulkIngestResponse {
        total_inserted,
        failed,
    })
}

#[cfg(not(feature = "local-bin"))]
async fn fetch_all_unique_videos(state: &AppState) -> Result<Vec<(String, String)>> {
    let query = "SELECT video_id, phash
         FROM
         (
          SELECT
              DISTINCT t1.video_id,
              t2.phash
            FROM
              `hot-or-not-feed-intelligence`.`yral_ds`.`video_unique` AS t1
            INNER JOIN
              `hot-or-not-feed-intelligence`.`yral_ds`.`videohash_phash` AS t2
            ON
              t1.video_id = t2.video_id
         )";

    log::debug!("Executing BigQuery query for ALL unique videos with pagination");

    let request = QueryRequest {
        query: query.to_string(),
        max_results: Some(10000), // Fetch 10K rows per page
        ..Default::default()
    };

    let mut response = state
        .bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await
        .context("Failed to execute BigQuery query for all unique videos")?;

    let mut videos = Vec::new();
    let mut page_count = 1;

    // Process first page
    if let Some(rows) = response.rows.as_ref() {
        for row in rows {
            if row.f.len() < 2 {
                continue;
            }

            let video_id = match &row.f[0].v {
                Value::String(s) => s.clone(),
                _ => continue,
            };

            let phash = match &row.f[1].v {
                Value::String(s) => s.clone(),
                _ => continue,
            };

            videos.push((video_id, phash));
        }
    }

    log::info!(
        "Fetched page {} with {} videos (total so far: {})",
        page_count,
        response.rows.as_ref().map(|r| r.len()).unwrap_or(0),
        videos.len()
    );

    // Paginate through remaining results
    while let Some(page_token) = response.page_token.as_ref() {
        if page_token.is_empty() {
            break;
        }

        page_count += 1;
        log::debug!("Fetching page {} with token: {}", page_count, page_token);

        // Fetch next page using getQueryResults
        let job_ref = &response.job_reference;
        let get_results_req = GetQueryResultsRequest {
            start_index: 0,
            page_token: Some(page_token.clone()),
            max_results: Some(10000),
            timeout_ms: None,
            location: job_ref.location.clone(),
            format_options: None,
        };

        let get_results_response = state
            .bigquery_client
            .job()
            .get_query_results(&job_ref.project_id, &job_ref.job_id, &get_results_req)
            .await
            .context(format!("Failed to fetch page {}", page_count))?;

        // Convert GetQueryResultsResponse to QueryResponse format for processing
        response.rows = get_results_response.rows;
        response.page_token = get_results_response.page_token;

        if let Some(rows) = response.rows.as_ref() {
            for row in rows {
                if row.f.len() < 2 {
                    continue;
                }

                let video_id = match &row.f[0].v {
                    Value::String(s) => s.clone(),
                    _ => continue,
                };

                let phash = match &row.f[1].v {
                    Value::String(s) => s.clone(),
                    _ => continue,
                };

                videos.push((video_id, phash));
            }
        }

        log::info!(
            "Fetched page {} with {} videos (total so far: {})",
            page_count,
            response.rows.as_ref().map(|r| r.len()).unwrap_or(0),
            videos.len()
        );
    }

    log::info!(
        "✅ Fetched ALL {} unique videos from BigQuery across {} pages",
        videos.len(),
        page_count
    );
    Ok(videos)
}

#[cfg(not(feature = "local-bin"))]
async fn bulk_insert_to_milvus_and_redis(
    state: &AppState,
    milvus_client: &MilvusClient,
    videos: &[(String, String)],
) -> Result<u32> {
    let created_at = chrono::Utc::now().timestamp();

    // Prepare batch for Milvus
    let records: Vec<milvus::VideoHashRecord> = videos
        .iter()
        .filter_map(
            |(video_id, phash)| match milvus::utils::phash_to_binary_vector(phash) {
                Ok(phash_vector) => Some(milvus::VideoHashRecord {
                    video_id: video_id.clone(),
                    phash_vector,
                    created_at,
                }),
                Err(e) => {
                    log::error!("Failed to convert phash for {}: {}", video_id, e);
                    None
                }
            },
        )
        .collect();

    // Insert batch into Milvus
    milvus::insert_batch_video_hashes(milvus_client, records)
        .await
        .context("Failed to insert batch into Milvus")?;

    // Insert into Redis using pipeline (for exact match caching)
    let mut conn = state
        .leaderboard_redis_pool
        .get()
        .await
        .context("Failed to get Redis connection")?;

    // Use Redis pipeline for batch SET operations (much faster than individual SETs)
    let mut pipe = redis::pipe();
    for (video_id, phash) in videos {
        let redis_key = get_redis_key(phash);
        pipe.set(&redis_key, video_id);
    }

    pipe.query_async::<()>(&mut *conn)
        .await
        .context("Failed to store phashes in Redis")?;

    Ok(videos.len() as u32)
}

/// QStash handler to deduplicate videos at a specific hamming distance threshold
/// Phase 2: Find duplicates using top_k=2 search and write to threshold-specific BigQuery table
#[cfg(not(feature = "local-bin"))]
#[utoipa::path(
    post,
    path = "/qstash/milvus/deduplicate_videos",
    request_body = DeduplicateRequest,
    responses(
        (status = 202, description = "Deduplication started"),
        (status = 500, description = "Internal server error")
    ),
    tag = "qstash"
)]
#[instrument(skip(state))]
pub async fn deduplicate_videos_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DeduplicateRequest>,
) -> Result<StatusCode, StatusCode> {
    log::info!(
        "Starting video deduplication (async): limit={}, threshold={}, concurrency={}",
        req.limit,
        req.hamming_distance,
        req.concurrency
    );

    // Check if Milvus client is available
    let milvus_client = match &state.milvus_client {
        Some(client) => client.clone(),
        None => {
            log::error!("Milvus client not initialized");
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    // Spawn background task
    tokio::spawn(async move {
        log::info!("Background task started for video deduplication");
        match process_deduplicate(&state, &milvus_client, &req).await {
            Ok(response) => {
                log::info!(
                    "✅ Deduplication completed: processed={}, unique={}, duplicate={}, failed={}",
                    response.total_processed,
                    response.unique_count,
                    response.duplicate_count,
                    response.failed
                );
            }
            Err(e) => {
                log::error!("❌ Deduplication failed: {}", e);
            }
        }
    });

    Ok(StatusCode::ACCEPTED)
}

#[cfg(not(feature = "local-bin"))]
async fn process_deduplicate(
    state: &AppState,
    milvus_client: &MilvusClient,
    req: &DeduplicateRequest,
) -> Result<DeduplicateResponse> {
    log::info!(
        "Deduplicating videos with hamming_distance <= {}",
        req.hamming_distance
    );

    // 1. Fetch unprocessed videos (not in threshold-specific table)
    let videos = fetch_unprocessed_videos_for_threshold(state, req).await?;

    if videos.is_empty() {
        log::info!("No unprocessed videos found for this threshold");
        return Ok(DeduplicateResponse {
            total_processed: 0,
            unique_count: 0,
            duplicate_count: 0,
            failed: 0,
        });
    }

    log::info!(
        "Processing {} videos with concurrency={} for threshold={}",
        videos.len(),
        req.concurrency,
        req.hamming_distance
    );

    // 2. Process videos concurrently
    let collect_metrics = req.collect_metrics;
    let hamming_distance = req.hamming_distance;
    let results: Vec<_> = stream::iter(videos)
        .map(|(video_id, phash)| {
            let state = state.clone();
            let milvus_client = milvus_client.clone();

            async move {
                let mut task_metrics = if collect_metrics {
                    MetricsCollector::Enabled(OperationMetrics::default())
                } else {
                    MetricsCollector::Disabled
                };
                let result = process_deduplicate_video(
                    &state,
                    &milvus_client,
                    &video_id,
                    &phash,
                    hamming_distance,
                    &mut task_metrics,
                )
                .await;
                (result, task_metrics)
            }
        })
        .buffer_unordered(req.concurrency)
        .collect()
        .await;

    // 3. Aggregate results
    let mut metrics = if req.collect_metrics {
        MetricsCollector::Enabled(OperationMetrics::default())
    } else {
        MetricsCollector::Disabled
    };
    let mut unique_count = 0;
    let mut duplicate_count = 0;
    let mut failed = 0;

    for (result, task_metrics) in results {
        metrics.merge(task_metrics);
        match result {
            Ok(true) => unique_count += 1, // is_unique = true
            Ok(false) => duplicate_count += 1,
            Err(e) => {
                log::error!("Failed to process video: {}", e);
                failed += 1;
            }
        }
    }

    // Log summary statistics
    log::info!(
        "📊 Deduplication Summary (HAM{}): Total={}, Unique={}, Duplicates={}, Failed={}",
        req.hamming_distance,
        unique_count + duplicate_count + failed,
        unique_count,
        duplicate_count,
        failed
    );

    if duplicate_count > 0 {
        log::info!(
            "   Deduplication rate: {:.1}% of processed videos were duplicates",
            (duplicate_count as f64 / (unique_count + duplicate_count) as f64) * 100.0
        );
    }

    // Log performance metrics
    metrics.log_summary();

    Ok(DeduplicateResponse {
        total_processed: (unique_count + duplicate_count + failed),
        unique_count,
        duplicate_count,
        failed,
    })
}

#[cfg(not(feature = "local-bin"))]
async fn fetch_unprocessed_videos_for_threshold(
    state: &AppState,
    req: &DeduplicateRequest,
) -> Result<Vec<(String, String)>> {
    let table_name = get_bigquery_table(req.hamming_distance);
    let query = format!(
        "SELECT video_id, phash
         FROM `hot-or-not-feed-intelligence.yral_ds.videohash_phash`
         WHERE video_id NOT IN (
           SELECT video_id
           FROM `hot-or-not-feed-intelligence.yral_ds.{}`
         )
         LIMIT {}",
        table_name, req.limit
    );

    log::debug!(
        "Executing BigQuery query for unprocessed videos (table: {}) with pagination",
        table_name
    );

    let request = QueryRequest {
        query,
        max_results: Some(10000), // Fetch 10K rows per page
        ..Default::default()
    };

    let mut response = state
        .bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await
        .context("Failed to execute BigQuery query for unprocessed videos")?;

    let mut videos = Vec::new();
    let mut page_count = 1;

    // Process first page
    if let Some(rows) = response.rows.as_ref() {
        for row in rows {
            if row.f.len() < 2 {
                continue;
            }

            let video_id = match &row.f[0].v {
                Value::String(s) => s.clone(),
                _ => continue,
            };

            let phash = match &row.f[1].v {
                Value::String(s) => s.clone(),
                _ => continue,
            };

            videos.push((video_id, phash));
        }
    }

    log::info!(
        "Fetched page {} with {} videos for HAM{} (total so far: {})",
        page_count,
        response.rows.as_ref().map(|r| r.len()).unwrap_or(0),
        req.hamming_distance,
        videos.len()
    );

    // Paginate through remaining results (up to the limit)
    while let Some(page_token) = response.page_token.as_ref() {
        if page_token.is_empty() || videos.len() >= req.limit as usize {
            break;
        }

        page_count += 1;
        log::debug!(
            "Fetching page {} for HAM{} with token: {}",
            page_count,
            req.hamming_distance,
            page_token
        );

        // Fetch next page using getQueryResults
        let job_ref = &response.job_reference;
        let get_results_req = GetQueryResultsRequest {
            start_index: 0,
            page_token: Some(page_token.clone()),
            max_results: Some(10000),
            timeout_ms: None,
            location: job_ref.location.clone(),
            format_options: None,
        };

        let get_results_response = state
            .bigquery_client
            .job()
            .get_query_results(&job_ref.project_id, &job_ref.job_id, &get_results_req)
            .await
            .context(format!(
                "Failed to fetch page {} for HAM{}",
                page_count, req.hamming_distance
            ))?;

        // Convert GetQueryResultsResponse to QueryResponse format for processing
        response.rows = get_results_response.rows;
        response.page_token = get_results_response.page_token;

        if let Some(rows) = response.rows.as_ref() {
            for row in rows {
                if videos.len() >= req.limit as usize {
                    break; // Stop if we've reached the limit
                }

                if row.f.len() < 2 {
                    continue;
                }

                let video_id = match &row.f[0].v {
                    Value::String(s) => s.clone(),
                    _ => continue,
                };

                let phash = match &row.f[1].v {
                    Value::String(s) => s.clone(),
                    _ => continue,
                };

                videos.push((video_id, phash));
            }
        }

        log::info!(
            "Fetched page {} with {} videos for HAM{} (total so far: {})",
            page_count,
            response.rows.as_ref().map(|r| r.len()).unwrap_or(0),
            req.hamming_distance,
            videos.len()
        );
    }

    log::info!(
        "✅ Fetched {} unprocessed videos for HAM{} across {} pages",
        videos.len(),
        req.hamming_distance,
        page_count
    );
    Ok(videos)
}

#[cfg(not(feature = "local-bin"))]
async fn process_deduplicate_video(
    state: &AppState,
    milvus_client: &MilvusClient,
    video_id: &str,
    phash: &str,
    hamming_distance: u32,
    metrics: &mut MetricsCollector,
) -> Result<bool> {
    // Search for top 2 results: [self-match, nearest_neighbor]
    log::debug!("Searching for duplicates of video: {}", video_id);
    let start = Instant::now();
    let results = milvus::search_similar_videos_topk2(milvus_client, phash)
        .await
        .context("Failed to search in Milvus")?;
    metrics.record_milvus_search(start.elapsed().as_micros());

    // Filter out self-match (distance=0 or same video_id)
    let nearest_neighbor = results
        .into_iter()
        .find(|r| r.hamming_distance > 0 && r.video_id != video_id);

    let (is_unique, duplicate_of, distance) = if let Some(neighbor) = nearest_neighbor {
        if neighbor.hamming_distance <= hamming_distance {
            log::info!(
                "🔍 DUPLICATE (HAM{}): Video {} matches {} with distance {}",
                hamming_distance,
                video_id,
                neighbor.video_id,
                neighbor.hamming_distance
            );
            (
                false,
                Some(neighbor.video_id),
                Some(neighbor.hamming_distance),
            )
        } else {
            log::info!(
                "✨ UNIQUE (HAM{}): Video {} has no duplicates within threshold (nearest: {} at distance {})",
                hamming_distance,
                video_id,
                neighbor.video_id,
                neighbor.hamming_distance
            );
            (true, None, None)
        }
    } else {
        log::info!(
            "✨ UNIQUE (HAM{}): Video {} has no other videos in index",
            hamming_distance,
            video_id
        );
        (true, None, None)
    };

    // Store in threshold-specific BigQuery table
    let start = Instant::now();
    store_dedup_status_with_table(
        state,
        video_id,
        phash,
        is_unique,
        duplicate_of,
        distance,
        hamming_distance,
    )
    .await
    .context("Failed to store dedup status")?;
    metrics.record_bigquery_insert(start.elapsed().as_micros());

    Ok(is_unique)
}

#[cfg(not(feature = "local-bin"))]
async fn store_dedup_status_with_table(
    state: &AppState,
    video_id: &str,
    phash: &str,
    is_unique: bool,
    duplicate_of: Option<String>,
    hamming_distance_actual: Option<u32>,
    hamming_distance_threshold: u32,
) -> Result<()> {
    let table_name = get_bigquery_table(hamming_distance_threshold);

    let row_data = json!({
        "video_id": video_id,
        "phash": phash,
        "is_duplicate": !is_unique,
        "duplicate_of": duplicate_of,
        "hamming_distance": hamming_distance_actual,
        "ingested_at": chrono::Utc::now().to_rfc3339(),
    });

    let request = InsertAllRequest {
        rows: vec![Row {
            insert_id: Some(format!(
                "dedup_{}_{}_{}",
                hamming_distance_threshold,
                video_id,
                chrono::Utc::now().timestamp_millis()
            )),
            json: row_data.clone(),
        }],
        ignore_unknown_values: Some(false),
        skip_invalid_rows: Some(false),
        ..Default::default()
    };

    let result = state
        .bigquery_client
        .tabledata()
        .insert(
            "hot-or-not-feed-intelligence",
            "yral_ds",
            &table_name,
            &request,
        )
        .await
        .context(format!(
            "Failed to insert into BigQuery table {}",
            table_name
        ))?;

    if let Some(errors) = result.insert_errors {
        if !errors.is_empty() {
            anyhow::bail!("BigQuery insert errors: {:?}", errors);
        }
    }

    // Also push to kvrocks with table suffix
    if let Some(ref kvrocks) = state.kvrocks_client {
        let table_suffix = format!("HAM{}", hamming_distance_threshold);
        if let Err(e) = kvrocks.store_video_dedup_status_with_table(&table_suffix, video_id, &row_data).await {
            log::error!("Error pushing dedup_status to kvrocks ({}): {}", table_suffix, e);
        }
    }

    Ok(())
}
