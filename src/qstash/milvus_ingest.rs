use crate::app_state::AppState;
use crate::milvus::{self, Client as MilvusClient};
use anyhow::{Context, Result};
use axum::{extract::State, http::StatusCode, Json};
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::http::tabledata::insert_all::{InsertAllRequest, Row};
use google_cloud_bigquery::http::tabledata::list::Value;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::instrument;
use utoipa::ToSchema;

#[derive(Debug, Deserialize, ToSchema)]
pub struct IngestPhashRequest {
    #[serde(default = "default_limit")]
    pub limit: u32,
}

fn default_limit() -> u32 {
    1000
}

#[derive(Debug, Serialize, ToSchema)]
pub struct IngestPhashResponse {
    pub total_processed: u32,
    pub unique_count: u32,
    pub duplicate_count: u32,
    pub failed: u32,
}

/// QStash handler to ingest video phashes from BigQuery into Milvus
/// Checks for near-duplicates (Hamming distance < 10) and records status
#[utoipa::path(
    post,
    path = "/qstash/milvus/ingest_phash",
    request_body = IngestPhashRequest,
    responses(
        (status = 200, description = "Batch ingestion completed", body = IngestPhashResponse),
        (status = 500, description = "Internal server error")
    ),
    tag = "qstash"
)]
#[instrument(skip(state))]
pub async fn ingest_phash_to_milvus_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<IngestPhashRequest>,
) -> Result<Json<IngestPhashResponse>, StatusCode> {
    log::info!("Starting Milvus ingestion: limit={}", req.limit);

    #[cfg(feature = "local-bin")]
    {
        log::warn!("Milvus ingestion not available in local-bin mode");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    #[cfg(not(feature = "local-bin"))]
    {
        // Check if Milvus client is available
        let milvus_client = match &state.milvus_client {
            Some(client) => client,
            None => {
                log::error!("Milvus client not initialized");
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
        };

        match process_batch(&state, milvus_client, &req).await {
            Ok(response) => {
                log::info!(
                    "Batch completed: processed={}, unique={}, duplicate={}, failed={}",
                    response.total_processed,
                    response.unique_count,
                    response.duplicate_count,
                    response.failed
                );
                Ok(Json(response))
            }
            Err(e) => {
                log::error!("Batch processing failed: {}", e);
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
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

    log::info!("Processing {} videos", videos.len());

    let mut unique_count = 0;
    let mut duplicate_count = 0;
    let mut failed = 0;

    // 2. Process each video
    for (video_id, phash) in videos {
        match process_single_video(state, milvus_client, &video_id, &phash).await {
            Ok(is_unique) => {
                if is_unique {
                    unique_count += 1;
                } else {
                    duplicate_count += 1;
                }
            }
            Err(e) => {
                log::error!("Failed to process video {}: {}", video_id, e);
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
async fn process_single_video(
    state: &AppState,
    milvus_client: &MilvusClient,
    video_id: &str,
    phash: &str,
) -> Result<bool> {
    const HAMMING_THRESHOLD: u32 = 10;

    // 1. Check if collection has any data (to avoid SDK panic on empty collection)
    let collection_has_data = has_any_processed_videos(state)
        .await
        .unwrap_or(false); // Default to false if query fails

    // 2. Search for similar videos in Milvus (skip if collection is empty)
    let similar_videos = if collection_has_data {
        milvus::search_similar_videos(milvus_client, phash, HAMMING_THRESHOLD)
            .await
            .context("Failed to search in Milvus")?
    } else {
        log::info!("Skipping Milvus search for video {} (empty collection)", video_id);
        Vec::new()
    };

    let is_duplicate = !similar_videos.is_empty();
    let (duplicate_of, hamming_distance) = if is_duplicate {
        let closest = &similar_videos[0];

        // Print duplicate match information
        log::info!(
            "🔍 DUPLICATE FOUND: Video {} matches {} with Hamming distance {}",
            video_id,
            closest.video_id,
            closest.hamming_distance
        );

        // Print all matches if there are multiple
        if similar_videos.len() > 1 {
            log::info!("  Additional matches for {}:", video_id);
            for (idx, similar) in similar_videos.iter().skip(1).enumerate() {
                log::info!(
                    "    {}. {} (distance: {})",
                    idx + 2,
                    similar.video_id,
                    similar.hamming_distance
                );
            }
        }

        (
            Some(closest.video_id.clone()),
            Some(closest.hamming_distance),
        )
    } else {
        (None, None)
    };

    // 3. Insert into Milvus (even if duplicate, for future searches)
    let created_at = chrono::Utc::now().timestamp();
    milvus::insert_video_hash(milvus_client, video_id, phash, created_at)
        .await
        .context("Failed to insert into Milvus")?;

    // 4. Record status in BigQuery
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
            json: row_data,
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

    Ok(())
}
