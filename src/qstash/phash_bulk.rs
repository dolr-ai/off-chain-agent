use crate::app_state::AppState;
use crate::duplicate_video::phash::{download_video_from_storj, extract_metadata, PHasher};
use crate::kvrocks::VideohashPhash;
use crate::pipeline::Step;
use crate::setup_context;
use axum::{extract::State, response::Response, Json};
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::http::tabledata::insert_all::{InsertAllRequest, Row};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::instrument;
use uuid::Uuid;

/// Request payload for computing video phash
#[derive(Debug, Deserialize, Serialize)]
pub struct ComputePhashRequest {
    pub video_id: String,
    pub publisher_user_id: String,
}

/// Request payload for bulk phash computation
#[derive(Debug, Deserialize, Serialize)]
pub struct BulkComputePhashRequest {
    /// Optional limit on number of videos to process
    #[serde(default = "default_limit")]
    pub limit: u32,
    /// QStash rate limit (requests per second)
    #[serde(default = "default_rate")]
    pub rate: u32,
    /// QStash parallelism (concurrent workers)
    #[serde(default = "default_parallelism")]
    pub parallelism: u32,
}

fn default_limit() -> u32 {
    1000
}

fn default_rate() -> u32 {
    10
}

fn default_parallelism() -> u32 {
    5
}

/// Response for bulk operation
#[derive(Debug, Serialize)]
pub struct BulkComputePhashResponse {
    pub total_videos: usize,
    pub queued: usize,
    pub failed: usize,
}

/// Handler for computing and storing phash for a single video
#[instrument(skip(state))]
pub async fn compute_video_phash_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ComputePhashRequest>,
) -> Result<Response, StatusCode> {
    setup_context!(&req.video_id, Step::Deduplication);

    log::info!("Computing phash for video ID: {}", req.video_id);

    // Create temp directory for video
    let temp_dir = std::env::temp_dir().join(format!("phash_{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&temp_dir).await.map_err(|e| {
        log::error!("Failed to create temp directory: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let video_path = temp_dir.join(format!("{}.mp4", req.video_id));

    if let Err(e) =
        download_video_from_storj(&req.publisher_user_id, &req.video_id, &video_path).await
    {
        log::error!("Failed to download video {}: {}", req.video_id, e);
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Compute phash
    let hasher = PHasher::new();
    let video_path_clone = video_path.clone();
    let video_id_clone = req.video_id.clone();

    let (phash, metadata) = tokio::task::spawn_blocking(move || {
        let phash = hasher.compute_hash(&video_path_clone)?;
        let metadata = extract_metadata(&video_path_clone, video_id_clone)?;
        Ok::<_, anyhow::Error>((phash, metadata))
    })
    .await
    .map_err(|e| {
        log::error!("Task join error: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?
    .map_err(|e| {
        log::error!("Failed to compute phash: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    log::info!(
        "Computed phash for video {}: {} (length: {})",
        req.video_id,
        &phash[..20.min(phash.len())],
        phash.len()
    );

    // Store in BigQuery
    if let Err(e) = store_phash_to_bigquery(&state, &req.video_id, &phash, &metadata).await {
        log::error!("Failed to store phash to BigQuery: {}", e);
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Cleanup
    if let Err(e) = tokio::fs::remove_dir_all(&temp_dir).await {
        log::warn!("Failed to cleanup temp directory: {}", e);
    }

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(format!("Phash computed and stored for video: {}", req.video_id).into())
        .unwrap();

    Ok(response)
}

/// Handler for bulk phash computation - reads from BigQuery and fires QStash requests
#[instrument(skip(state))]
pub async fn bulk_compute_phash_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BulkComputePhashRequest>,
) -> Result<Json<BulkComputePhashResponse>, StatusCode> {
    log::info!(
        "Starting bulk phash computation for unprocessed bot videos (limit: {}, rate: {}, parallelism: {})",
        req.limit, req.rate, req.parallelism
    );

    // Query BigQuery for video_ids
    let video_ids = fetch_video_ids_from_bigquery(&state, &req)
        .await
        .map_err(|e| {
            log::error!("Failed to fetch video IDs from BigQuery: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let total_videos = video_ids.len();
    log::info!("Found {} videos to process", total_videos);

    if total_videos == 0 {
        return Ok(Json(BulkComputePhashResponse {
            total_videos: 0,
            queued: 0,
            failed: 0,
        }));
    }

    // Queue all videos using batch API
    let queued = total_videos;
    let failed = 0;

    match state
        .qstash_client
        .queue_compute_phash_batch(video_ids, req.rate, req.parallelism)
        .await
    {
        Ok(_) => {
            log::info!(
                "Successfully queued {} videos for phash computation using batch API",
                total_videos
            );
        }
        Err(e) => {
            log::error!("Batch operation failed: {}", e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    Ok(Json(BulkComputePhashResponse {
        total_videos,
        queued,
        failed,
    }))
}

/// Fetch video IDs from BigQuery table - excludes already processed videos
async fn fetch_video_ids_from_bigquery(
    state: &AppState,
    req: &BulkComputePhashRequest,
) -> Result<Vec<(String, String)>, anyhow::Error> {
    // Query bot-generated videos that haven't been processed yet
    let query = format!(
        "SELECT t1.video_id, t1.publisher_user_id
         FROM (
           SELECT DISTINCT
             JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
             JSON_EXTRACT_SCALAR(params, '$.publisher_user_id') AS publisher_user_id
           FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
           WHERE event = 'video_upload_successful'
             AND JSON_EXTRACT_SCALAR(params, '$.country') LIKE '%-BOT'
         ) AS t1
         WHERE t1.video_id NOT IN (
           SELECT video_id
           FROM `hot-or-not-feed-intelligence.yral_ds.videohash_phash`
           WHERE video_id IS NOT NULL
         )
         LIMIT {}",
        req.limit
    );

    log::debug!("Executing BigQuery query: {}", query);

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    #[cfg(not(feature = "local-bin"))]
    {
        let response = state
            .bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &request)
            .await?;

        let mut video_data = Vec::new();

        if let Some(rows) = response.rows.as_ref() {
            for row in rows {
                if row.f.len() >= 2 {
                    if let (
                        google_cloud_bigquery::http::tabledata::list::Value::String(video_id),
                        google_cloud_bigquery::http::tabledata::list::Value::String(publisher_id),
                    ) = (&row.f[0].v, &row.f[1].v)
                    {
                        video_data.push((video_id.clone(), publisher_id.clone()));
                    }
                }
            }
        }

        Ok(video_data)
    }

    #[cfg(feature = "local-bin")]
    {
        // Return dummy data for local testing
        Ok(vec![
            ("test_video_1".to_string(), "test_publisher_1".to_string()),
            ("test_video_2".to_string(), "test_publisher_2".to_string()),
        ])
    }
}

/// Store phash to BigQuery using Streaming Insert API (no quota limits)
async fn store_phash_to_bigquery(
    state: &AppState,
    video_id: &str,
    phash: &str,
    metadata: &crate::duplicate_video::phash::VideoMetadata,
) -> Result<(), anyhow::Error> {
    log::info!(
        "Storing phash via streaming insert for video_id: {}",
        video_id
    );

    // Prepare row data
    let row_data = json!({
        "video_id": video_id,
        "phash": phash,
        "num_frames": 10,
        "hash_size": 8,
        "duration": metadata.duration,
        "width": metadata.width as i64,
        "height": metadata.height as i64,
        "fps": metadata.fps,
        "created_at": chrono::Utc::now().to_rfc3339(),
    });

    let request = InsertAllRequest {
        rows: vec![Row {
            insert_id: Some(format!(
                "phash_{}_{}",
                video_id,
                chrono::Utc::now().timestamp_millis()
            )),
            json: row_data,
        }],
        ignore_unknown_values: Some(false),
        skip_invalid_rows: Some(false),
        ..Default::default()
    };

    #[cfg(not(feature = "local-bin"))]
    {
        let result = state
            .bigquery_client
            .tabledata()
            .insert(
                "hot-or-not-feed-intelligence",
                "yral_ds",
                "videohash_phash",
                &request,
            )
            .await?;

        // Check for insert errors
        if let Some(errors) = result.insert_errors {
            if !errors.is_empty() {
                log::error!("BigQuery streaming insert errors: {:?}", errors);
                anyhow::bail!("Failed to insert row: {:?}", errors);
            }
        }

        log::debug!("Successfully inserted phash for video_id: {}", video_id);

        // Also push to kvrocks
        let phash_data = VideohashPhash {
            video_id: video_id.to_string(),
            phash: phash.to_string(),
            num_frames: 10,
            hash_size: 8,
            duration: metadata.duration,
            width: metadata.width as i64,
            height: metadata.height as i64,
            fps: metadata.fps,
            created_at: chrono::Utc::now().to_rfc3339(),
        };
        if let Err(e) = state
            .kvrocks_client
            .store_videohash_phash(&phash_data)
            .await
        {
            log::error!("Error pushing phash to kvrocks: {}", e);
        }
    }

    #[cfg(feature = "local-bin")]
    {
        log::info!("Local mode: would stream insert to BigQuery: {:?}", request);
    }

    Ok(())
}
