use crate::app_state::AppState;
use crate::duplicate_video::phash::{
    download_video_from_cloudflare, extract_metadata, PHasher, VideoMetadata,
};
use axum::{extract::State, http::StatusCode, Json};
use google_cloud_bigquery::http::tabledata::insert_all::{InsertAllRequest, Row};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tracing::instrument;
use utoipa::ToSchema;
use uuid::Uuid;

/// Request to compute phash for a video
#[derive(Debug, Deserialize, ToSchema)]
pub struct ComputePhashRequest {
    /// Video ID to process
    pub video_id: String,
}

/// Response with computed phash and metadata
#[derive(Debug, Serialize, ToSchema)]
pub struct ComputePhashResponse {
    /// Video ID
    pub video_id: String,
    /// Binary phash string (640 characters of '0' and '1')
    pub phash: String,
    /// Number of frames used for hashing
    pub num_frames: usize,
    /// Hash size (8x8 = 64 bits per frame)
    pub hash_size: u32,
    /// Video duration in seconds
    pub duration: f64,
    /// Video width in pixels
    pub width: u32,
    /// Video height in pixels
    pub height: u32,
    /// Frames per second
    pub fps: f64,
}

/// Compute phash for a video (testing endpoint)
#[utoipa::path(
    post,
    path = "/compute_phash",
    request_body = ComputePhashRequest,
    tag = "videos",
    responses(
        (status = 200, description = "Phash computed successfully", body = ComputePhashResponse),
        (status = 400, description = "Invalid request"),
        (status = 500, description = "Failed to compute phash")
    )
)]
#[instrument(skip(state))]
pub async fn compute_phash_api(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ComputePhashRequest>,
) -> Result<Json<ComputePhashResponse>, StatusCode> {
    log::info!("Computing phash for video ID (API): {}", req.video_id);

    // Create temp directory for video
    let temp_dir = std::env::temp_dir().join(format!("phash_api_{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&temp_dir).await.map_err(|e| {
        log::error!("Failed to create temp directory: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let video_path = temp_dir.join(format!("{}.mp4", req.video_id));

    // Download video from Cloudflare
    if let Err(e) = download_video_from_cloudflare(&req.video_id, &video_path).await {
        log::error!("Failed to download video {}: {}", req.video_id, e);
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    // Compute phash in blocking task
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

    // Store in BigQuery
    #[cfg(not(feature = "local-bin"))]
    {
        if let Err(e) = store_phash_to_bigquery(&state, &req.video_id, &phash, &metadata).await {
            log::error!("Failed to store phash to BigQuery: {}", e);
            let _ = tokio::fs::remove_dir_all(&temp_dir).await;
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    // Cleanup temp directory
    if let Err(e) = tokio::fs::remove_dir_all(&temp_dir).await {
        log::warn!("Failed to cleanup temp directory: {}", e);
    }

    log::info!(
        "Successfully computed phash for video {}: length={}, duration={}s",
        req.video_id,
        phash.len(),
        metadata.duration
    );

    Ok(Json(ComputePhashResponse {
        video_id: req.video_id,
        phash,
        num_frames: 10,
        hash_size: 8,
        duration: metadata.duration,
        width: metadata.width,
        height: metadata.height,
        fps: metadata.fps,
    }))
}

/// Store phash to BigQuery using Streaming Insert API (no quota limits)
async fn store_phash_to_bigquery(
    state: &AppState,
    video_id: &str,
    phash: &str,
    metadata: &VideoMetadata,
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
    }

    #[cfg(feature = "local-bin")]
    {
        log::info!("Local mode: would stream insert to BigQuery: {:?}", request);
    }

    Ok(())
}
