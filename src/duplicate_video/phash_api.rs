use crate::app_state::AppState;
use crate::duplicate_video::phash::compute_phash_from_cloudflare;
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::instrument;
use utoipa::ToSchema;

/// Request to compute phash for a video
#[derive(Debug, Deserialize, ToSchema)]
pub struct ComputePhashRequest {
    /// Video ID to process
    pub video_id: String,
    pub publisher_user_id: String,
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
#[instrument(skip(_state))]
pub async fn compute_phash_api(
    State(_state): State<Arc<AppState>>,
    Json(req): Json<ComputePhashRequest>,
) -> Result<Json<ComputePhashResponse>, StatusCode> {
    log::info!("Computing phash for video ID (API): {}", req.video_id);

    let (phash, metadata) = compute_phash_from_cloudflare(&req.publisher_user_id, &req.video_id)
        .await
        .map_err(|e| {
            log::error!("Failed to compute phash for {}: {}", req.video_id, e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

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
