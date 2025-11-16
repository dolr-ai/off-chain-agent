use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::instrument;
use utoipa::ToSchema;

use crate::app_state::AppState;

use super::frame_diff::{compare_videos, upload_frame_to_gcs};

#[derive(Debug, Deserialize, ToSchema)]
pub struct CompareVideosRequest {
    pub publisher_user_id_1: String,
    pub video_id_1: String,
    pub publisher_user_id_2: String,
    pub video_id_2: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CompareVideosResponse {
    pub video_id_1: String,
    pub video_id_2: String,
    pub video1_phash: String,
    pub video2_phash: String,
    pub total_frames: usize,
    pub differing_frames_count: usize,
    pub frame_comparisons: Vec<FrameComparison>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct FrameComparison {
    pub frame_index: usize,
    pub hamming_distance: u32,
    pub video1_url: String,
    pub video2_url: String,
}

/// Compare two videos frame-by-frame and return URLs of combined images for differing frames
#[utoipa::path(
    post,
    path = "/compare",
    request_body = CompareVideosRequest,
    responses(
        (status = 200, description = "Videos compared successfully", body = CompareVideosResponse),
        (status = 400, description = "Bad request - video download failed"),
        (status = 500, description = "Internal server error")
    ),
    tag = "video"
)]
#[instrument(skip(state))]
pub async fn compare_videos_api(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CompareVideosRequest>,
) -> Result<Json<CompareVideosResponse>, StatusCode> {
    log::info!(
        "Compare videos API called: {} vs {}",
        req.video_id_1,
        req.video_id_2
    );

    // Compare videos and get differing frame indices with hamming distances
    let (differing_frames, frames1, frames2, video1_phash, video2_phash) = compare_videos(
        &req.publisher_user_id_1,
        &req.video_id_1,
        &req.publisher_user_id_2,
        &req.video_id_2,
    )
    .await
    .map_err(|e| {
        log::error!(
            "Failed to compare videos {} vs {}: {}",
            req.video_id_1,
            req.video_id_2,
            e
        );
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let total_frames = frames1.len();

    // For each differing frame, upload both frames separately to GCS
    let mut frame_comparisons = Vec::new();

    for &(frame_index, hamming_distance) in &differing_frames {
        if frame_index >= frames1.len() || frame_index >= frames2.len() {
            log::warn!("Frame index {} out of bounds, skipping", frame_index);
            continue;
        }

        let frame1 = &frames1[frame_index];
        let frame2 = &frames2[frame_index];

        // Upload both frames separately
        let (video1_url, video2_url) = tokio::try_join!(
            upload_frame_to_gcs(
                state.gcs_client.clone(),
                frame1,
                &req.video_id_1,
                &req.video_id_2,
                frame_index,
                1
            ),
            upload_frame_to_gcs(
                state.gcs_client.clone(),
                frame2,
                &req.video_id_1,
                &req.video_id_2,
                frame_index,
                2
            )
        )
        .map_err(|e| {
            log::error!(
                "Failed to upload frames {} for videos {} vs {}: {}",
                frame_index,
                req.video_id_1,
                req.video_id_2,
                e
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        frame_comparisons.push(FrameComparison {
            frame_index,
            hamming_distance,
            video1_url,
            video2_url,
        });
    }

    log::info!(
        "Successfully compared videos {} vs {}: {} differing frames out of {}",
        req.video_id_1,
        req.video_id_2,
        frame_comparisons.len(),
        total_frames
    );

    Ok(Json(CompareVideosResponse {
        video_id_1: req.video_id_1,
        video_id_2: req.video_id_2,
        video1_phash,
        video2_phash,
        total_frames,
        differing_frames_count: frame_comparisons.len(),
        frame_comparisons,
    }))
}
