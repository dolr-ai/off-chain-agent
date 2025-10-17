use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use utoipa::ToSchema;

use crate::{app_state::AppState, duplicate_video::videohash::VideoHash, AppError};

#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct VideoHashResponse {
    pub video_id: String,
    pub hash: String,
}

#[utoipa::path(
    get,
    path = "/videohash/{video_id}",
    params(
        ("video_id" = String, Path, description = "The video ID to generate hash for")
    ),
    tag = "posts",
    responses(
        (status = 200, description = "Video hash generated successfully", body = VideoHashResponse),
        (status = 500, description = "Failed to generate video hash"),
    )
)]
#[instrument(skip(state))]
pub async fn get_videohash(
    Path(video_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    let video_url = format!(
        "https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{}/downloads/default.mp4",
        video_id
    );

    log::info!("Generating videohash for video_id: {}", video_id);

    let video_hash = VideoHash::from_url(&video_url)
        .await
        .map_err(|e| {
            log::error!("Failed to generate videohash for {}: {}", video_id, e);
            anyhow::anyhow!("Failed to generate videohash: {}", e)
        })?;

    Ok((
        StatusCode::OK,
        Json(VideoHashResponse {
            video_id,
            hash: video_hash.hash,
        }),
    ))
}
