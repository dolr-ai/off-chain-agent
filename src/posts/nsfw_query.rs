use std::sync::Arc;

use anyhow::Error;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use utoipa::ToSchema;

use crate::{app_state::AppState, kvrocks::KvrocksClient, AppError};

#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct NsfwQueryResponse {
    pub nsfw_probability: Option<f32>,
}

#[utoipa::path(
    get,
    path = "/nsfw_prob/{video_id}",
    params(
        ("video_id" = String, Path, description = "The video ID to query NSFW data for")
    ),
    tag = "posts",
    responses(
        (status = 200, description = "NSFW data found", body = NsfwQueryResponse),
        (status = 404, description = "Video not found"),
        (status = 500, description = "Internal server error"),
    )
)]
#[instrument(skip(state))]
pub async fn get_nsfw_data(
    Path(video_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    let nsfw_probability = query_nsfw(&state.kvrocks_client, &video_id).await?;

    match nsfw_probability {
        Some(probability) => Ok((
            StatusCode::OK,
            Json(NsfwQueryResponse {
                nsfw_probability: Some(probability),
            }),
        )),
        None => Ok((
            StatusCode::NOT_FOUND,
            Json(NsfwQueryResponse {
                nsfw_probability: None,
            }),
        )),
    }
}

#[instrument(skip(kvrocks_client))]
async fn query_nsfw(
    kvrocks_client: &Option<KvrocksClient>,
    video_id: &str,
) -> Result<Option<f32>, Error> {
    let Some(kvrocks) = kvrocks_client else {
        return Ok(None);
    };

    let nsfw_data = kvrocks.get_video_nsfw(video_id).await?;

    let probability = nsfw_data.and_then(|data| data.probability);

    Ok(probability)
}
