use std::sync::Arc;

use anyhow::Error;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use google_cloud_bigquery::http::job::query::QueryRequest;
use serde::{Deserialize, Serialize};
use tracing::instrument;
use utoipa::ToSchema;

use crate::{app_state::AppState, AppError};

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
    let nsfw_probability = query_nsfw_from_bigquery(&state.bigquery_client, &video_id).await?;

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

#[instrument(skip(bigquery_client))]
async fn query_nsfw_from_bigquery(
    bigquery_client: &google_cloud_bigquery::client::Client,
    video_id: &str,
) -> Result<Option<f32>, Error> {
    let query = format!(
        "SELECT probability FROM `hot-or-not-feed-intelligence.yral_ds.video_nsfw_agg` WHERE video_id = '{}'",
        video_id
    );

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    let result = bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await?;

    // Check if we have any rows
    let rows = result.rows.unwrap_or_default();
    if rows.is_empty() {
        return Ok(None);
    }

    // Get the first row and extract probability
    let row = &rows[0];
    let probability = match &row.f[0].v {
        google_cloud_bigquery::http::tabledata::list::Value::String(s) => s.parse::<f32>().ok(),
        _ => None,
    };

    Ok(probability)
}
