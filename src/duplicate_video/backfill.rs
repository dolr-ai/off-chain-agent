use crate::app_state;
use axum::{extract::Query, extract::State, http::HeaderMap, Json};
#[cfg(not(feature = "local-bin"))]
use google_cloud_bigquery::http::job::query::QueryRequest;
#[cfg(not(feature = "local-bin"))]
use log::{error, info, warn};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
#[cfg(not(feature = "local-bin"))]
use std::env;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct BackfillQueryParams {
    #[allow(dead_code)]
    batch_size: Option<usize>,
    #[allow(dead_code)]
    parallelism: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct BackfillResponse {
    message: String,
    videos_queued: usize,
}

#[cfg(not(feature = "local-bin"))]
pub async fn trigger_videohash_backfill(
    State(state): State<Arc<app_state::AppState>>,
    headers: HeaderMap,
    Query(params): Query<BackfillQueryParams>,
) -> Result<Json<BackfillResponse>, StatusCode> {
    // Extract Bearer token from headers
    let auth_token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim_start_matches("Bearer ").to_string())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    // Get token from environment variable
    let expected_token = match env::var("VIDEOHASH_BACKFILL_TOKEN") {
        Ok(token) => token,
        Err(_) => {
            error!("VIDEOHASH_BACKFILL_TOKEN environment variable not set");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    // Validate the bearer token
    if auth_token != expected_token {
        warn!("Unauthorized access attempt to videohash backfill endpoint");
        return Err(StatusCode::UNAUTHORIZED);
    }

    // Get parameters with defaults
    let batch_size = params.batch_size.unwrap_or(100);
    let parallelism = params.parallelism.unwrap_or(10);

    info!(
        "Starting videohash backfill job with batch_size={batch_size}, parallelism={parallelism}"
    );

    // Execute the backfill
    let videos_queued = execute_backfill(&state, batch_size, parallelism)
        .await
        .map_err(|e| {
            error!("Backfill execution error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(BackfillResponse {
        message: format!(
            "Queued {videos_queued} videos for processing with parallelism {parallelism}"
        ),
        videos_queued,
    }))
}

#[cfg(feature = "local-bin")]
pub async fn trigger_videohash_backfill(
    _state: State<Arc<app_state::AppState>>,
    _headers: HeaderMap,
    _params: Query<BackfillQueryParams>,
) -> Result<Json<BackfillResponse>, StatusCode> {
    Ok(Json(BackfillResponse {
        message: "Backfill is not available in local-bin mode".to_string(),
        videos_queued: 0,
    }))
}

#[cfg(not(feature = "local-bin"))]
async fn execute_backfill(
    state: &Arc<app_state::AppState>,
    batch_size: usize,
    parallelism: usize,
) -> anyhow::Result<usize> {
    info!("Using existing BigQuery client from app state");
    let bigquery_client = &state.bigquery_client;

    let query = format!(
        "SELECT
          SUBSTR(uri, 18, LENGTH(uri) - 21) as video_id,
          (SELECT value FROM UNNEST(t.metadata) WHERE name = 'canister_id') AS canister_id,
          (SELECT value FROM UNNEST(t.metadata) WHERE name = 'post_id') AS post_id
        FROM
          `hot-or-not-feed-intelligence`.`yral_ds`.`video_object_table` AS t
        WHERE 
          SUBSTR(uri, 18, LENGTH(uri) - 21) NOT IN (
            SELECT video_id FROM `hot-or-not-feed-intelligence.yral_ds.videohash_original`
          )
          AND t.size > 10000  /* Require at least 10KB for videos */
        ORDER BY updated ASC
        LIMIT {batch_size}"
    );
    info!("Executing BigQuery query: {query}");

    let request = QueryRequest {
        query,
        timeout_ms: Some(200000),
        ..Default::default()
    };

    info!("Sending query to BigQuery...");
    let response = match bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await
    {
        Ok(resp) => {
            info!("BigQuery query executed successfully");
            resp
        }
        Err(e) => {
            error!("BigQuery query failed: {e}");
            return Err(anyhow::anyhow!("BigQuery query failed: {}", e));
        }
    };

    let rows = match response.rows {
        Some(rows) => rows,
        None => return Ok(0),
    };

    info!("Found {} videos to process", rows.len());

    // Queue each video to QStash for processing
    let mut queued_count = 0;

    for row in rows {
        if row.f.len() < 3 {
            continue;
        }

        let video_id = match &row.f[0].v {
            // If it's already a string type, use it directly
            google_cloud_bigquery::http::tabledata::list::Value::String(s) => s.clone(),
            other => {
                // For other types, use debug formatting but extract just the ID
                let raw = format!("{other:?}");
                // Extract just the ID from String("ID") format
                if raw.contains("String(\"") {
                    raw.trim_start_matches("String(\"")
                        .trim_end_matches("\")")
                        .to_string()
                } else {
                    raw.trim_matches(|c| c == '"' || c == '\\').to_string()
                }
            }
        };
        if video_id.is_empty() {
            continue;
        }

        let canister_id_raw = format!("{:?}", row.f[1].v);
        let canister_id = canister_id_raw
            .trim_matches(|c| c == '"' || c == '\\')
            .to_string();
        let post_id_raw = format!("{:?}", row.f[2].v);
        let post_id_str = post_id_raw.trim_matches(|c| c == '"' || c == '\\');
        let post_id = match post_id_str.parse::<u64>() {
            Ok(id) => id,
            Err(e) => {
                warn!(
                    "Invalid post_id format for video {video_id}: {post_id_str} - {e}"
                );
                0
            }
        };

        // Queue to QStash
        if let Err(e) = queue_video_to_qstash(
            &state.qstash_client,
            &video_id,
            &canister_id,
            post_id,
            parallelism,
        )
        .await
        {
            error!("Failed to queue video {video_id}: {e}");
            continue;
        }

        queued_count += 1;
    }

    info!("Successfully queued {queued_count} videos for processing");
    Ok(queued_count)
}

#[cfg(not(feature = "local-bin"))]
async fn queue_video_to_qstash(
    qstash_client: &crate::qstash::client::QStashClient,
    video_id: &str,
    canister_id: &str,
    post_id: u64,
    parallelism: usize,
) -> anyhow::Result<()> {
    use crate::consts::OFF_CHAIN_AGENT_URL;
    use http::header::CONTENT_TYPE;

    // Prepare the video URL
    let video_url = format!(
        "https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{video_id}/downloads/default.mp4"
    );

    // Create request payload - this is specifically for backfill
    let request_data = serde_json::json!({
        "video_id": video_id,
        "video_url": video_url,
        "publisher_data": {
            "canister_id": canister_id,
            "publisher_principal": "unknown", // Default for backfill
            "post_id": post_id
        }
    });

    // Use the dedicated process_single_video endpoint for backfill jobs
    // This avoids the full pipeline that video_deduplication would trigger
    let off_chain_ep = OFF_CHAIN_AGENT_URL
        .join("qstash/process_single_video")
        .unwrap();
    let url = qstash_client
        .base_url
        .join(&format!("publish/{off_chain_ep}"))?;

    // Send to QStash with flow control
    qstash_client
        .client
        .post(url)
        .json(&request_data)
        .header(CONTENT_TYPE, "application/json")
        .header("upstash-method", "POST")
        .header("Upstash-Flow-Control-Key", "VIDEOHASH_BACKFILL")
        .header(
            "Upstash-Flow-Control-Value",
            format!("Parallelism={parallelism}"),
        )
        .send()
        .await?;

    info!("Queued video_id [{video_id}] for processing");
    Ok(())
}
