//! AI Video Detection Backfill endpoints
//!
//! Two-endpoint architecture using QStash:
//!
//! 1. POST /qstash/ai_video_backfill
//!    - Fetches ALL unprocessed video IDs from BigQuery (in batches)
//!    - Enqueues each video to QStash queue for processing
//!    - Returns total count of videos queued
//!
//! 2. POST /qstash/ai_video_backfill_process
//!    - Processes a single video (called by QStash)
//!    - Tries Storj URL first, falls back to Cloudflare
//!    - Updates kvrocks and BigQuery based on verdict

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    response::{IntoResponse, Response},
    Json,
};
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::http::tabledata::list::Value;
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{
    app_state::AppState,
    consts::{get_cloudflare_stream_url, get_storj_video_url},
};

// ============================================================================
// Request/Response types
// ============================================================================

/// Batch size for BigQuery queries
const BATCH_SIZE: u32 = 1000;

#[derive(Debug, Deserialize)]
pub struct BackfillParams {
    // No params needed - we fetch ALL videos
}

#[derive(Debug, Serialize)]
pub struct BackfillResponse {
    status: &'static str,
    message: String,
    videos_queued: usize,
}

#[derive(Debug, Deserialize)]
pub struct ProcessVideoRequest {
    video_id: String,
    publisher_user_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum Verdict {
    Allow,
    Block,
    Review,
}

#[derive(Debug, Deserialize)]
struct DetectionResponse {
    verdict: Verdict,
    confidence: f64,
}

// ============================================================================
// AI Video Detection
// ============================================================================

async fn detect_video(
    client: &reqwest::Client,
    api_url: &str,
    api_key: &str,
    video_url: &str,
) -> Result<DetectionResponse, String> {
    let form = reqwest::multipart::Form::new().text("url", video_url.to_string());

    let resp = client
        .post(api_url)
        .header("x-api-key", api_key)
        .multipart(form)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !resp.status().is_success() {
        let err = resp.text().await.unwrap_or_default();
        return Err(format!("API error: {}", err));
    }

    resp.json()
        .await
        .map_err(|e| format!("Failed to parse response: {}", e))
}

// ============================================================================
// Endpoint 1: Enqueue videos for processing
// ============================================================================

/// Fetches ALL unprocessed videos from BigQuery and enqueues them to QStash
pub async fn ai_video_backfill_handler(
    State(state): State<Arc<AppState>>,
    Query(_params): Query<BackfillParams>,
) -> Response {
    // Validate API key exists
    if std::env::var("AI_VIDEO_DETECTOR_API_KEY")
        .map(|k| k.is_empty())
        .unwrap_or(true)
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "AI_VIDEO_DETECTOR_API_KEY not set",
        )
            .into_response();
    }

    log::info!("AI Video Backfill: Starting to fetch ALL unprocessed videos");

    let mut total_queued = 0usize;
    let mut batch_num = 0usize;

    // Loop through all batches until no more videos
    loop {
        batch_num += 1;

        // Query next batch of unprocessed videos
        let query = format!(
            "SELECT video_id, publisher_user_id FROM (
              SELECT
                JSON_EXTRACT_SCALAR(params, '$.video_id') AS video_id,
                JSON_EXTRACT_SCALAR(params, '$.publisher_user_id') AS publisher_user_id
              FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics`
              WHERE event = 'video_upload_success' OR event = 'video_upload_successful'
            )
            WHERE video_id IS NOT NULL
              AND publisher_user_id IS NOT NULL
              AND video_id NOT IN (
                SELECT video_id FROM `hot-or-not-feed-intelligence.yral_ds.ai_ugc`
              )
              AND video_id NOT IN (
                SELECT video_id FROM `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
              )
            LIMIT {}",
            BATCH_SIZE
        );

        let req = QueryRequest {
            query,
            ..Default::default()
        };

        let resp = match state
            .bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &req)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                log::error!("BigQuery query failed at batch {}: {}", batch_num, e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("BigQuery query failed: {}", e),
                )
                    .into_response();
            }
        };

        let rows = resp.rows.unwrap_or_default();

        if rows.is_empty() {
            log::info!(
                "AI Video Backfill: No more videos to process after {} batches",
                batch_num - 1
            );
            break;
        }

        // Extract video data (strip quotes if present)
        let video_data: Vec<(String, String)> = rows
            .iter()
            .filter_map(|row| {
                if row.f.len() >= 2 {
                    let video_id = match &row.f[0].v {
                        Value::String(s) => s.trim_matches('"').to_string(),
                        _ => return None,
                    };
                    let publisher_user_id = match &row.f[1].v {
                        Value::String(s) => s.trim_matches('"').to_string(),
                        _ => return None,
                    };
                    Some((video_id, publisher_user_id))
                } else {
                    None
                }
            })
            .collect();

        let batch_count = video_data.len();
        log::info!(
            "AI Video Backfill: Batch {} - Enqueueing {} videos to QStash",
            batch_num,
            batch_count
        );

        // Enqueue batch to QStash
        if let Err(e) = state
            .qstash_client
            .queue_ai_video_backfill_batch(video_data)
            .await
        {
            log::error!("Failed to enqueue batch {} to QStash: {}", batch_num, e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to enqueue batch {}: {}", batch_num, e),
            )
                .into_response();
        }

        total_queued += batch_count;

        // If we got less than BATCH_SIZE, we're done
        if batch_count < BATCH_SIZE as usize {
            break;
        }
    }

    log::info!(
        "AI Video Backfill: Completed - {} total videos queued in {} batches",
        total_queued,
        batch_num
    );

    Json(BackfillResponse {
        status: "queued",
        message: format!(
            "Queued {} videos for processing via QStash ({} batches)",
            total_queued, batch_num
        ),
        videos_queued: total_queued,
    })
    .into_response()
}

// ============================================================================
// Endpoint 2: Process a single video (called by QStash)
// ============================================================================

/// Processes a single video - called by QStash
pub async fn ai_video_backfill_process_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ProcessVideoRequest>,
) -> Response {
    let video_id = req.video_id;
    let publisher_user_id = req.publisher_user_id;

    log::info!("AI Video Backfill: Processing video {}", video_id);

    let api_key = match std::env::var("AI_VIDEO_DETECTOR_API_KEY") {
        Ok(key) if !key.is_empty() => key,
        _ => {
            log::error!("AI_VIDEO_DETECTOR_API_KEY not set");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "AI_VIDEO_DETECTOR_API_KEY not set",
            )
                .into_response();
        }
    };

    let api_url = std::env::var("AI_VIDEO_DETECTOR_URL")
        .unwrap_or_else(|_| "https://ai-video-detector.fly.dev".to_string());
    let detect_url = format!("{}/detect", api_url);

    let http = reqwest::Client::new();

    // Try Storj first, fallback to Cloudflare (same pattern as duplicate.rs)
    let storj_url = get_storj_video_url(&publisher_user_id, &video_id, false);
    let detection = match detect_video(&http, &detect_url, &api_key, &storj_url).await {
        Ok(response) => Ok(response),
        Err(storj_err) => {
            log::warn!(
                "Storj detection failed for {}, trying Cloudflare: {}",
                video_id,
                storj_err
            );
            let cf_url = get_cloudflare_stream_url(&video_id);
            detect_video(&http, &detect_url, &api_key, &cf_url).await
        }
    };

    match detection {
        Ok(det) => {
            log::info!(
                "AI Video Backfill: {} -> {:?} (conf: {:.2})",
                video_id,
                det.verdict,
                det.confidence
            );

            match det.verdict {
                Verdict::Allow => {
                    // Auto-approve: update kvrocks and BigQuery
                    if let Err(e) = state
                        .kvrocks_client
                        .update_user_uploaded_content_approval_status(&video_id, true)
                        .await
                    {
                        log::error!("Failed to update kvrocks for {}: {}", video_id, e);
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("kvrocks error: {}", e),
                        )
                            .into_response();
                    }

                    let update_query = format!(
                        "UPDATE `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
                         SET is_approved = TRUE WHERE video_id = '{}'",
                        video_id.replace('\'', "''")
                    );
                    let bq_req = QueryRequest {
                        query: update_query,
                        ..Default::default()
                    };
                    let _ = state
                        .bigquery_client
                        .job()
                        .query("hot-or-not-feed-intelligence", &bq_req)
                        .await;

                    log::info!("AI Video Backfill: {} APPROVED", video_id);
                }
                Verdict::Block => {
                    // Delete from kvrocks and BigQuery
                    let _ = state
                        .kvrocks_client
                        .delete_user_uploaded_content_approval(&video_id)
                        .await;

                    let delete_query = format!(
                        "DELETE FROM `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
                         WHERE video_id = '{}'",
                        video_id.replace('\'', "''")
                    );
                    let bq_req = QueryRequest {
                        query: delete_query,
                        ..Default::default()
                    };
                    let _ = state
                        .bigquery_client
                        .job()
                        .query("hot-or-not-feed-intelligence", &bq_req)
                        .await;

                    log::info!("AI Video Backfill: {} BLOCKED/DELETED", video_id);
                }
                Verdict::Review => {
                    // Insert as pending review
                    if let Err(e) = state
                        .kvrocks_client
                        .update_user_uploaded_content_approval_status(&video_id, false)
                        .await
                    {
                        log::error!("Failed to update kvrocks for {}: {}", video_id, e);
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("kvrocks error: {}", e),
                        )
                            .into_response();
                    }

                    let insert_query = format!(
                        "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval` (video_id, is_approved, created_at)
                         VALUES ('{}', FALSE, CURRENT_TIMESTAMP())",
                        video_id.replace('\'', "''")
                    );
                    let bq_req = QueryRequest {
                        query: insert_query,
                        ..Default::default()
                    };
                    let _ = state
                        .bigquery_client
                        .job()
                        .query("hot-or-not-feed-intelligence", &bq_req)
                        .await;

                    log::info!("AI Video Backfill: {} REVIEW (pending)", video_id);
                }
            }

            (StatusCode::OK, format!("{:?}", det.verdict)).into_response()
        }
        Err(e) => {
            log::error!("AI Video Backfill: {} ERROR - {}", video_id, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Detection failed: {}", e),
            )
                .into_response()
        }
    }
}
