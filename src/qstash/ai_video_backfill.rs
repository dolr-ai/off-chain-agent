//! AI Video Detection Backfill endpoints
//!
//! Two-endpoint architecture using QStash:
//!
//! 1. POST /qstash/ai_video_backfill
//!    - Spawns a background task to fetch ALL unprocessed video IDs from BigQuery
//!    - Returns immediately while enqueueing happens in background
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
    ai_video_detector::{AiVideoDetectorClient, Verdict},
    app_state::AppState,
    consts::{get_cloudflare_stream_url, get_storj_video_url},
    qstash::client::QStashClient,
};
use google_cloud_bigquery::client::Client as BigQueryClient;

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

// ============================================================================
// Endpoint 1: Enqueue videos for processing
// ============================================================================

/// Background task that fetches ALL unprocessed videos and enqueues them to QStash
async fn backfill_videos_task(bigquery_client: BigQueryClient, qstash_client: QStashClient) {
    log::info!("AI Video Backfill: Background task started - fetching ALL unprocessed videos");

    let mut total_queued = 0usize;
    let mut batch_num = 0usize;

    loop {
        batch_num += 1;

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

        let resp = match bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &req)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                log::error!(
                    "AI Video Backfill: BigQuery query failed at batch {}: {}",
                    batch_num,
                    e
                );
                return;
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

        if let Err(e) = qstash_client
            .queue_ai_video_backfill_batch(video_data)
            .await
        {
            log::error!(
                "AI Video Backfill: Failed to enqueue batch {} to QStash: {}",
                batch_num,
                e
            );
            return;
        }

        total_queued += batch_count;

        if batch_count < BATCH_SIZE as usize {
            break;
        }
    }

    log::info!(
        "AI Video Backfill: Background task completed - {} total videos queued in {} batches",
        total_queued,
        batch_num
    );
}

/// Starts backfill in background and returns immediately
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

    log::info!("AI Video Backfill: Spawning background task");

    // Clone what we need for the background task
    let bigquery_client = state.bigquery_client.clone();
    let qstash_client = state.qstash_client.clone();

    // Spawn the background task
    tokio::spawn(async move {
        backfill_videos_task(bigquery_client, qstash_client).await;
    });

    // Return immediately
    Json(BackfillResponse {
        status: "started",
        message: "Backfill job started in background. Check logs for progress.".to_string(),
        videos_queued: 0,
    })
    .into_response()
}

// ============================================================================
// Endpoint 2: Process a single video (called by QStash)
// ============================================================================

/// Processes a single video - called by QStash
/// Uses exact same detection logic as duplicate.rs
pub async fn ai_video_backfill_process_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ProcessVideoRequest>,
) -> Response {
    let video_id = req.video_id;
    let publisher_user_id = req.publisher_user_id;

    log::info!("AI Video Backfill: Processing video {}", video_id);

    // Use the same AiVideoDetectorClient as duplicate.rs
    let ai_detector = AiVideoDetectorClient::new();

    if !ai_detector.is_configured() {
        log::error!("AI_VIDEO_DETECTOR_API_KEY not configured");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "AI_VIDEO_DETECTOR_API_KEY not configured",
        )
            .into_response();
    }

    // Try Storj first, fallback to Cloudflare (same pattern as duplicate.rs)
    let storj_url = get_storj_video_url(&publisher_user_id, &video_id, false);
    let cf_url = get_cloudflare_stream_url(&video_id);

    log::info!(
        "AI Video Backfill: Trying Storj URL first for video {}",
        video_id
    );

    let detection_result = match ai_detector.detect_video(&storj_url).await {
        Ok(response) => Ok(response),
        Err(storj_err) => {
            log::warn!(
                "Storj detection failed for {}: {}. Trying Cloudflare...",
                video_id,
                storj_err
            );
            ai_detector.detect_video(&cf_url).await
        }
    };

    match detection_result {
        Ok(response) => {
            log::info!(
                "AI Video Backfill: {} -> {:?} (conf: {:.2})",
                video_id,
                response.verdict,
                response.confidence
            );

            match response.verdict {
                Verdict::Allow => {
                    // Auto-approve: insert into kvrocks and BigQuery
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

                    let insert_query = format!(
                        "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
                         (video_id, is_approved, created_at)
                         VALUES ('{}', TRUE, CURRENT_TIMESTAMP())",
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

                    log::info!("AI Video Backfill: {} APPROVED", video_id);
                }
                Verdict::Block => {
                    // For backfill: insert as blocked (is_approved=FALSE) so it's excluded from future queries
                    // Also delete from kvrocks if it exists
                    let _ = state
                        .kvrocks_client
                        .delete_user_uploaded_content_approval(&video_id)
                        .await;

                    let insert_query = format!(
                        "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
                         (video_id, is_approved, created_at)
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

                    log::info!("AI Video Backfill: {} BLOCKED", video_id);
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

            (StatusCode::OK, format!("{:?}", response.verdict)).into_response()
        }
        Err(e) => {
            let error_str = e.to_string();

            // Check if it's a 404 (video not found on any storage)
            if error_str.contains("404") {
                log::warn!(
                    "AI Video Backfill: {} NOT_FOUND - sending to review",
                    video_id
                );

                // Treat as Review - insert into ugc_content_approval with is_approved=FALSE
                let insert_query = format!(
                    "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
                     (video_id, is_approved, created_at)
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

                // Return 200 so QStash doesn't retry
                (StatusCode::OK, "REVIEW").into_response()
            } else {
                // Other errors (API down, network issues) - return 500 so QStash retries
                log::error!(
                    "AI Video Backfill: {} ERROR - detection failed: {}",
                    video_id,
                    e
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Detection failed: {}", e),
                )
                    .into_response()
            }
        }
    }
}
