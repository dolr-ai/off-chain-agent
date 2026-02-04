//! AI Video Detection Backfill endpoint
//!
//! POST /qstash/ai_video_backfill
//! Query params:
//!   - dry_run: Only detect, don't update anything (default: false)
//!
//! Processes ALL unprocessed videos in one go.
//! Returns immediately and processes in background.

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

#[derive(Debug, Deserialize)]
pub struct BackfillParams {
    #[serde(default)]
    dry_run: bool,
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

#[derive(Debug, Serialize)]
pub struct BackfillAckResponse {
    status: &'static str,
    message: String,
    dry_run: bool,
}

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

const BATCH_SIZE: u32 = 100;

/// Background task that processes all unprocessed videos in batches
async fn process_backfill(state: Arc<AppState>, dry_run: bool) {
    let api_key = match std::env::var("AI_VIDEO_DETECTOR_API_KEY") {
        Ok(key) if !key.is_empty() => key,
        _ => {
            log::error!("AI_VIDEO_DETECTOR_API_KEY not set");
            return;
        }
    };

    let api_url = std::env::var("AI_VIDEO_DETECTOR_URL")
        .unwrap_or_else(|_| "https://ai-video-detector.fly.dev".to_string());
    let detect_url = format!("{}/detect", api_url);

    let http = reqwest::Client::new();
    let mut total_processed = 0usize;
    let mut total_allow = 0usize;
    let mut total_block = 0usize;
    let mut total_review = 0usize;
    let mut total_errors = 0usize;
    let mut batch_num = 0usize;

    loop {
        batch_num += 1;

        // Query next batch of unprocessed videos (with publisher_user_id for Storj URL)
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
                log::error!("BigQuery query failed: {}", e);
                break;
            }
        };

        let rows = resp.rows.unwrap_or_default();

        if rows.is_empty() {
            log::info!("AI Video Backfill: No more videos to process");
            break;
        }

        log::info!(
            "AI Video Backfill: Batch {} - Processing {} videos",
            batch_num,
            rows.len()
        );

        let batch_total = rows.len();
        let mut batch_processed = 0usize;

        for row in &rows {
            // Extract video_id and publisher_user_id (strip quotes if present)
            let video_id = match row.f.first().and_then(|c| match &c.v {
                Value::String(s) => Some(s.trim_matches('"').to_string()),
                _ => None,
            }) {
                Some(id) => id,
                None => continue,
            };

            let publisher_user_id = match row.f.get(1).and_then(|c| match &c.v {
                Value::String(s) => Some(s.trim_matches('"').to_string()),
                _ => None,
            }) {
                Some(id) => id,
                None => continue,
            };

            batch_processed += 1;
            total_processed += 1;

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
                Ok(det) => match det.verdict {
                    Verdict::Allow => {
                        total_allow += 1;
                        if !dry_run {
                            if let Err(e) = state
                                .kvrocks_client
                                .update_user_uploaded_content_approval_status(&video_id, true)
                                .await
                            {
                                log::error!(
                                    "[B{} {}/{}] {} ALLOW - kvrocks failed: {}",
                                    batch_num,
                                    batch_processed,
                                    batch_total,
                                    video_id,
                                    e
                                );
                                total_errors += 1;
                            } else {
                                let update_query = format!(
                                        "UPDATE `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
                                         SET is_approved = TRUE WHERE video_id = '{}'",
                                        video_id.replace('\'', "''")
                                    );
                                let req = QueryRequest {
                                    query: update_query,
                                    ..Default::default()
                                };
                                let _ = state
                                    .bigquery_client
                                    .job()
                                    .query("hot-or-not-feed-intelligence", &req)
                                    .await;
                                log::info!(
                                    "[B{} {}/{}] {} ALLOW (conf: {:.2}) -> APPROVED",
                                    batch_num,
                                    batch_processed,
                                    batch_total,
                                    video_id,
                                    det.confidence
                                );
                            }
                        } else {
                            log::info!(
                                "[B{} {}/{}] {} ALLOW (conf: {:.2}) -> would_approve",
                                batch_num,
                                batch_processed,
                                batch_total,
                                video_id,
                                det.confidence
                            );
                        }
                    }
                    Verdict::Block => {
                        total_block += 1;
                        if !dry_run {
                            let _ = state
                                .kvrocks_client
                                .delete_user_uploaded_content_approval(&video_id)
                                .await;
                            let delete_query = format!(
                                    "DELETE FROM `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
                                     WHERE video_id = '{}'",
                                    video_id.replace('\'', "''")
                                );
                            let req = QueryRequest {
                                query: delete_query,
                                ..Default::default()
                            };
                            let _ = state
                                .bigquery_client
                                .job()
                                .query("hot-or-not-feed-intelligence", &req)
                                .await;
                            log::info!(
                                "[B{} {}/{}] {} BLOCK (conf: {:.2}) -> DELETED",
                                batch_num,
                                batch_processed,
                                batch_total,
                                video_id,
                                det.confidence
                            );
                        } else {
                            log::info!(
                                "[B{} {}/{}] {} BLOCK (conf: {:.2}) -> would_delete",
                                batch_num,
                                batch_processed,
                                batch_total,
                                video_id,
                                det.confidence
                            );
                        }
                    }
                    Verdict::Review => {
                        total_review += 1;
                        if !dry_run {
                            if let Err(e) = state
                                .kvrocks_client
                                .update_user_uploaded_content_approval_status(&video_id, false)
                                .await
                            {
                                log::error!(
                                    "[B{} {}/{}] {} REVIEW - kvrocks failed: {}",
                                    batch_num,
                                    batch_processed,
                                    batch_total,
                                    video_id,
                                    e
                                );
                                total_errors += 1;
                            } else {
                                let insert_query = format!(
                                        "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval` (video_id, is_approved, created_at)
                                         VALUES ('{}', FALSE, CURRENT_TIMESTAMP())",
                                        video_id.replace('\'', "''")
                                    );
                                let req = QueryRequest {
                                    query: insert_query,
                                    ..Default::default()
                                };
                                let _ = state
                                    .bigquery_client
                                    .job()
                                    .query("hot-or-not-feed-intelligence", &req)
                                    .await;
                                log::info!(
                                    "[B{} {}/{}] {} REVIEW (conf: {:.2}) -> INSERTED_PENDING",
                                    batch_num,
                                    batch_processed,
                                    batch_total,
                                    video_id,
                                    det.confidence
                                );
                            }
                        } else {
                            log::info!(
                                "[B{} {}/{}] {} REVIEW (conf: {:.2}) -> would_insert_pending",
                                batch_num,
                                batch_processed,
                                batch_total,
                                video_id,
                                det.confidence
                            );
                        }
                    }
                },
                Err(e) => {
                    total_errors += 1;
                    log::error!(
                        "[B{} {}/{}] {} ERROR: {}",
                        batch_num,
                        batch_processed,
                        batch_total,
                        video_id,
                        e
                    );
                }
            }
        }

        log::info!(
            "AI Video Backfill: Batch {} complete. Running totals: processed={}, allow={}, block={}, review={}, errors={}",
            batch_num,
            total_processed,
            total_allow,
            total_block,
            total_review,
            total_errors
        );
    }

    log::info!(
        "AI Video Backfill COMPLETE: total_processed={}, allow={}, block={}, review={}, errors={}, batches={}, dry_run={}",
        total_processed,
        total_allow,
        total_block,
        total_review,
        total_errors,
        batch_num,
        dry_run
    );
}

pub async fn ai_video_backfill_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<BackfillParams>,
) -> Response {
    // Validate API key exists before spawning
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

    log::info!("AI Video Backfill started: dry_run={}", params.dry_run);

    // Spawn background task
    let dry_run = params.dry_run;
    tokio::spawn(async move {
        process_backfill(state, dry_run).await;
    });

    // Return immediately
    Json(BackfillAckResponse {
        status: "started",
        message: "Backfill job started in background. Processing ALL unprocessed videos. Check logs for progress.".to_string(),
        dry_run: params.dry_run,
    })
    .into_response()
}
