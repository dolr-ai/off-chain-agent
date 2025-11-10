use crate::app_state::AppState;
use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::instrument;
use utoipa::ToSchema;

/// Request to check if a video is duplicate
#[derive(Debug, Deserialize, ToSchema)]
pub struct CheckDuplicateRequest {
    /// Video URL to check (supports any video hosting platform)
    pub video_url: String,

    /// Maximum Hamming distance threshold for similarity matching (default: 30)
    #[serde(default = "default_check_threshold")]
    pub hamming_threshold: u32,
}

fn default_check_threshold() -> u32 {
    30
}

/// Information about the matched duplicate video
#[derive(Debug, Serialize, ToSchema)]
pub struct DuplicateInfo {
    /// Video ID of the matched duplicate
    pub matched_video_id: String,
    /// Hamming distance between the videos (0 = exact match)
    pub hamming_distance: u32,
    /// Phash of the matched video
    pub phash: String,
}

/// Response with duplicate check results
#[derive(Debug, Serialize, ToSchema)]
pub struct CheckDuplicateResponse {
    /// Whether the video is a duplicate
    pub is_duplicate: bool,
    /// Information about the duplicate (if found)
    pub duplicate_info: Option<DuplicateInfo>,
    /// Computed phash for the input video
    pub computed_phash: String,
    /// Hamming distance threshold used
    pub threshold_used: u32,
}

/// Check if a video URL is a duplicate using Milvus vector search
///
/// This endpoint:
/// - Downloads the video from any URL
/// - Computes the perceptual hash (phash)
/// - Checks Redis for exact matches (Tier 1)
/// - Checks Milvus for similar matches (Tier 2)
/// - Returns duplicate information if found
/// - Does NOT store the video in the database (read-only operation)
#[utoipa::path(
    post,
    path = "/check_duplicate",
    request_body = CheckDuplicateRequest,
    tag = "milvus",
    responses(
        (status = 200, description = "Duplicate check completed", body = CheckDuplicateResponse),
        (status = 400, description = "Invalid request"),
        (status = 500, description = "Internal server error"),
        (status = 503, description = "Milvus service unavailable")
    )
)]
#[instrument(skip(state))]
pub async fn check_duplicate_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CheckDuplicateRequest>,
) -> Result<Json<CheckDuplicateResponse>, StatusCode> {
    log::info!(
        "Checking duplicate for video URL: {} with threshold {}",
        req.video_url,
        req.hamming_threshold
    );

    // Compute phash from any video URL
    let (phash, _metadata) = crate::duplicate_video::phash::compute_phash_from_url(&req.video_url)
        .await
        .map_err(|e| {
            log::error!("Failed to compute phash for URL {}: {}", req.video_url, e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    log::info!("Computed phash: {} for URL: {}", phash, req.video_url);

    // TIER 1: Check Redis for exact match (FAST - <1ms)
    log::debug!("Tier 1: Checking Redis for exact phash match");
    let redis_key = format!("video_phash:{}", phash);
    let mut redis_conn = state.leaderboard_redis_pool.get().await.map_err(|e| {
        log::error!("Failed to get Redis connection: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let exact_match: Option<String> = redis::cmd("GET")
        .arg(&redis_key)
        .query_async(&mut *redis_conn)
        .await
        .map_err(|e| {
            log::error!("Failed to query Redis: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if let Some(matching_video_id) = exact_match {
        log::info!(
            "⚡ EXACT DUPLICATE (Redis): URL {} has identical phash to video {}",
            req.video_url,
            matching_video_id
        );

        return Ok(Json(CheckDuplicateResponse {
            is_duplicate: true,
            duplicate_info: Some(DuplicateInfo {
                matched_video_id: matching_video_id,
                hamming_distance: 0,
                phash: phash.clone(),
            }),
            computed_phash: phash,
            threshold_used: req.hamming_threshold,
        }));
    }

    log::debug!("Tier 1: No exact match in Redis");

    // TIER 2: Check Milvus for similar matches (SLOWER - 10-50ms)
    log::debug!(
        "Tier 2: Checking Milvus for similar videos (Hamming distance <= {})",
        req.hamming_threshold
    );

    if let Some(milvus_client) = &state.milvus_client {
        match crate::milvus::search_similar_videos(milvus_client, &phash, req.hamming_threshold)
            .await
        {
            Ok(results) => {
                if let Some(nearest) = results.first() {
                    log::info!(
                        "🔍 SIMILAR DUPLICATE (Milvus): URL {} matches video {} with distance {}",
                        req.video_url,
                        nearest.video_id,
                        nearest.hamming_distance
                    );

                    return Ok(Json(CheckDuplicateResponse {
                        is_duplicate: true,
                        duplicate_info: Some(DuplicateInfo {
                            matched_video_id: nearest.video_id.clone(),
                            hamming_distance: nearest.hamming_distance,
                            phash: phash.clone(),
                        }),
                        computed_phash: phash,
                        threshold_used: req.hamming_threshold,
                    }));
                } else {
                    log::info!("✨ UNIQUE: URL {} has no similar matches", req.video_url);
                }
            }
            Err(e) => {
                log::error!("Milvus search failed for URL {}: {}", req.video_url, e);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    } else {
        log::warn!("Milvus client not available");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    // No duplicates found
    Ok(Json(CheckDuplicateResponse {
        is_duplicate: false,
        duplicate_info: None,
        computed_phash: phash,
        threshold_used: req.hamming_threshold,
    }))
}
