use crate::{
    app_state::AppState,
    consts::USER_POST_SERVICE_CANISTER_ID,
    rewards::{
        config::RewardConfig,
        history::{HistoryTracker, RewardRecord, ViewRecord},
    },
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use candid::Principal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use yral_canisters_client::user_post_service::{Result1, UserPostService};

#[cfg(not(feature = "local-bin"))]
use google_cloud_bigquery::http::job::query::QueryRequest;

/// Query BigQuery to get post_id from video_id
#[cfg(not(feature = "local-bin"))]
async fn query_post_id_from_bigquery(
    bigquery_client: &google_cloud_bigquery::client::Client,
    video_id: &str,
) -> Option<String> {
    let query = format!(
        "SELECT JSON_EXTRACT_SCALAR(params, '$.post_id') as post_id \
         FROM `hot-or-not-feed-intelligence.analytics_335143420.test_events_analytics` \
         WHERE event = 'video_upload_successful' \
         AND JSON_EXTRACT_SCALAR(params, '$.video_id') = '{}' \
         LIMIT 1",
        video_id
    );

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    match bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await
    {
        Ok(result) => {
            let rows = result.rows.unwrap_or_default();
            if rows.is_empty() {
                log::warn!("No post_id found in BigQuery for video_id: {}", video_id);
                return None;
            }

            // Extract post_id from first row
            let row = &rows[0];
            match &row.f[0].v {
                google_cloud_bigquery::http::tabledata::list::Value::String(s) => {
                    log::debug!("Found post_id {} for video_id {}", s, video_id);
                    Some(s.clone())
                }
                _ => {
                    log::warn!(
                        "Unexpected value type for post_id in BigQuery for video_id: {}",
                        video_id
                    );
                    None
                }
            }
        }
        Err(e) => {
            log::error!("Failed to query BigQuery for post_id: {}", e);
            None
        }
    }
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct PaginationParams {
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub _offset: usize,
}

fn default_limit() -> usize {
    100
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ViewHistoryResponse {
    pub views: Vec<ViewRecord>,
    pub total: usize,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RewardHistoryResponse {
    pub rewards: Vec<RewardRecord>,
    pub total: usize,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct VideoStatsResponse {
    pub video_id: String,
    pub view_count: u64,
    pub last_milestone: u64,
    pub next_milestone: u64,
    pub views_to_next_milestone: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ConfigResponse {
    pub config: Option<RewardConfig>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct BulkVideoStatsRequest {
    pub video_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct VideoStats {
    pub count: u64,
    pub total_count_loggedin: u64,
    pub total_count_all: u64,
    pub last_milestone: u64,
}

pub type BulkVideoStatsResponse = HashMap<String, VideoStats>;

#[derive(Debug, Serialize, ToSchema)]
pub struct VideoStatsV2 {
    pub video_id: String,
    pub count: u64,
    pub total_count_loggedin: u64,
    pub total_count_all: u64,
    pub last_milestone: u64,
}

pub type BulkVideoStatsResponseV2 = Vec<VideoStatsV2>;

pub fn rewards_router(state: Arc<AppState>) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(get_video_views))
        .routes(routes!(get_user_view_history))
        .routes(routes!(get_user_reward_history))
        .routes(routes!(get_creator_reward_history))
        .routes(routes!(get_reward_config))
        .routes(routes!(bulk_get_video_stats))
        .routes(routes!(bulk_get_video_stats_v2))
        .with_state(state)
}

#[utoipa::path(
    get,
    path = "/video/{video_id}/views",
    params(
        ("video_id" = String, Path, description = "Video ID"),
        PaginationParams,
    ),
    tag = "rewards",
    responses(
        (status = 200, description = "View history retrieved", body = ViewHistoryResponse),
        (status = 500, description = "Internal server error"),
    )
)]
async fn get_video_views(
    State(state): State<Arc<AppState>>,
    Path(video_id): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ViewHistoryResponse>, (StatusCode, String)> {
    let history_tracker = HistoryTracker::new(state.leaderboard_redis_pool.clone());

    let views = history_tracker
        .get_video_views(&video_id, params.limit)
        .await
        .map_err(|e| {
            log::error!("Failed to get video views: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    let total = views.len();

    Ok(Json(ViewHistoryResponse { views, total }))
}

#[utoipa::path(
    get,
    path = "/user/{user_id}/views",
    params(
        ("user_id" = String, Path, description = "User Principal ID"),
        PaginationParams,
    ),
    tag = "rewards",
    responses(
        (status = 200, description = "User view history retrieved", body = ViewHistoryResponse),
        (status = 500, description = "Internal server error"),
    )
)]
async fn get_user_view_history(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<ViewHistoryResponse>, (StatusCode, String)> {
    let principal = Principal::from_text(&user_id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid principal: {}", e)))?;

    let history_tracker = HistoryTracker::new(state.leaderboard_redis_pool.clone());

    let views = history_tracker
        .get_user_view_history(&principal, params.limit)
        .await
        .map_err(|e| {
            log::error!("Failed to get user view history: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    let total = views.len();

    Ok(Json(ViewHistoryResponse { views, total }))
}

#[utoipa::path(
    get,
    path = "/user/{user_id}/rewards",
    params(
        ("user_id" = String, Path, description = "User Principal ID"),
        PaginationParams,
    ),
    tag = "rewards",
    responses(
        (status = 200, description = "User reward history retrieved", body = RewardHistoryResponse),
        (status = 500, description = "Internal server error"),
    )
)]
async fn get_user_reward_history(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<RewardHistoryResponse>, (StatusCode, String)> {
    let principal = Principal::from_text(&user_id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid principal: {}", e)))?;

    let history_tracker = HistoryTracker::new(state.leaderboard_redis_pool.clone());

    let rewards = history_tracker
        .get_user_reward_history(&principal, params.limit)
        .await
        .map_err(|e| {
            log::error!("Failed to get user reward history: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    let total = rewards.len();

    Ok(Json(RewardHistoryResponse { rewards, total }))
}

#[utoipa::path(
    get,
    path = "/creator/{creator_id}/rewards",
    params(
        ("creator_id" = String, Path, description = "Creator Principal ID"),
        PaginationParams,
    ),
    tag = "rewards",
    responses(
        (status = 200, description = "Creator reward history retrieved", body = RewardHistoryResponse),
        (status = 500, description = "Internal server error"),
    )
)]
async fn get_creator_reward_history(
    State(state): State<Arc<AppState>>,
    Path(creator_id): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<RewardHistoryResponse>, (StatusCode, String)> {
    let principal = Principal::from_text(&creator_id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid principal: {}", e)))?;

    let history_tracker = HistoryTracker::new(state.leaderboard_redis_pool.clone());

    let rewards = history_tracker
        .get_creator_reward_history(&principal, params.limit)
        .await
        .map_err(|e| {
            log::error!("Failed to get creator reward history: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    let total = rewards.len();

    Ok(Json(RewardHistoryResponse { rewards, total }))
}

#[utoipa::path(
    get,
    path = "/config",
    tag = "rewards",
    responses(
        (status = 200, description = "Configuration retrieved", body = ConfigResponse),
        (status = 500, description = "Internal server error"),
    )
)]
async fn get_reward_config(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ConfigResponse>, (StatusCode, String)> {
    // Get config from RewardEngine (fetches from Redis)
    let config = state.rewards_module.reward_engine.get_config().await;

    // Return None if reward_amount_inr is 0 (rewards disabled)
    let response = if config.reward_amount_inr == 0.0 {
        ConfigResponse { config: None }
    } else {
        ConfigResponse {
            config: Some(config),
        }
    };

    Ok(Json(response))
}

pub async fn update_reward_config(
    State(state): State<Arc<AppState>>,
    Json(new_config): Json<RewardConfig>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    log::info!("Updating reward configuration: {:?}", new_config);

    // Update the configuration through the rewards module
    if let Err(e) = state
        .rewards_module
        .reward_engine
        .update_config(new_config)
        .await
    {
        log::error!("Failed to update reward configuration: {}", e);
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to update configuration: {}", e),
        ));
    }

    Ok(StatusCode::OK)
}

#[utoipa::path(
    post,
    path = "/videos/bulk-stats",
    request_body = BulkVideoStatsRequest,
    tag = "rewards",
    responses(
        (status = 200, description = "Bulk video stats retrieved", body = BulkVideoStatsResponse),
        (status = 500, description = "Internal server error"),
    )
)]
async fn bulk_get_video_stats(
    State(state): State<Arc<AppState>>,
    Json(request): Json<BulkVideoStatsRequest>,
) -> Result<Json<BulkVideoStatsResponse>, (StatusCode, String)> {
    let reward_engine = &state.rewards_module.reward_engine;

    // Use Redis pipelining for efficient batched retrieval
    let response = reward_engine
        .get_bulk_video_stats(&request.video_ids)
        .await
        .map_err(|e| {
            log::error!("Failed to get bulk video stats: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/videos/bulk-stats-v2",
    request_body = BulkVideoStatsRequest,
    tag = "rewards",
    responses(
        (status = 200, description = "Bulk video stats retrieved (v2 - ordered list)", body = BulkVideoStatsResponseV2),
        (status = 500, description = "Internal server error"),
    )
)]
async fn bulk_get_video_stats_v2(
    State(state): State<Arc<AppState>>,
    Json(request): Json<BulkVideoStatsRequest>,
) -> Result<Json<BulkVideoStatsResponseV2>, (StatusCode, String)> {
    let reward_engine = &state.rewards_module.reward_engine;

    // Use Redis pipelining for efficient batched retrieval
    let stats_map = reward_engine
        .get_bulk_video_stats(&request.video_ids)
        .await
        .map_err(|e| {
            log::error!("Failed to get bulk video stats: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    // Special handling for single video: fetch total_count_all from canister
    let canister_total_view_count = if request.video_ids.len() == 1 {
        let video_id = &request.video_ids[0];

        #[cfg(not(feature = "local-bin"))]
        {
            // Query BigQuery to get post_id from video_id
            let post_id_opt = query_post_id_from_bigquery(&state.bigquery_client, video_id).await;

            if let Some(post_id) = post_id_opt {
                let user_post_service =
                    UserPostService(*USER_POST_SERVICE_CANISTER_ID, &state.agent);

                match user_post_service
                    .get_individual_post_details_by_id(post_id.clone())
                    .await
                {
                    Ok(Result1::Ok(post)) => {
                        log::debug!(
                            "Retrieved total_view_count from canister for video {} (post_id {}): {}",
                            video_id,
                            post_id,
                            post.view_stats.total_view_count
                        );
                        Some(post.view_stats.total_view_count)
                    }
                    Ok(Result1::Err(e)) => {
                        log::warn!(
                            "Canister returned error for video {} (post_id {}): {:?}, falling back to Redis",
                            video_id,
                            post_id,
                            e
                        );
                        None
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to get post details from canister for video {} (post_id {}): {}, falling back to Redis",
                            video_id,
                            post_id,
                            e
                        );
                        None
                    }
                }
            } else {
                log::warn!(
                    "Could not find post_id for video {}, falling back to Redis",
                    video_id
                );
                None
            }
        }

        #[cfg(feature = "local-bin")]
        {
            log::debug!(
                "Skipping canister call for video {} (local-bin mode), using Redis value",
                video_id
            );
            None
        }
    } else {
        None
    };

    // Convert HashMap to ordered Vec, preserving request order
    let response: Vec<VideoStatsV2> = request
        .video_ids
        .iter()
        .enumerate()
        .map(|(idx, video_id)| {
            let stats = stats_map.get(video_id).cloned().unwrap_or(VideoStats {
                count: 0,
                total_count_loggedin: 0,
                total_count_all: 0,
                last_milestone: 0,
            });

            // Use canister value for total_count_all if available (single video case)
            let total_count_all = if idx == 0 {
                canister_total_view_count.unwrap_or(stats.total_count_all)
            } else {
                stats.total_count_all
            };

            VideoStatsV2 {
                video_id: video_id.clone(),
                count: stats.count,
                total_count_loggedin: stats.total_count_loggedin,
                total_count_all,
                last_milestone: stats.last_milestone,
            }
        })
        .collect();

    Ok(Json(response))
}
