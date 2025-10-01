use crate::{
    app_state::AppState,
    events::types::{EventPayload, RewardEarnedPayload},
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
use std::sync::Arc;
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

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
    pub config: RewardConfig,
}

pub fn rewards_router(state: Arc<AppState>) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(get_video_views))
        .routes(routes!(get_user_view_history))
        .routes(routes!(get_user_reward_history))
        .routes(routes!(get_creator_reward_history))
        .routes(routes!(get_reward_config))
        .routes(routes!(test_send_reward_notification))
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

    Ok(Json(ConfigResponse { config }))
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

#[derive(Debug, Deserialize, IntoParams)]
pub struct TestNotificationParams {
    #[serde(default = "default_creator_id")]
    pub creator_id: String,
    #[serde(default = "default_video_id")]
    pub video_id: String,
}

fn default_creator_id() -> String {
    "fopov-cd5tj-fnz6m-m6jdm-as6bl-2dj74-ujoco-jad55-icv5w-onpky-gqe".to_string()
}

fn default_video_id() -> String {
    "5b10e4ece3c94288a2db79e49bdafa9b".to_string()
}

#[utoipa::path(
    get,
    path = "/test/send-notification",
    params(
        TestNotificationParams,
    ),
    tag = "rewards",
    responses(
        (status = 200, description = "Notification sent successfully"),
        (status = 400, description = "Invalid creator_id"),
        (status = 500, description = "Internal server error"),
    )
)]
async fn test_send_reward_notification(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TestNotificationParams>,
) -> Result<Json<String>, (StatusCode, String)> {
    let creator_id = Principal::from_text(&params.creator_id)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid creator_id principal: {}", e)))?;

    let payload = RewardEarnedPayload {
        creator_id,
        video_id: params.video_id.clone(),
        milestone: 100,
        reward_btc: 0.0,
        reward_inr: 10.0,
        view_count: 100,
        timestamp: chrono::Utc::now().timestamp(),
        rewards_received_bs: true,
    };

    let event = EventPayload::RewardEarned(payload);
    event.send_notification(&state).await;

    Ok(Json(format!(
        "Reward notification sent successfully to creator {} for video {}",
        params.creator_id, params.video_id
    )))
}
