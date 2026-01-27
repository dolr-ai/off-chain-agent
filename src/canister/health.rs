use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, Json};
use http::StatusCode;
use serde::Serialize;
use utoipa::ToSchema;

use crate::{
    app_state::AppState,
    consts::{
        RATE_LIMITS_CANISTER_ID, USER_INFO_SERVICE_CANISTER_ID, USER_POST_SERVICE_CANISTER_ID,
    },
};
use yral_canisters_client::{
    rate_limits::RateLimits, user_info_service::UserInfoService, user_post_service::UserPostService,
};

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CanisterStatus {
    pub canister_id: String,
    pub name: String,
    pub healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CanisterHealthResponse {
    pub healthy: bool,
    pub canisters: Vec<CanisterStatus>,
}

async fn check_rate_limits(app_state: &AppState) -> CanisterStatus {
    let canister_id = *RATE_LIMITS_CANISTER_ID;
    let client = RateLimits(canister_id, &app_state.agent);

    match client.get_version().await {
        Ok(version) => CanisterStatus {
            canister_id: canister_id.to_text(),
            name: "rate_limits".to_string(),
            healthy: true,
            version: Some(version),
            error: None,
        },
        Err(e) => CanisterStatus {
            canister_id: canister_id.to_text(),
            name: "rate_limits".to_string(),
            healthy: false,
            version: None,
            error: Some(e.to_string()),
        },
    }
}

async fn check_user_info_service(app_state: &AppState) -> CanisterStatus {
    let canister_id = *USER_INFO_SERVICE_CANISTER_ID;
    let client = UserInfoService(canister_id, &app_state.agent);

    match client.get_version().await {
        Ok(version) => CanisterStatus {
            canister_id: canister_id.to_text(),
            name: "user_info_service".to_string(),
            healthy: true,
            version: Some(version),
            error: None,
        },
        Err(e) => CanisterStatus {
            canister_id: canister_id.to_text(),
            name: "user_info_service".to_string(),
            healthy: false,
            version: None,
            error: Some(e.to_string()),
        },
    }
}

async fn check_user_post_service(app_state: &AppState) -> CanisterStatus {
    let canister_id = *USER_POST_SERVICE_CANISTER_ID;
    let client = UserPostService(canister_id, &app_state.agent);

    match client.get_version().await {
        Ok(version) => CanisterStatus {
            canister_id: canister_id.to_text(),
            name: "user_post_service".to_string(),
            healthy: true,
            version: Some(version),
            error: None,
        },
        Err(e) => CanisterStatus {
            canister_id: canister_id.to_text(),
            name: "user_post_service".to_string(),
            healthy: false,
            version: None,
            error: Some(e.to_string()),
        },
    }
}

/// Check health of all canisters
#[utoipa::path(
    get,
    path = "/canister-health",
    tag = "health",
    responses(
        (status = 200, description = "All canisters healthy", body = CanisterHealthResponse),
        (status = 503, description = "One or more canisters unhealthy", body = CanisterHealthResponse),
    )
)]
pub async fn canister_health_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let state_ref = &state;

    let (rate_limits, user_info, user_post) = tokio::join!(
        check_rate_limits(state_ref),
        check_user_info_service(state_ref),
        check_user_post_service(state_ref),
    );

    let results = vec![rate_limits, user_info, user_post];
    let all_healthy = results.iter().all(|s| s.healthy);

    let response = CanisterHealthResponse {
        healthy: all_healthy,
        canisters: results,
    };

    let status = if all_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status, Json(response))
}
