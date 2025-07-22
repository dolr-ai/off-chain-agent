use axum::http::StatusCode;
use candid::Principal;
use std::sync::Arc;
use yral_canisters_client::rate_limits::{RateLimitResult, RateLimits};
use yral_canisters_client::user_info_service::{SessionType, UserInfoService};

use crate::{
    app_state::AppState,
    consts::{
        RATE_LIMITS_CANISTER_ID, USER_INFO_SERVICE_CANISTER_ID, VIDEOGEN_RATE_LIMIT_PROPERTY,
    },
};
use videogen_common::VideoGenError;

/// Verifies rate limit for video generation requests
///
/// # Arguments
/// * `user_principal` - The user's Principal
/// * `app_state` - The application state containing the IC agent
///
/// # Returns
/// * `Ok(Principal)` - The user principal if rate limit check passes
/// * `Err((StatusCode, VideoGenError))` - Error with appropriate status code
pub async fn verify_rate_limit(
    user_principal: Principal,
    app_state: &Arc<AppState>,
) -> Result<Principal, (StatusCode, VideoGenError)> {
    // Get user session type to determine if registered
    let user_info_service = UserInfoService(*USER_INFO_SERVICE_CANISTER_ID, &app_state.agent);

    let is_registered = match user_info_service
        .get_session_type_principal(user_principal)
        .await
    {
        Ok(result) => match result {
            yral_canisters_client::user_info_service::Result2::Ok(session_type) => {
                matches!(session_type, SessionType::RegisteredSession)
            }
            yral_canisters_client::user_info_service::Result2::Err(_) => {
                // If we can't determine session type, treat as unregistered
                false
            }
        },
        Err(e) => {
            // If the service is unavailable, we could either fail or continue with unregistered
            // For now, let's log and continue as unregistered
            log::error!(
                "Failed to get session type for principal {}: {}",
                user_principal,
                e
            );
            false
        }
    };

    // Create rate limits client
    let rate_limits_client = RateLimits(*RATE_LIMITS_CANISTER_ID, &app_state.agent);

    // Check rate limit
    let rate_limit_result = rate_limits_client
        .increment_request_count(
            user_principal,
            VIDEOGEN_RATE_LIMIT_PROPERTY.to_string(),
            is_registered,
        )
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                VideoGenError::NetworkError(format!("Rate limit check failed: {}", e)),
            )
        })?;

    match rate_limit_result {
        RateLimitResult::Ok(_) => Ok(user_principal),
        RateLimitResult::Err(msg) => Err((
            StatusCode::TOO_MANY_REQUESTS,
            VideoGenError::ProviderError(format!("Rate limit exceeded: {}", msg)),
        )),
    }
}
