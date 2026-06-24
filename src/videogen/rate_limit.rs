use axum::http::StatusCode;
use candid::Principal;
use std::sync::Arc;
use yral_canisters_client::rate_limits::{
    RateLimits, TokenType as CanisterTokenType, VideoGenRequest, VideoGenRequestKey,
};
use yral_canisters_client::user_info_service::{SessionType, UserInfoService};

use crate::{
    app_state::AppState,
    consts::{RATE_LIMITS_CANISTER_ID, USER_INFO_SERVICE_CANISTER_ID},
};
use videogen_common::{TokenType, VideoGenError};

/// Checks if user is registered via user info service
async fn check_user_registration(user_principal: Principal, app_state: &Arc<AppState>) -> bool {
    let user_info_service = UserInfoService(*USER_INFO_SERVICE_CANISTER_ID, &app_state.agent);

    let result = match user_info_service
        .get_user_session_type(user_principal)
        .await
    {
        Ok(result) => result,
        Err(e) => {
            log::error!("Failed to get session type for principal {user_principal}: {e}");
            return false;
        }
    };

    match result {
        yral_canisters_client::user_info_service::Result8::Ok(session_type) => {
            matches!(session_type, SessionType::RegisteredSession)
        }
        yral_canisters_client::user_info_service::Result8::Err(e) => {
            if e.contains("User not found") {
                // TODO: migrate to user_post_service/user_info_service
                // Previously fell back to IndividualUserTemplate.get_session_type().
                // Individual user canisters have been decommissioned — if the user
                // is not found in user_info_service, they are not registered.
                log::info!(
                    "User {user_principal} not found in user info service — treating as not registered"
                );
                false
            } else {
                log::warn!("Failed to get session type for principal {user_principal}: {e}");
                false
            }
        }
    }
}

pub async fn fetch_request(
    user_principal: Principal,
    counter: u64,
    app_state: &Arc<AppState>,
) -> Result<VideoGenRequest, (StatusCode, VideoGenError)> {
    let rate_limits_client = RateLimits(*RATE_LIMITS_CANISTER_ID, &app_state.agent);

    let videogen_request_key = VideoGenRequestKey {
        principal: user_principal,
        counter,
    };

    let request_result = rate_limits_client
        .get_video_generation_request(videogen_request_key)
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                VideoGenError::NetworkError(format!(
                    "Failed to fetch video generation request: {e}"
                )),
            )
        })?;

    match request_result {
        Some(request) => {
            log::info!(
                "Fetched video generation request for principal {} counter {}",
                user_principal,
                counter
            );
            Ok(request)
        }
        None => Err((
            StatusCode::NOT_FOUND,
            VideoGenError::InvalidInput(format!(
                "Request not found for principal {} counter {}",
                user_principal, counter
            )),
        )),
    }
}

/// Verifies rate limit and creates video generation request if allowed (v1 with payment support)
///
/// # Arguments
/// * `user_principal` - The user's Principal
/// * `model` - The video generation model name (e.g., "VEO3", "LUMALABS")
/// * `prompt` - The generation prompt
/// * `property` - The rate limit property (e.g., "VIDEOGEN")
/// * `token_type` - The token type (Free, Sats, or Dolr)
/// * `payment_amount` - Optional payment amount for paid requests
/// * `app_state` - The application state containing the IC agent
///
/// # Returns
/// * `Ok(VideoGenRequestKey)` - The request key if rate limit check passes and request is created
/// * `Err((StatusCode, VideoGenError))` - Error with appropriate status code
pub async fn verify_rate_limit_and_create_request_v1(
    user_principal: Principal,
    model: &str,
    prompt: &str,
    property: &str,
    token_type: &TokenType,
    payment_amount: Option<u64>,
    app_state: &Arc<AppState>,
) -> Result<crate::videogen::VideoGenRequestKey, (StatusCode, VideoGenError)> {
    // Get user session type to determine if registered
    let is_registered = check_user_registration(user_principal, app_state).await;

    log::info!(
        "User {} is {}registered for model {} with token type {:?}",
        user_principal,
        if is_registered { "" } else { "not " },
        model,
        token_type
    );

    // Determine if this is a paid request based on token type
    let is_paid = !matches!(token_type, TokenType::Free);
    let payment_amount_str = if is_paid {
        payment_amount.map(|amt| amt.to_string())
    } else {
        None
    };

    // Create rate limits client
    let rate_limits_client = RateLimits(*RATE_LIMITS_CANISTER_ID, &app_state.agent);

    let canister_token_type = match token_type {
        TokenType::Free => CanisterTokenType::Free,
        TokenType::Sats => CanisterTokenType::Sats,
        TokenType::Dolr => CanisterTokenType::Dolr,
        TokenType::YralProSubscription => CanisterTokenType::YralProSubscription,
    };

    // Use the v2 method that handles both rate limit check and request creation
    let request_key_result = rate_limits_client
        .create_video_generation_request_v_2(
            user_principal,
            model.to_string(),
            prompt.to_string(),
            property.to_string(),
            canister_token_type,
            is_registered,
            is_paid,
            payment_amount_str,
        )
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                VideoGenError::NetworkError(format!(
                    "Failed to create video generation request: {e}"
                )),
            )
        })?;

    match request_key_result {
        yral_canisters_client::rate_limits::Result_::Ok(key) => {
            Ok(crate::videogen::VideoGenRequestKey {
                principal: key.principal,
                counter: key.counter,
            })
        }
        yral_canisters_client::rate_limits::Result_::Err(e) => {
            // Check if it's a rate limit error
            if e.contains("Rate limit exceeded") || e.contains("Property rate limit exceeded") {
                Err((
                    StatusCode::TOO_MANY_REQUESTS,
                    VideoGenError::ProviderError(e),
                ))
            } else {
                Err((
                    StatusCode::BAD_REQUEST,
                    VideoGenError::InvalidInput(format!("Failed to create request: {e}")),
                ))
            }
        }
    }
}
