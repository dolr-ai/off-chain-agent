use axum::http::StatusCode;
use candid::Principal;
use std::sync::Arc;
use yral_canisters_client::individual_user_template::IndividualUserTemplate;
use yral_canisters_client::rate_limits::RateLimits;
use yral_canisters_client::user_info_service::{SessionType, UserInfoService};

use crate::{
    app_state::AppState,
    consts::{RATE_LIMITS_CANISTER_ID, USER_INFO_SERVICE_CANISTER_ID},
};
use videogen_common::VideoGenError;

/// Checks if user is registered via individual canister
async fn check_individual_canister_registration(
    user_principal: Principal,
    app_state: &Arc<AppState>,
) -> bool {
    let canister_id = match app_state
        .get_individual_canister_by_user_principal(user_principal)
        .await
    {
        Ok(id) => id,
        Err(e) => {
            log::warn!("Failed to get individual canister for principal {user_principal}: {e}");
            return false;
        }
    };

    let individual_user = IndividualUserTemplate(canister_id, &app_state.agent);

    let session_result = match individual_user.get_session_type().await {
        Ok(result) => result,
        Err(e) => {
            log::warn!(
                "Error calling get_session_type on individual canister for {user_principal}: {e}"
            );
            return false;
        }
    };

    match session_result {
        yral_canisters_client::individual_user_template::Result7::Ok(session_type) => {
            matches!(
                session_type,
                yral_canisters_client::individual_user_template::SessionType::RegisteredSession
            )
        }
        yral_canisters_client::individual_user_template::Result7::Err(e) => {
            log::warn!(
                "Failed to get session type from individual canister for {user_principal}: {e}"
            );
            false
        }
    }
}

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
        yral_canisters_client::user_info_service::Result2::Ok(session_type) => {
            matches!(session_type, SessionType::RegisteredSession)
        }
        yral_canisters_client::user_info_service::Result2::Err(e) => {
            if e.contains("User not found") {
                log::info!(
                    "User {user_principal} not found in user info service, checking individual canister"
                );
                check_individual_canister_registration(user_principal, app_state).await
            } else {
                log::warn!("Failed to get session type for principal {user_principal}: {e}");
                false
            }
        }
    }
}

/// Verifies rate limit and creates video generation request if allowed
///
/// # Arguments
/// * `user_principal` - The user's Principal
/// * `model` - The video generation model name (e.g., "VEO3", "LUMALABS")
/// * `prompt` - The generation prompt
/// * `property` - The rate limit property (e.g., "VIDEOGEN")
/// * `app_state` - The application state containing the IC agent
///
/// # Returns
/// * `Ok(VideoGenRequestKey)` - The request key if rate limit check passes and request is created
/// * `Err((StatusCode, VideoGenError))` - Error with appropriate status code
pub async fn verify_rate_limit_and_create_request(
    user_principal: Principal,
    model: &str,
    prompt: &str,
    property: &str,
    app_state: &Arc<AppState>,
) -> Result<crate::videogen::VideoGenRequestKey, (StatusCode, VideoGenError)> {
    // Get user session type to determine if registered
    let is_registered = check_user_registration(user_principal, app_state).await;

    log::info!(
        "User {} is {}registered for model {}",
        user_principal,
        if is_registered { "" } else { "not " },
        model
    );

    // Create rate limits client
    let rate_limits_client = RateLimits(*RATE_LIMITS_CANISTER_ID, &app_state.agent);

    // Create video generation request (includes rate limit check)
    let request_key_result = rate_limits_client
        .create_video_generation_request(
            user_principal,
            model.to_string(),
            prompt.to_string(),
            property.to_string(),
            is_registered,
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
            if e.contains("Rate limit exceeded") {
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
