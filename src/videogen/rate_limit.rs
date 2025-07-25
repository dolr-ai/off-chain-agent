use axum::http::StatusCode;
use candid::Principal;
use std::sync::Arc;
use yral_canisters_client::rate_limits::{RateLimitResult, RateLimits};
use yral_canisters_client::user_info_service::{SessionType, UserInfoService};
use yral_canisters_client::individual_user_template::IndividualUserTemplate;

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
/// * `model` - The video generation model name (e.g., "VEO3", "LUMALABS")
/// * `app_state` - The application state containing the IC agent
///
/// # Returns
/// * `Ok(Principal)` - The user principal if rate limit check passes
/// * `Err((StatusCode, VideoGenError))` - Error with appropriate status code
pub async fn verify_rate_limit(
    user_principal: Principal,
    model: &str,
    app_state: &Arc<AppState>,
) -> Result<Principal, (StatusCode, VideoGenError)> {
    // Get user session type to determine if registered
    let user_info_service = UserInfoService(*USER_INFO_SERVICE_CANISTER_ID, &app_state.agent);

    let is_registered = match user_info_service
        .get_user_session_type(user_principal)
        .await
    {
        Ok(result) => match result {
            yral_canisters_client::user_info_service::Result2::Ok(session_type) => {
                matches!(session_type, SessionType::RegisteredSession)
            }
            yral_canisters_client::user_info_service::Result2::Err(e) => {
                // Check if the error is "User not found"
                if e.contains("User not found") {
                    log::info!(
                        "User {} not found in user info service, checking individual canister",
                        user_principal
                    );
                    
                    // Try to get the user's individual canister
                    match app_state.get_individual_canister_by_user_principal(user_principal).await {
                        Ok(canister_id) => {
                            // Create individual user template and get session type
                            let individual_user = IndividualUserTemplate(canister_id, &app_state.agent);
                            match individual_user.get_session_type().await {
                                Ok(session_result) => match session_result {
                                    yral_canisters_client::individual_user_template::Result7::Ok(session_type) => {
                                        matches!(session_type, yral_canisters_client::individual_user_template::SessionType::RegisteredSession)
                                    }
                                    yral_canisters_client::individual_user_template::Result7::Err(e) => {
                                        log::warn!(
                                            "Failed to get session type from individual canister for {}: {}",
                                            user_principal,
                                            e
                                        );
                                        false
                                    }
                                },
                                Err(e) => {
                                    log::warn!(
                                        "Error calling get_session_type on individual canister for {}: {}",
                                        user_principal,
                                        e
                                    );
                                    false
                                }
                            }
                        }
                        Err(e) => {
                            log::warn!(
                                "Failed to get individual canister for principal {}: {}",
                                user_principal,
                                e
                            );
                            false
                        }
                    }
                } else {
                    log::warn!(
                        "Failed to get session type for principal {}: {}",
                        user_principal,
                        e
                    );
                    false
                }
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

    log::info!(
        "User {} is {}registered for model {}",
        user_principal,
        if is_registered { "" } else { "not " },
        model
    );

    // Create rate limits client
    let rate_limits_client = RateLimits(*RATE_LIMITS_CANISTER_ID, &app_state.agent);

    // Use VIDEOGEN_RATE_LIMIT_PROPERTY for all models except IntTest
    let property = if model == "INTTEST" {
        "VIDEOGEN_INTTEST".to_string()
    } else {
        VIDEOGEN_RATE_LIMIT_PROPERTY.to_string()
    };

    let res = rate_limits_client
        .check_rate_limit(
            user_principal,
            property.clone(),
            false,
        )
        .await;
    log::info!("Rate limit check for user {} with property {}: {:?}", user_principal, property, res);

    // Check rate limit
    let rate_limit_result = rate_limits_client
        .increment_request_count(
            user_principal,
            property,
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
