use axum::{http::StatusCode, Json};
use candid::Principal;
use ic_agent::{
    identity::{DelegatedIdentity, Identity as IdentityTrait},
    Agent,
};
use std::sync::Arc;
use videogen_common::{
    types_v2::VideoUploadHandling, TokenType, VideoGenError, VideoGenProvider,
    VideoGenQueuedResponse, VideoGenRequest, VideoGenRequestWithIdentity, VideoGenerator,
};

use super::qstash_types::QstashVideoGenRequest;
use super::token_operations::add_token_balance;
use crate::app_state::AppState;
use crate::consts::OFF_CHAIN_AGENT_URL;

/// Metadata extracted from a video generation request
pub struct RequestMetadata {
    pub model_id: String,
    pub prompt: String,
    pub token_type: TokenType,
    pub property: String,
    pub provider: VideoGenProvider,
    pub cost: u64,
}

/// Validates the delegated identity and returns the user principal
pub fn validate_delegated_identity(
    identity_request: &VideoGenRequestWithIdentity,
) -> Result<Principal, (StatusCode, Json<VideoGenError>)> {
    // Parse delegated identity
    let identity: DelegatedIdentity = identity_request
        .delegated_identity
        .clone()
        .try_into()
        .map_err(|e: k256::elliptic_curve::Error| {
            log::error!("Failed to create delegated identity: {e}");
            (StatusCode::UNAUTHORIZED, Json(VideoGenError::AuthError))
        })?;

    // Extract user principal
    let user_principal = identity
        .sender()
        .map_err(|_| (StatusCode::UNAUTHORIZED, Json(VideoGenError::AuthError)))?;

    // Verify the principal matches the request
    if user_principal != identity_request.request.principal {
        return Err((StatusCode::UNAUTHORIZED, Json(VideoGenError::AuthError)));
    }

    log::info!("Identity verified for user {user_principal}");
    Ok(user_principal)
}

/// Extracts metadata from the video generation request
pub fn extract_request_metadata(request: &VideoGenRequest) -> RequestMetadata {
    let model_id = request.input.model_id();
    let prompt = request.input.get_prompt();
    let token_type = request.token_type;
    let property = if model_id == "inttest" {
        "VIDEOGEN_INTTEST"
    } else {
        "VIDEOGEN"
    };
    let provider = request.input.provider();
    let cost = super::token_operations::get_model_cost(model_id, &token_type);

    RequestMetadata {
        model_id: model_id.to_string(),
        prompt: prompt.to_string(),
        token_type,
        property: property.to_string(),
        provider,
        cost,
    }
}

/// Retrieves the JWT token from environment variables
pub fn get_hon_worker_jwt_token() -> Result<String, (StatusCode, Json<VideoGenError>)> {
    std::env::var("YRAL_HON_WORKER_JWT").map_err(|_| {
        log::error!("YRAL_HON_WORKER_JWT environment variable not set");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(VideoGenError::AuthError),
        )
    })
}

/// Rolls back balance on failure
pub async fn rollback_balance_on_failure(
    user_principal: Principal,
    deducted_amount: Option<u64>,
    token_type: &TokenType,
    jwt_token: String,
    admin_agent: &Agent,
) -> Result<(), VideoGenError> {
    // Only rollback if there was a deduction
    if let Some(amount) = deducted_amount {
        let jwt_opt = match token_type {
            TokenType::Sats => Some(jwt_token),
            TokenType::Dolr => None,
            TokenType::Free => None,
            TokenType::YralProSubscription => None,
        };

        let admin_agent_opt =
            if matches!(token_type, TokenType::Dolr | TokenType::YralProSubscription) {
                Some(admin_agent.clone())
            } else {
                None
            };

        add_token_balance(user_principal, amount, token_type, jwt_opt, admin_agent_opt)
            .await
            .map_err(|e| {
                log::error!(
                    "Failed to rollback {amount} {token_type:?} for user {user_principal}: {e}"
                );
                e
            })?;
    }
    Ok(())
}

/// Builds the final queued response
pub fn build_queued_response(
    request_key: crate::videogen::VideoGenRequestKey,
    provider: VideoGenProvider,
) -> VideoGenQueuedResponse {
    VideoGenQueuedResponse {
        operation_id: format!("{}_{}", request_key.principal, request_key.counter),
        provider: provider.to_string(),
        request_key: videogen_common::VideoGenRequestKey {
            principal: request_key.principal,
            counter: request_key.counter,
        },
    }
}

/// Common video generation processing logic used by both v1 and v2 APIs
pub async fn process_video_generation(
    app_state: &Arc<AppState>,
    user_principal: Principal,
    video_gen_input: videogen_common::VideoGenInput,
    token_type: TokenType,
    handle_video_upload: Option<VideoUploadHandling>,
) -> Result<crate::videogen::VideoGenRequestKey, (StatusCode, Json<VideoGenError>)> {
    let model_id = video_gen_input.model_id();
    let prompt = video_gen_input.get_prompt();

    let property = if model_id == "inttest" {
        "VIDEOGEN_INTTEST"
    } else {
        "VIDEOGEN"
    };

    let request_key = super::rate_limit::verify_rate_limit_and_create_request_v1(
        user_principal,
        model_id,
        prompt,
        property,
        &token_type,
        None,
        app_state,
    )
    .await
    .map_err(|(status, error)| (status, Json(error)))?;

    let uses_webhook = video_gen_input.supports_webhook_callbacks();
    let callback_url = if !uses_webhook {
        Some(
            OFF_CHAIN_AGENT_URL
                .join("qstash/video_gen_callback")
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(VideoGenError::NetworkError(format!(
                            "Failed to construct callback URL: {e}"
                        ))),
                    )
                })?
                .to_string(),
        )
    } else {
        None
    };

    let qstash_request = super::qstash_types::QstashVideoGenRequest {
        user_principal,
        input: video_gen_input,
        request_key: request_key.clone(),
        property: property.to_string(),
        deducted_amount: None,
        token_type,
        handle_video_upload,
        encrypted_identity: None,
    };

    app_state
        .qstash_client
        .queue_video_generation(&qstash_request, callback_url.as_deref())
        .await
        .map_err(|e| {
            log::error!("Failed to queue video generation: {e}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(VideoGenError::NetworkError(format!(
                    "Failed to queue video generation: {e}"
                ))),
            )
        })?;

    Ok(request_key)
}
