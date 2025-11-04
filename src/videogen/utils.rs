use axum::{http::StatusCode, Json};
use candid::Principal;
use ic_agent::{
    identity::{DelegatedIdentity, Identity as IdentityTrait},
    Agent,
};
use std::sync::Arc;
use videogen_common::{
    TokenType, VideoGenError, VideoGenProvider, VideoGenQueuedResponse, VideoGenRequest,
    VideoGenRequestWithIdentity, VideoGenerator,
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

/// Creates a user agent for DOLR operations if needed
pub fn create_user_agent_if_dolr(
    identity: DelegatedIdentity,
    token_type: &TokenType,
) -> Result<Option<Agent>, (StatusCode, Json<VideoGenError>)> {
    if matches!(token_type, TokenType::Dolr) {
        let agent = Agent::builder()
            .with_identity(identity)
            .with_url("https://ic0.app")
            .build()
            .map_err(|e| {
                log::error!("Failed to build agent: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(VideoGenError::NetworkError(
                        "Failed to create agent".to_string(),
                    )),
                )
            })?;
        Ok(Some(agent))
    } else {
        Ok(None)
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
        };

        let admin_agent_opt = if matches!(token_type, TokenType::Dolr) {
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

/// Queues video generation to Qstash with automatic rollback on failure
pub async fn queue_to_qstash_with_rollback(
    app_state: &Arc<AppState>,
    qstash_request: QstashVideoGenRequest,
    jwt_token: String,
    user_principal: Principal,
) -> Result<(), (StatusCode, Json<VideoGenError>)> {
    let uses_webhook = qstash_request.input.supports_webhook_callbacks();

    // Build callback URL
    let callback_url = OFF_CHAIN_AGENT_URL
        .join("qstash/video_gen_callback")
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(VideoGenError::NetworkError(format!(
                    "Failed to construct callback URL: {e}"
                ))),
            )
        })?
        .to_string();

    // Attempt to queue
    if let Err(e) = app_state
        .qstash_client
        .queue_video_generation(
            &qstash_request,
            if uses_webhook {
                None
            } else {
                Some(callback_url)
            },
        )
        .await
    {
        log::error!("Failed to queue video generation: {e}. Rolling back balance.");

        // Rollback balance on failure
        if let Err(rollback_err) = rollback_balance_on_failure(
            user_principal,
            qstash_request.deducted_amount,
            &qstash_request.token_type,
            jwt_token,
            &app_state.agent,
        )
        .await
        {
            log::error!("Rollback failed: {rollback_err}");
        }

        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(VideoGenError::NetworkError(format!(
                "Failed to queue video generation: {e}"
            ))),
        ));
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
/// This function handles everything after input validation and preparation
pub async fn process_video_generation(
    app_state: &Arc<AppState>,
    user_principal: Principal,
    video_gen_input: videogen_common::VideoGenInput,
    token_type: TokenType,
    delegated_identity_wire: yral_types::delegated_identity::DelegatedIdentityWire,
) -> Result<crate::videogen::VideoGenRequestKey, (StatusCode, Json<VideoGenError>)> {
    // Extract metadata from the input
    let model_id = video_gen_input.model_id();
    let prompt = video_gen_input.get_prompt();

    // Determine property for rate limiting
    let property = if model_id == "inttest" {
        "VIDEOGEN_INTTEST"
    } else {
        "VIDEOGEN"
    };

    // Get cost for the model
    let cost = super::token_operations::get_model_cost(model_id, &token_type);

    // Determine payment amount for rate limit
    let payment_amount = if matches!(token_type, TokenType::Free) {
        None
    } else {
        Some(cost)
    };

    // Verify rate limit
    let request_key = super::rate_limit::verify_rate_limit_and_create_request_v1(
        user_principal,
        model_id,
        prompt,
        property,
        &token_type,
        payment_amount,
        app_state,
    )
    .await
    .map_err(|(status, error)| (status, Json(error)))?;

    // Create delegated identity for agent creation
    let identity: DelegatedIdentity =
        delegated_identity_wire
            .try_into()
            .map_err(|e: k256::elliptic_curve::Error| {
                log::error!("Failed to create delegated identity: {e}");
                (StatusCode::UNAUTHORIZED, Json(VideoGenError::AuthError))
            })?;

    // Create user agent if needed for DOLR operations
    let user_agent = create_user_agent_if_dolr(identity, &token_type)?;

    // Get JWT token
    let jwt_token = get_hon_worker_jwt_token()?;

    // Deduct balance with automatic cleanup on failure
    let deducted_amount = super::token_operations::deduct_balance_with_cleanup(
        user_principal,
        cost,
        &token_type,
        jwt_token.clone(),
        &app_state.agent,
        &request_key,
        property,
        user_agent.as_ref(),
    )
    .await?;

    // Prepare Qstash request
    let qstash_request = super::qstash_types::QstashVideoGenRequest {
        user_principal,
        input: video_gen_input,
        request_key: request_key.clone(),
        property: property.to_string(),
        deducted_amount,
        token_type,
    };

    // Queue to Qstash with automatic rollback on failure
    queue_to_qstash_with_rollback(app_state, qstash_request, jwt_token, user_principal).await?;

    Ok(request_key)
}
