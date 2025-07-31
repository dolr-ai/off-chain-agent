use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use std::sync::Arc;
use videogen_common::VideoGenerator;

use super::token_operations::{deduct_token_balance, add_token_balance, get_model_cost};
use super::qstash_types::QstashVideoGenRequest;
use super::rate_limit::verify_rate_limit_and_create_request;
use super::signature::verify_videogen_request;
use crate::app_state::AppState;
use crate::auth::verify_jwt_from_header;
use crate::consts::OFF_CHAIN_AGENT_URL;

/// Generate a video using the specified provider
#[utoipa::path(
    post,
    path = "/generate",
    request_body = videogen_common::VideoGenRequest,
    responses(
        (status = 200, description = "Video generation started successfully", body = videogen_common::VideoGenQueuedResponse),
        (status = 400, description = "Invalid input", body = videogen_common::VideoGenError),
        (status = 401, description = "Authentication failed - Bearer token required", body = videogen_common::VideoGenError),
        (status = 429, description = "Rate limit exceeded", body = videogen_common::VideoGenError),
        (status = 502, description = "Provider error", body = videogen_common::VideoGenError),
        (status = 503, description = "Service unavailable", body = videogen_common::VideoGenError),
    ),
    security(
        ("Bearer" = [])
    ),
    tag = "VideoGen"
)]
pub async fn generate_video(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<videogen_common::VideoGenRequest>,
) -> Result<
    Json<videogen_common::VideoGenQueuedResponse>,
    (StatusCode, Json<videogen_common::VideoGenError>),
> {
    // Verify JWT token
    let jwt_public_key = std::env::var("JWT_PUBLIC_KEY_PEM").map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(videogen_common::VideoGenError::AuthError),
        )
    })?;

    let jwt_aud = std::env::var("JWT_AUD").map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(videogen_common::VideoGenError::AuthError),
        )
    })?;

    verify_jwt_from_header(&jwt_public_key, jwt_aud, &headers).map_err(|(_, status)| {
        (
            StatusCode::from_u16(status).unwrap_or(StatusCode::UNAUTHORIZED),
            Json(videogen_common::VideoGenError::AuthError),
        )
    })?;

    log::info!(
        "agent princ {:?}",
        app_state.agent.get_principal().unwrap().to_text()
    );

    // Verify rate limit and create request atomically
    let model_name = request.input.model_name();
    let prompt = request.input.get_prompt();
    let property = if model_name == "INTTEST" {
        "VIDEOGEN_INTTEST"
    } else {
        "VIDEOGEN"
    };

    let request_key = verify_rate_limit_and_create_request(
        request.principal,
        model_name,
        prompt,
        property,
        &app_state,
    )
    .await
    .map_err(|(status, error)| (status, Json(error)))?;

    // Get provider before moving input
    let provider = request.input.provider();

    // Queue to Qstash
    let qstash_request = QstashVideoGenRequest {
        user_principal: request.principal,
        input: request.input,
        request_key: request_key.clone(),
        property: property.to_string(),
        deducted_amount: None, // No balance deduction for JWT-based auth
        token_type: request.token_type.clone(),
    };

    let callback_url = OFF_CHAIN_AGENT_URL
        .join("qstash/video_gen_callback")
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(videogen_common::VideoGenError::NetworkError(format!(
                    "Failed to construct callback URL: {}",
                    e
                ))),
            )
        })?
        .to_string();

    app_state
        .qstash_client
        .queue_video_generation(&qstash_request, &callback_url)
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(videogen_common::VideoGenError::NetworkError(format!(
                    "Failed to queue video generation: {}",
                    e
                ))),
            )
        })?;

    // Return polling response
    Ok(Json(videogen_common::VideoGenQueuedResponse {
        operation_id: format!("{}_{}", request_key.principal, request_key.counter),
        provider: provider.to_string(),
        request_key: videogen_common::VideoGenRequestKey {
            principal: request_key.principal,
            counter: request_key.counter,
        },
    }))
}

/// Generate a video using a signed request with signature verification and balance deduction
#[utoipa::path(
    post,
    path = "/generate_signed",
    request_body = videogen_common::VideoGenRequestWithSignature,
    responses(
        (status = 200, description = "Video generation started successfully", body = videogen_common::VideoGenQueuedResponse),
        (status = 400, description = "Invalid input", body = videogen_common::VideoGenError),
        (status = 401, description = "Authentication failed - Invalid signature", body = videogen_common::VideoGenError),
        (status = 402, description = "Insufficient balance", body = videogen_common::VideoGenError),
        (status = 429, description = "Rate limit exceeded", body = videogen_common::VideoGenError),
        (status = 502, description = "Provider error", body = videogen_common::VideoGenError),
        (status = 503, description = "Service unavailable", body = videogen_common::VideoGenError),
    ),
    tag = "VideoGen"
)]
pub async fn generate_video_signed(
    State(app_state): State<Arc<AppState>>,
    Json(signed_request): Json<videogen_common::VideoGenRequestWithSignature>,
) -> Result<
    Json<videogen_common::VideoGenQueuedResponse>,
    (StatusCode, Json<videogen_common::VideoGenError>),
> {
    let user_principal = signed_request.request.principal;

    // Verify signature
    verify_videogen_request(user_principal, &signed_request)
        .map_err(|e| (StatusCode::UNAUTHORIZED, Json(e)))?;

    log::info!("Signature verified for user {}", user_principal);

    // Verify rate limit and create request atomically
    let model_name = signed_request.request.input.model_name();
    let prompt = signed_request.request.input.get_prompt();
    let property = if model_name == "INTTEST" {
        "VIDEOGEN_INTTEST"
    } else {
        "VIDEOGEN"
    };

    let request_key = verify_rate_limit_and_create_request(
        user_principal,
        model_name,
        prompt,
        property,
        &app_state,
    )
    .await
    .map_err(|(status, error)| (status, Json(error)))?;

    // Get provider before moving input
    let provider = signed_request.request.input.provider();

    // Get JWT token from environment variable
    let jwt_token = std::env::var("YRAL_HON_WORKER_JWT").map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(videogen_common::VideoGenError::AuthError),
        )
    })?;

    // Get the cost based on model and token type
    let model_name = signed_request.request.input.model_name();
    let token_type = &signed_request.request.token_type;
    let cost = get_model_cost(&model_name, token_type);
    
    // Prepare auth based on token type
    let jwt_opt = match token_type {
        videogen_common::TokenType::Sats => Some(jwt_token.clone()),
        videogen_common::TokenType::Dolr => None, // DOLR would need agent from app_state
    };
    
    // Get agent for DOLR operations
    let agent = if matches!(token_type, videogen_common::TokenType::Dolr) {
        Some(app_state.agent.clone())
    } else {
        None
    };

    // Deduct balance after successful rate limit check
    let deducted_amount = deduct_token_balance(
        user_principal, 
        cost,
        token_type,
        jwt_opt,
        agent,
    )
    .await
    .map_err(|e| match e {
        videogen_common::VideoGenError::InsufficientBalance => {
            (StatusCode::PAYMENT_REQUIRED, Json(e))
        }
        _ => (StatusCode::SERVICE_UNAVAILABLE, Json(e)),
    })?;

    log::info!(
        "Successfully deducted {} {:?} from user {}",
        deducted_amount,
        token_type,
        user_principal
    );

    // Queue to Qstash
    let qstash_request = QstashVideoGenRequest {
        user_principal: signed_request.request.principal,
        input: signed_request.request.input,
        request_key: request_key.clone(),
        property: property.to_string(),
        deducted_amount: Some(deducted_amount),
        token_type: signed_request.request.token_type.clone(),
    };

    let callback_url = OFF_CHAIN_AGENT_URL
        .join("qstash/video_gen_callback")
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(videogen_common::VideoGenError::NetworkError(format!(
                    "Failed to construct callback URL: {}",
                    e
                ))),
            )
        })?
        .to_string();

    match app_state
        .qstash_client
        .queue_video_generation(&qstash_request, &callback_url)
        .await
    {
        Ok(()) => {
            // Successfully queued
        }
        Err(e) => {
            // Failed to queue - rollback balance
            log::error!(
                "Failed to queue video generation: {}. Rolling back balance.",
                e
            );

            if let Some(deducted_amount) = qstash_request.deducted_amount {
                let jwt_opt = match &qstash_request.token_type {
                    videogen_common::TokenType::Sats => Some(jwt_token.clone()),
                    videogen_common::TokenType::Dolr => None,
                };
                
                let agent = if matches!(&qstash_request.token_type, videogen_common::TokenType::Dolr) {
                    Some(app_state.agent.clone())
                } else {
                    None
                };
                
                if let Err(rollback_err) = add_token_balance(
                    user_principal,
                    deducted_amount,
                    &qstash_request.token_type,
                    jwt_opt,
                    agent,
                ).await {
                    log::error!(
                        "Failed to rollback {} {:?} for user {}: {}",
                        deducted_amount,
                        qstash_request.token_type,
                        user_principal,
                        rollback_err
                    );
                }
            }

            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(videogen_common::VideoGenError::NetworkError(format!(
                    "Failed to queue video generation: {}",
                    e
                ))),
            ));
        }
    }

    // Return polling response
    Ok(Json(videogen_common::VideoGenQueuedResponse {
        operation_id: format!("{}_{}", request_key.principal, request_key.counter),
        provider: provider.to_string(),
        request_key: videogen_common::VideoGenRequestKey {
            principal: request_key.principal,
            counter: request_key.counter,
        },
    }))
}
