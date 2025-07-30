use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use std::sync::Arc;
use videogen_common::VideoGenerator;

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

    // Queue to Qstash
    let qstash_request = QstashVideoGenRequest {
        user_principal: signed_request.request.principal,
        input: signed_request.request.input,
        request_key: request_key.clone(),
        property: property.to_string(),
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
