use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use std::sync::Arc;
use videogen_common::VideoGenResponse;

use super::balance::{deduct_videogen_balance, rollback_videogen_balance};
use super::rate_limit::verify_rate_limit;
use super::signature::verify_videogen_request;
use crate::app_state::AppState;
use crate::auth::verify_jwt_from_header;

/// Generate a video using the specified provider
#[utoipa::path(
    post,
    path = "/generate",
    request_body = videogen_common::VideoGenRequest,
    responses(
        (status = 200, description = "Video generation started successfully", body = videogen_common::VideoGenResponse),
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
    Json<videogen_common::VideoGenResponse>,
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

    // Verify rate limit for the user
    let model_name = request.input.model_name();
    let _user_principal = verify_rate_limit(request.principal, model_name, &app_state)
        .await
        .map_err(|(status, error)| (status, Json(error)))?;
    // Route to appropriate provider
    let result = match &request.input {
        videogen_common::VideoGenInput::Veo3 { .. } => {
            super::models::veo3::generate(request.input, &app_state).await
        }
        videogen_common::VideoGenInput::Veo3Fast { .. } => {
            super::models::veo3_fast::generate(request.input, &app_state).await
        }
        videogen_common::VideoGenInput::FalAi { .. } => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(videogen_common::VideoGenError::InvalidInput(
                    "FalAi provider not implemented yet".to_string(),
                )),
            ));
        }
        videogen_common::VideoGenInput::LumaLabs { .. } => {
            super::models::lumalabs::generate(request.input, &app_state).await
        }
        videogen_common::VideoGenInput::IntTest { .. } => {
            super::models::inttest::generate(request.input, &app_state).await
        }
    };

    match result {
        Ok(response) => Ok(Json(response)),
        Err(e) => {
            let status_code = match &e {
                videogen_common::VideoGenError::AuthError => StatusCode::UNAUTHORIZED,
                videogen_common::VideoGenError::InvalidInput(_) => StatusCode::BAD_REQUEST,
                videogen_common::VideoGenError::ProviderError(_) => StatusCode::BAD_GATEWAY,
                videogen_common::VideoGenError::NetworkError(_) => StatusCode::SERVICE_UNAVAILABLE,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            Err((status_code, Json(e)))
        }
    }
}

/// Generate a video using a signed request with signature verification and balance deduction
#[utoipa::path(
    post,
    path = "/generate_signed",
    request_body = videogen_common::VideoGenRequestWithSignature,
    responses(
        (status = 200, description = "Video generation started successfully", body = videogen_common::VideoGenResponse),
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
    Json<videogen_common::VideoGenResponse>,
    (StatusCode, Json<videogen_common::VideoGenError>),
> {
    let user_principal = signed_request.request.principal;

    // Verify signature
    verify_videogen_request(user_principal, &signed_request)
        .map_err(|e| (StatusCode::UNAUTHORIZED, Json(e)))?;

    log::info!("Signature verified for user {}", user_principal);

    // // Verify rate limit for the user
    let model_name = signed_request.request.input.model_name();
    let _user_principal = verify_rate_limit(user_principal, model_name, &app_state)
        .await
        .map_err(|(status, error)| (status, Json(error)))?;

    // Route to appropriate provider
    let result = match &signed_request.request.input {
        videogen_common::VideoGenInput::Veo3 { .. } => {
            super::models::veo3::generate(signed_request.request.input, &app_state).await
        }
        videogen_common::VideoGenInput::Veo3Fast { .. } => {
            super::models::veo3_fast::generate(signed_request.request.input, &app_state).await
        }
        videogen_common::VideoGenInput::FalAi { .. } => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(videogen_common::VideoGenError::InvalidInput(
                    "FalAi provider not implemented yet".to_string(),
                )),
            ));
        }
        videogen_common::VideoGenInput::LumaLabs { .. } => {
            super::models::lumalabs::generate(signed_request.request.input, &app_state).await
        }
        videogen_common::VideoGenInput::IntTest { .. } => {
            super::models::inttest::generate(signed_request.request.input, &app_state).await
        }
    };

    log::info!("Video generation result: {:?}", result);

    match result {
        Ok(response) => Ok(Json(response)),
        Err(e) => {
            let status_code = match &e {
                videogen_common::VideoGenError::AuthError => StatusCode::UNAUTHORIZED,
                videogen_common::VideoGenError::InvalidInput(_) => StatusCode::BAD_REQUEST,
                videogen_common::VideoGenError::ProviderError(_) => StatusCode::BAD_GATEWAY,
                videogen_common::VideoGenError::NetworkError(_) => StatusCode::SERVICE_UNAVAILABLE,
                videogen_common::VideoGenError::InsufficientBalance => StatusCode::PAYMENT_REQUIRED,
                videogen_common::VideoGenError::InvalidSignature => StatusCode::UNAUTHORIZED,
            };
            Err((status_code, Json(e)))
        }
    }
}
