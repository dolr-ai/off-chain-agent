use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use std::sync::Arc;

use super::rate_limit::verify_rate_limit;
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
    let _user_principal = verify_rate_limit(request.principal, &app_state)
        .await
        .map_err(|(status, error)| (status, Json(error)))?;
    // Check if FalAi is requested (not implemented)
    match &request.input {
        videogen_common::VideoGenInput::FalAi { .. } => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(videogen_common::VideoGenError::InvalidInput(
                    "FalAi provider not implemented yet".to_string(),
                )),
            ));
        }
        _ => {}
    }

    let result = super::veo3::generate(request.input, &app_state).await;

    match result {
        Ok(response) => Ok(Json(response)),
        Err(e) => {
            let status_code = match &e {
                videogen_common::VideoGenError::AuthError => StatusCode::UNAUTHORIZED,
                videogen_common::VideoGenError::InvalidInput(_) => StatusCode::BAD_REQUEST,
                videogen_common::VideoGenError::ProviderError(_) => StatusCode::BAD_GATEWAY,
                videogen_common::VideoGenError::NetworkError(_) => StatusCode::SERVICE_UNAVAILABLE,
            };
            Err((status_code, Json(e)))
        }
    }
}
