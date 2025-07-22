use axum::{extract::State, http::StatusCode, Json};
use std::sync::Arc;

use super::rate_limit::verify_rate_limit;
use crate::app_state::AppState;

/// Generate a video using the specified provider
#[utoipa::path(
    post,
    path = "/generate",
    request_body = videogen_common::VideoGenRequest,
    responses(
        (status = 200, description = "Video generation started successfully", body = videogen_common::VideoGenResponse),
        (status = 400, description = "Invalid input", body = videogen_common::VideoGenError),
        (status = 401, description = "Authentication failed", body = videogen_common::VideoGenError),
        (status = 429, description = "Rate limit exceeded", body = videogen_common::VideoGenError),
        (status = 502, description = "Provider error", body = videogen_common::VideoGenError),
        (status = 503, description = "Service unavailable", body = videogen_common::VideoGenError),
    ),
    tag = "VideoGen"
)]
pub async fn generate_video(
    State(app_state): State<Arc<AppState>>,
    Json(request): Json<videogen_common::VideoGenRequest>,
) -> Result<
    Json<videogen_common::VideoGenResponse>,
    (StatusCode, Json<videogen_common::VideoGenError>),
> {
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
