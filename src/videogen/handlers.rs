use axum::{extract::State, http::StatusCode, Json};
use std::sync::Arc;
use videogen_common::{VideoGenError, VideoGenInput, VideoGenResponse};

use crate::app_state::AppState;

/// Generate a video using the specified provider
#[utoipa::path(
    post,
    path = "/generate",
    request_body = super::types::VideoGenInput,
    responses(
        (status = 200, description = "Video generation started successfully", body = super::types::VideoGenResponse),
        (status = 400, description = "Invalid input", body = super::types::VideoGenError),
        (status = 401, description = "Authentication failed", body = super::types::VideoGenError),
        (status = 502, description = "Provider error", body = super::types::VideoGenError),
        (status = 503, description = "Service unavailable", body = super::types::VideoGenError),
    ),
    tag = "VideoGen"
)]
pub async fn generate_video(
    State(app_state): State<Arc<AppState>>,
    Json(input): Json<VideoGenInput>,
) -> Result<Json<VideoGenResponse>, (StatusCode, Json<VideoGenError>)> {
    let result = match input {
        VideoGenInput::Veo3 { .. } => super::veo3::generate(input, &app_state).await,
        VideoGenInput::FalAi { .. } => Err(VideoGenError::InvalidInput(
            "FalAi provider not implemented yet".to_string(),
        )),
    };

    match result {
        Ok(response) => Ok(Json(response)),
        Err(e) => {
            let status_code = match &e {
                VideoGenError::AuthError => StatusCode::UNAUTHORIZED,
                VideoGenError::InvalidInput(_) => StatusCode::BAD_REQUEST,
                VideoGenError::ProviderError(_) => StatusCode::BAD_GATEWAY,
                VideoGenError::NetworkError(_) => StatusCode::SERVICE_UNAVAILABLE,
            };
            Err((status_code, Json(e)))
        }
    }
}
