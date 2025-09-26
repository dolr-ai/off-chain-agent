use axum::{extract::State, http::StatusCode, Json};
use std::sync::Arc;
use tracing::instrument;
use videogen_common::{VideoGenError, VideoGenInput, VideoGenerator};

use crate::{
    app_state::AppState,
    videogen::qstash_types::{
        QstashVideoGenCallback, QstashVideoGenRequest, VideoGenCallbackResult,
    },
};

/// Process video generation request from Qstash queue
#[instrument(skip(state))]
pub async fn process_video_generation(
    State(state): State<Arc<AppState>>,
    Json(request): Json<QstashVideoGenRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<VideoGenError>)> {
    log::info!(
        "Processing video generation for user {} with model {}",
        request.user_principal,
        request.input.model_id()
    );

    // Route to appropriate model handler based on the input type
    let result = match request.input {
        VideoGenInput::Veo3(_) => {
            crate::videogen::models::veo3::generate(request.input, &state).await
        }
        VideoGenInput::Veo3Fast(_) => {
            crate::videogen::models::veo3_fast::generate(request.input, &state).await
        }
        VideoGenInput::LumaLabs(_) => {
            crate::videogen::models::lumalabs::generate(request.input, &state).await
        }
        VideoGenInput::IntTest(_) => {
            crate::videogen::models::inttest::generate(request.input, &state).await
        }
        VideoGenInput::TalkingHead(_) => {
            // TalkingHead model not yet implemented in qstash processing
            Err(VideoGenError::InvalidInput(
                "TalkingHead model not supported in qstash processing".to_string(),
            ))
        }
        VideoGenInput::Wan22(_) => {
            crate::videogen::models::wan2_2::generate(request.input, &state).await
        }
        VideoGenInput::LlmHandler(_) => {
            // LLM Handler returns a different response type, not directly usable here
            Err(VideoGenError::InvalidInput(
                "LLM Handler should not be processed through qstash".to_string(),
            ))
        }
        VideoGenInput::StableAudio(_) => {
            crate::videogen::models::stable_audio::generate(request.input, &state).await
        }
    };

    // Prepare callback data
    let callback_result = match result {
        Ok(response) => VideoGenCallbackResult::Success(response),
        Err(e) => VideoGenCallbackResult::Failure(e.to_string()),
    };

    let callback = QstashVideoGenCallback {
        request_key: request.request_key,
        result: callback_result,
        property: request.property,
        deducted_amount: request.deducted_amount,
        token_type: request.token_type,
    };

    // Return the callback data as the response
    // Qstash will automatically send this to the callback URL
    Ok(Json(serde_json::to_value(&callback).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(VideoGenError::ProviderError(format!(
                "Failed to serialize callback: {e}"
            ))),
        )
    })?))
}
