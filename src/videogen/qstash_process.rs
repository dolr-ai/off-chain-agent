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
        request.input.model_name()
    );

    // Route to appropriate model handler based on the input type
    let result = match request.input {
        VideoGenInput::Veo3(_) => {
            crate::videogen::models::veo3::generate(request.input, &state).await
        }
        VideoGenInput::Veo3Fast(_) => {
            crate::videogen::models::veo3_fast::generate(request.input, &state).await
        }
        VideoGenInput::FalAi(_) => Err(VideoGenError::InvalidInput(
            "FalAi provider not implemented yet".to_string(),
        )),
        VideoGenInput::LumaLabs(_) => {
            crate::videogen::models::lumalabs::generate(request.input, &state).await
        }
        VideoGenInput::IntTest(_) => {
            crate::videogen::models::inttest::generate(request.input, &state).await
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
    };

    // Create a response that includes both callback data and original_balance for rollback
    let mut response_value = serde_json::to_value(&callback).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(VideoGenError::ProviderError(format!(
                "Failed to serialize callback: {}",
                e
            ))),
        )
    })?;
    
    // Add deducted_amount to the response if present
    if let Some(deducted_amount) = request.deducted_amount {
        if let serde_json::Value::Object(ref mut map) = response_value {
            map.insert(
                "deducted_amount".to_string(),
                serde_json::Value::Number(serde_json::Number::from(deducted_amount)),
            );
        }
    }

    // Return the callback data as the response
    // Qstash will automatically send this to the callback URL
    Ok(Json(response_value))
}
