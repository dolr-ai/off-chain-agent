use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use std::sync::Arc;
use tracing::instrument;
use videogen_common::{VideoGenError, VideoGenInput, VideoGenerator};

use crate::{
    app_state::AppState,
    videogen::{
        qstash_callback::handle_video_gen_callback_internal,
        qstash_types::{QstashVideoGenCallback, QstashVideoGenRequest, VideoGenCallbackResult},
        upload_ai_generated_video_to_canister_in_drafts::{
            upload_ai_generated_video_to_canister_impl, UploadAiVideoToCanisterRequest,
        },
    },
};

pub async fn process_prompt_generation(
    State(_state): State<Arc<AppState>>,
    Json(request): Json<QstashVideoGenRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<VideoGenError>)> {
    log::info!(
        "Processing prompt generation for user {} with model {}",
        request.user_principal,
        request.input.model_id()
    );

    Ok(Json(json!({})))
}

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
        VideoGenInput::LumaLabs(_) => {
            let input = request.input.clone();
            crate::videogen::models::lumalabs::generate(input, &state).await
        }
        VideoGenInput::IntTest(_) => {
            let input = request.input.clone();
            crate::videogen::models::inttest::generate_with_context(input, &state, &request).await
        }
        VideoGenInput::Wan25(_) => {
            let input = request.input.clone();
            crate::videogen::models::wan2_5::generate_with_context(input, &state, &request).await
        }
        VideoGenInput::Wan25Fast(_) => {
            let input = request.input.clone();
            crate::videogen::models::wan2_5_fast::generate_with_context(input, &state, &request)
                .await
        }
        VideoGenInput::SpeechToVideo(_) => {
            let input = request.input.clone();
            crate::videogen::models::speech_to_video::generate_with_context(input, &state, &request)
                .await
        }
        VideoGenInput::Ltx2(_) => {
            let input = request.input.clone();
            crate::videogen::models::ltx2::generate_with_context(input, &state, &request).await
        }
        _ => Err(VideoGenError::UnsupportedModel(
            request.input.model_id().to_string(),
        )),
    };

    let supports_webhook = request.input.supports_webhook_callbacks();

    // Prepare callback data for non-webhook or error cases
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
        handle_video_upload: request.handle_video_upload,
    };

    if supports_webhook {
        if let VideoGenCallbackResult::Failure(_e) = &callback.result {
            handle_video_gen_callback_internal(state, callback.clone())
                .await
                .map_err(|e| (e.0, Json(VideoGenError::NetworkError(e.1))))?;
        }
    }

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

pub async fn upload_ai_generated_video_to_canister_in_drafts(
    State(_state): State<Arc<AppState>>,
    Json(request): Json<UploadAiVideoToCanisterRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    match upload_ai_generated_video_to_canister_impl(&request.ai_video_url, request.user_id).await {
        Ok(()) => Ok(StatusCode::OK),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}
