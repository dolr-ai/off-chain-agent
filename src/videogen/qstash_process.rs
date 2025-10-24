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
        VideoGenInput::LumaLabs(_) => {
            crate::videogen::models::lumalabs::generate(request.input, &state).await
        }
        VideoGenInput::IntTest(_) => {
            crate::videogen::models::inttest::generate(request.input, &state).await
        }
        VideoGenInput::Wan25(_) => {
            let input = request.input.clone();
            crate::videogen::models::wan2_5::generate_with_context(input, &state, Some(&request)).await
        }
        VideoGenInput::Wan25Fast(_) => {
            let input = request.input.clone();
            crate::videogen::models::wan2_5_fast::generate_with_context(input, &state, Some(&request)).await
        }
        _ => Err(VideoGenError::UnsupportedModel(
            request.input.model_id().to_string(),
        )),
    };

    // Check if this is a webhook-enabled model that returned with empty video URL
    // In this case, we should not trigger the callback here as the webhook will handle it
    let is_webhook_pending = matches!(&result, 
        Ok(response) if response.video_url.is_empty() && 
        std::env::var("ENABLE_REPLICATE_WEBHOOKS").unwrap_or_default() == "true"
    );

    if is_webhook_pending {
        // For webhook-enabled requests with empty video URL, return a "pending" response
        // The webhook will handle the actual callback when video generation completes
        log::info!(
            "Webhook-enabled request for {} - waiting for webhook completion, not triggering callback",
            request.request_key.principal
        );
        return Ok(Json(serde_json::json!({
            "status": "pending",
            "message": "Video generation in progress, webhook will handle completion"
        })));
    }

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
