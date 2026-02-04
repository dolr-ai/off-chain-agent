use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use candid::Principal;
use serde::Deserialize;
use std::sync::Arc;
use tracing::instrument;
use videogen_common::{types_v2::VideoUploadHandling, TokenType, VideoGenError, VideoGenResponse};

use crate::{
    app_state::AppState,
    videogen::{
        comfyui_client::{extract_video_url_from_webhook, ComfyUIWebhookPayload},
        qstash_types::{QstashVideoGenCallback, VideoGenCallbackResult},
    },
};

use yral_canisters_client::rate_limits::TokenType as CanisterTokenType;

/// Query parameters for webhook URL
#[derive(Debug, Deserialize)]
pub struct ComfyUIWebhookQueryParams {
    pub principal: String,
    pub counter: u64,
    #[serde(default)]
    pub handle_video_upload: Option<VideoUploadHandling>,
}

/// Convert ComfyUI status to our callback result
fn convert_comfyui_status(
    payload: &ComfyUIWebhookPayload,
    api_url: &str,
) -> VideoGenCallbackResult {
    match payload.status.as_str() {
        "completed" => {
            if let Some(video_url) = extract_video_url_from_webhook(payload, api_url) {
                VideoGenCallbackResult::Success(VideoGenResponse {
                    operation_id: payload.id.clone(),
                    video_url,
                    provider: "comfyui_ltx2".to_string(),
                })
            } else {
                VideoGenCallbackResult::Failure(
                    "Generation completed but no video URL found in output".to_string(),
                )
            }
        }
        "failed" => {
            let error = payload
                .message
                .clone()
                .unwrap_or_else(|| "Unknown error".to_string());
            VideoGenCallbackResult::Failure(format!("Video generation failed: {error}"))
        }
        _ => VideoGenCallbackResult::Failure(format!("Unknown status: {}", payload.status)),
    }
}

/// Handle ComfyUI webhook notifications
#[instrument(skip(state, payload))]
pub async fn handle_comfyui_webhook(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ComfyUIWebhookQueryParams>,
    Json(payload): Json<ComfyUIWebhookPayload>,
) -> Result<StatusCode, (StatusCode, Json<VideoGenError>)> {
    log::info!(
        "Received ComfyUI webhook notification for request {}",
        payload.id
    );

    // Get the ComfyUI API URL from config for constructing video URLs
    let api_url = state
        .comfyui_client
        .as_ref()
        .map(|c| c.config.api_url.as_str())
        .unwrap_or("");

    log::info!(
        "Processing ComfyUI webhook for request {} with status {}",
        payload.id,
        payload.status
    );

    // Process the webhook
    let Ok(user_principal) = Principal::from_text(params.principal.clone()) else {
        log::error!("Failed to parse principal: {}", params.principal);
        return Ok(StatusCode::OK);
    };

    let Ok(video_gen_request) =
        super::rate_limit::fetch_request(user_principal, params.counter, &state).await
    else {
        log::error!(
            "Failed to fetch request for principal {} counter {}",
            params.principal,
            params.counter
        );
        return Ok(StatusCode::OK);
    };

    // Convert to callback result
    let callback_result = convert_comfyui_status(&payload, api_url);

    let token_type = match video_gen_request.token_type {
        Some(token_type_canister) => match token_type_canister {
            CanisterTokenType::Free => TokenType::Free,
            CanisterTokenType::Sats => TokenType::Sats,
            CanisterTokenType::Dolr => TokenType::Dolr,
            CanisterTokenType::YralProSubscription => TokenType::YralProSubscription,
        },
        None => TokenType::Free,
    };

    // Create callback data compatible with existing system
    let callback = QstashVideoGenCallback {
        request_key: crate::videogen::VideoGenRequestKey {
            principal: user_principal,
            counter: params.counter,
        },
        result: callback_result,
        property: video_gen_request.model_name.clone(),
        deducted_amount: video_gen_request
            .payment_amount
            .and_then(|a| a.parse::<u64>().ok()),
        token_type,
        handle_video_upload: params.handle_video_upload,
    };

    // Use existing callback handler logic
    crate::videogen::qstash_callback::handle_video_gen_callback_internal(state, callback)
        .await
        .map_err(|(status, error)| (status, Json(VideoGenError::ProviderError(error))))?;

    log::info!(
        "Successfully processed ComfyUI webhook for request {}",
        payload.id
    );

    Ok(StatusCode::OK)
}

/// Generate webhook URL for a ComfyUI request with principal and counter
pub fn generate_comfyui_webhook_url(
    base_url: &str,
    principal: &str,
    counter: u64,
    handle_video_upload: &str,
) -> String {
    format!(
        "{}/comfyui/webhook?principal={}&counter={}&handle_video_upload={}",
        base_url.trim_end_matches('/'),
        principal,
        counter,
        handle_video_upload
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_comfyui_webhook_url() {
        let base_url = "https://example.com";
        let principal = "test-principal-123";
        let counter = 42;
        let webhook_url = generate_comfyui_webhook_url(base_url, principal, counter, "Client");
        assert_eq!(
            webhook_url,
            "https://example.com/comfyui/webhook?principal=test-principal-123&counter=42&handle_video_upload=Client"
        );
    }
}
