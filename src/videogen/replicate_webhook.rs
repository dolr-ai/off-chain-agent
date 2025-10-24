use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use candid::Principal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::instrument;
use videogen_common::{TokenType, VideoGenError, VideoGenResponse};

use crate::{
    app_state::AppState,
    videogen::{
        qstash_types::{QstashVideoGenCallback, VideoGenCallbackResult},
        webhook_signature::{verify_webhook_signature, WebhookHeaders},
    },
};

/// Replicate webhook payload structure
#[derive(Debug, Deserialize, Serialize)]
pub struct ReplicateWebhookPayload {
    pub id: String,
    pub version: String,
    pub status: String,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub webhook_completed: Option<String>,
}

/// Query parameters for webhook URL
#[derive(Debug, Deserialize)]
pub struct WebhookQueryParams {
    pub principal: String,
    pub counter: u64,
    pub token_type: String,
}

/// Metadata we include in the Replicate prediction to track our internal state
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ReplicateWebhookMetadata {
    pub user_principal: String,
    pub request_key_principal: String,
    pub request_key_counter: u64,
    pub property: String,
    pub deducted_amount: Option<u64>,
    pub token_type: String, // TokenType serialized as string
}

/// Extract webhook signing secret from environment
fn get_webhook_signing_secret() -> Result<String, VideoGenError> {
    std::env::var("REPLICATE_WEBHOOK_SIGNING_SECRET").map_err(|_| {
        log::error!("REPLICATE_WEBHOOK_SIGNING_SECRET environment variable not set");
        VideoGenError::AuthError
    })
}

/// Convert Replicate status to our callback result
fn convert_replicate_status(payload: &ReplicateWebhookPayload) -> VideoGenCallbackResult {
    match payload.status.as_str() {
        "succeeded" => {
            if let Some(output) = &payload.output {
                // Output can be a string URL or an array with a URL
                let video_url = if let Some(url_str) = output.as_str() {
                    Some(url_str.to_string())
                } else if let Some(arr) = output.as_array() {
                    arr.first().and_then(|v| v.as_str()).map(|s| s.to_string())
                } else {
                    None
                };

                if let Some(url) = video_url {
                    VideoGenCallbackResult::Success(VideoGenResponse {
                        operation_id: payload.id.clone(),
                        video_url: url,
                        provider: "replicate".to_string(),
                    })
                } else {
                    VideoGenCallbackResult::Failure(
                        "Generation completed but no video URL found in output".to_string(),
                    )
                }
            } else {
                VideoGenCallbackResult::Failure(
                    "Generation completed but output not available".to_string(),
                )
            }
        }
        "failed" | "canceled" => {
            let error = payload
                .error
                .clone()
                .unwrap_or_else(|| format!("Prediction {}", payload.status));
            VideoGenCallbackResult::Failure(format!("Video generation failed: {error}"))
        }
        _ => VideoGenCallbackResult::Failure(format!("Unknown status: {}", payload.status)),
    }
}

/// Handle Replicate webhook notifications
#[instrument(skip(state, payload_bytes))]
pub async fn handle_replicate_webhook(
    State(state): State<Arc<AppState>>,
    Query(params): Query<WebhookQueryParams>,
    headers: axum::http::HeaderMap,
    payload_bytes: axum::body::Bytes,
) -> Result<StatusCode, (StatusCode, Json<VideoGenError>)> {
    log::info!("Received Replicate webhook notification");

    // Extract and verify webhook headers
    let webhook_headers = WebhookHeaders::from_http_headers(&headers).map_err(|e| {
        log::error!("Failed to extract webhook headers: {:?}", e);
        let (status, _message) = e.into();
        (status, Json(VideoGenError::AuthError))
    })?;

    // Get signing secret
    let signing_secret =
        get_webhook_signing_secret().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(e)))?;

    // Verify webhook signature
    verify_webhook_signature(&webhook_headers, &payload_bytes, &signing_secret).map_err(|e| {
        log::error!("Webhook signature verification failed: {:?}", e);
        let (status, _message) = e.into();
        (status, Json(VideoGenError::AuthError))
    })?;

    // Parse webhook payload
    let payload: ReplicateWebhookPayload = serde_json::from_slice(&payload_bytes).map_err(|e| {
        log::error!("Failed to parse webhook payload: {e}");
        (
            StatusCode::BAD_REQUEST,
            Json(VideoGenError::ProviderError(format!(
                "Failed to parse webhook payload: {e}"
            ))),
        )
    })?;

    log::info!(
        "Processing Replicate webhook for prediction {} with status {}",
        payload.id,
        payload.status
    );

    // Only process final states (completed, failed, canceled)
    match payload.status.as_str() {
        "succeeded" | "failed" | "canceled" => {
            // Extract metadata if available

            let Ok(user_principal) = Principal::from_text(params.principal.clone()) else {
                log::error!("Failed to parse principal");
                return Ok(StatusCode::OK);
            };

            let Ok(video_gen_request) =
                super::rate_limit::fetch_request(user_principal, params.counter, &state).await
            else {
                return Ok(StatusCode::OK);
            };

            // Parse token type

            // Convert to callback result
            let callback_result = convert_replicate_status(&payload);

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
                token_type: TokenType::Free, //TODO: verify once the videogen is updated.
            };

            // Use existing callback handler logic
            crate::videogen::qstash_callback::handle_video_gen_callback_internal(state, callback)
                .await
                .map_err(|(status, error)| (status, Json(VideoGenError::ProviderError(error))))?;

            log::info!(
                "Successfully processed Replicate webhook for prediction {}",
                payload.id
            );
        }
        _ => {
            // Intermediate states like "starting", "processing" - just acknowledge
            log::debug!(
                "Received intermediate status '{}' for prediction {}, acknowledging",
                payload.status,
                payload.id
            );
        }
    }

    Ok(StatusCode::OK)
}

/// Generate webhook URL for a Replicate prediction with principal and counter
pub fn generate_webhook_url(base_url: &str, principal: &str, counter: u64) -> String {
    format!(
        "{}/replicate/webhook?principal={}&counter={}",
        base_url.trim_end_matches('/'),
        principal,
        counter
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderName, HeaderValue};
    use serde_json::json;

    fn create_test_payload() -> ReplicateWebhookPayload {
        ReplicateWebhookPayload {
            id: "test-prediction-id".to_string(),
            version: "test-version".to_string(),
            status: "succeeded".to_string(),
            output: Some(json!("https://example.com/video.mp4")),
            error: None,
            webhook_completed: Some("2023-01-01T00:00:00Z".to_string()),
        }
    }

    #[test]
    fn test_generate_webhook_url() {
        let base_url = "https://example.com";
        let principal = "test-principal-123";
        let counter = 42;
        let webhook_url = generate_webhook_url(base_url, principal, counter);
        assert_eq!(
            webhook_url,
            "https://example.com/replicate/webhook?principal=test-principal-123&counter=42"
        );

        let base_url_with_slash = "https://example.com/";
        let webhook_url = generate_webhook_url(base_url_with_slash, principal, counter);
        assert_eq!(
            webhook_url,
            "https://example.com/replicate/webhook?principal=test-principal-123&counter=42"
        );
    }

    #[test]
    fn test_convert_replicate_status_success() {
        let payload = create_test_payload();
        let result = convert_replicate_status(&payload);

        match result {
            VideoGenCallbackResult::Success(response) => {
                assert_eq!(response.operation_id, "test-prediction-id");
                assert_eq!(response.video_url, "https://example.com/video.mp4");
                assert_eq!(response.provider, "replicate");
            }
            _ => panic!("Expected success result"),
        }
    }

    #[test]
    fn test_convert_replicate_status_failure() {
        let mut payload = create_test_payload();
        payload.status = "failed".to_string();
        payload.error = Some("Test error".to_string());
        payload.output = None;

        let result = convert_replicate_status(&payload);

        match result {
            VideoGenCallbackResult::Failure(error) => {
                assert!(error.contains("Test error"));
            }
            _ => panic!("Expected failure result"),
        }
    }

    #[test]
    fn test_convert_replicate_status_array_output() {
        let mut payload = create_test_payload();
        payload.output = Some(json!([
            "https://example.com/video.mp4",
            "https://example.com/video2.mp4"
        ]));

        let result = convert_replicate_status(&payload);

        match result {
            VideoGenCallbackResult::Success(response) => {
                assert_eq!(response.video_url, "https://example.com/video.mp4");
            }
            _ => panic!("Expected success result"),
        }
    }

    #[test]
    fn test_replicate_webhook_metadata_serialization() {
        let metadata = ReplicateWebhookMetadata {
            user_principal: "test-principal".to_string(),
            request_key_principal: "test-principal".to_string(),
            request_key_counter: 42,
            property: "VIDEOGEN".to_string(),
            deducted_amount: Some(100),
            token_type: "Sats".to_string(),
        };

        let json = serde_json::to_value(&metadata).unwrap();
        let deserialized: ReplicateWebhookMetadata = serde_json::from_value(json).unwrap();

        assert_eq!(metadata.user_principal, deserialized.user_principal);
        assert_eq!(
            metadata.request_key_counter,
            deserialized.request_key_counter
        );
        assert_eq!(metadata.deducted_amount, deserialized.deducted_amount);
    }
}
