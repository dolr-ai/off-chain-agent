use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info;
use videogen_common::{VideoGenError, VideoGenInput, VideoGenResponse};

use crate::app_state::AppState;
use crate::consts::{OFF_CHAIN_AGENT_URL, REPLICATE_API_URL, REPLICATE_WAN2_5_MODEL};
use crate::videogen::replicate_webhook::{generate_webhook_url, ReplicateWebhookMetadata};

#[derive(Serialize)]
pub struct ReplicatePredictionRequest {
    pub version: String,
    pub input: Wan25Input,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub webhook: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Serialize)]
pub struct Wan25Input {
    pub prompt: String,

    // Hardcoded parameters from Python script and API spec
    #[serde(rename = "size")]
    pub size: String, // "720*1280"

    pub duration: u32, // 5
    pub seed: i32,     // -1

    #[serde(skip_serializing_if = "Option::is_none")]
    pub negative_prompt: Option<String>, // ""

    pub enable_prompt_expansion: bool, // true (renamed from enable_prompt_optimization)
}

#[derive(Deserialize)]
pub struct ReplicatePredictionResponse {
    pub id: String,
    pub status: String,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
}

pub async fn generate(
    input: VideoGenInput,
    app_state: &AppState,
) -> Result<VideoGenResponse, VideoGenError> {
    generate_with_context(input, app_state, None).await
}

pub async fn generate_with_context(
    input: VideoGenInput,
    app_state: &AppState,
    context: Option<&crate::videogen::qstash_types::QstashVideoGenRequest>,
) -> Result<VideoGenResponse, VideoGenError> {
    let VideoGenInput::Wan25(model) = input else {
        return Err(VideoGenError::InvalidInput(
            "Only Wan25 input is supported".to_string(),
        ));
    };

    let api_key = &app_state.replicate_api_token;
    if api_key.is_empty() {
        return Err(VideoGenError::AuthError);
    }

    let client = reqwest::Client::new();

    // Check if we should use webhook (when context is provided)
    let use_webhook = context.is_some() && std::env::var("ENABLE_REPLICATE_WEBHOOKS").unwrap_or_default() == "true";
    
    let (webhook_url, metadata) = if use_webhook {
        if let Some(ctx) = context {
        let webhook_url = generate_webhook_url(&OFF_CHAIN_AGENT_URL.as_str());
        let metadata = ReplicateWebhookMetadata {
            user_principal: ctx.user_principal.to_string(),
            request_key_principal: ctx.request_key.principal.to_string(),
            request_key_counter: ctx.request_key.counter,
            property: ctx.property.clone(),
            deducted_amount: ctx.deducted_amount,
            token_type: serde_json::to_string(&ctx.token_type).unwrap_or_default().trim_matches('"').to_string(),
        };
            (Some(webhook_url), Some(serde_json::to_value(metadata).unwrap()))
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    // Build request with hardcoded parameters
    let request = ReplicatePredictionRequest {
        version: REPLICATE_WAN2_5_MODEL.to_string(),
        input: Wan25Input {
            prompt: model.prompt.clone(),
            size: "720*1280".to_string(),
            duration: 5,
            seed: -1,
            negative_prompt: Some("".to_string()),
            enable_prompt_expansion: true,
        },
        webhook: webhook_url,
        metadata,
    };

    // Submit prediction
    let submit_url = format!("{REPLICATE_API_URL}/predictions");

    info!(
        "Submitting Wan 2.5 generation prediction for prompt: {}",
        &model.prompt[..model.prompt.len().min(60)]
    );

    let response = client
        .post(submit_url)
        .bearer_auth(api_key)
        .header("Content-Type", "application/json")
        .json(&request)
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to submit prediction: {e}")))?;

    if !response.status().is_success() {
        let error_text = response
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());
        return Err(VideoGenError::ProviderError(format!(
            "Replicate API error: {error_text}"
        )));
    }

    let prediction_response: ReplicatePredictionResponse = response.json().await.map_err(|e| {
        VideoGenError::ProviderError(format!("Failed to parse prediction response: {e}"))
    })?;

    info!(
        "Wan 2.5 prediction submitted with ID: {}",
        prediction_response.id
    );

    if use_webhook {
        // With webhooks, return immediately - the webhook will handle completion
        info!("Using webhook for Wan 2.5 prediction {}, returning immediately", prediction_response.id);
        Ok(VideoGenResponse {
            operation_id: prediction_response.id,
            video_url: String::new(), // Will be filled by webhook
            provider: "wan2_5".to_string(),
        })
    } else {
        // Fallback to polling for backward compatibility
        info!("Using polling for Wan 2.5 prediction {}", prediction_response.id);
        let video_url = poll_for_completion(&prediction_response.id, api_key).await?;
        Ok(VideoGenResponse {
            operation_id: prediction_response.id,
            video_url,
            provider: "wan2_5".to_string(),
        })
    }
}

async fn poll_for_completion(prediction_id: &str, api_key: &str) -> Result<String, VideoGenError> {
    let client = reqwest::Client::new();
    let status_url = format!("{REPLICATE_API_URL}/predictions/{prediction_id}");

    info!("Starting to poll for completion of Wan 2.5 prediction: {prediction_id}");

    let max_attempts = 120; // 20 minutes with 10s intervals
    let poll_interval = Duration::from_secs(10);

    for attempt in 0..max_attempts {
        let response = client
            .get(&status_url)
            .bearer_auth(api_key)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| {
                VideoGenError::NetworkError(format!("Failed to check prediction status: {e}"))
            })?;

        if !response.status().is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(VideoGenError::ProviderError(format!(
                "Failed to check prediction status: {error_text}"
            )));
        }

        let prediction: ReplicatePredictionResponse = response.json().await.map_err(|e| {
            VideoGenError::ProviderError(format!("Failed to parse prediction response: {e}"))
        })?;

        match prediction.status.as_str() {
            "succeeded" => {
                if let Some(output) = prediction.output {
                    // Output can be a string URL or an array with a URL
                    let video_url = if let Some(url_str) = output.as_str() {
                        Some(url_str.to_string())
                    } else if let Some(arr) = output.as_array() {
                        arr.first().and_then(|v| v.as_str()).map(|s| s.to_string())
                    } else {
                        None
                    };

                    if let Some(url) = video_url {
                        info!("Wan 2.5 video generation completed");
                        return Ok(url);
                    } else {
                        return Err(VideoGenError::ProviderError(
                            "Generation completed but no video URL found in output".to_string(),
                        ));
                    }
                } else {
                    return Err(VideoGenError::ProviderError(
                        "Generation completed but output not available".to_string(),
                    ));
                }
            }
            "failed" | "canceled" => {
                let error = prediction
                    .error
                    .unwrap_or_else(|| format!("Prediction {}", prediction.status));
                return Err(VideoGenError::ProviderError(format!(
                    "Video generation failed: {error}"
                )));
            }
            "starting" | "processing" => {
                if attempt > 0 && attempt % 6 == 0 {
                    info!(
                        "Wan 2.5 generation still in progress... ({} seconds elapsed)",
                        attempt * 10
                    );
                }
            }
            _ => {
                // Unknown status, continue polling
                info!("Unknown status: {}", prediction.status);
            }
        }

        if attempt < max_attempts - 1 {
            tokio::time::sleep(poll_interval).await;
        }
    }

    Err(VideoGenError::ProviderError(
        "Video generation timed out after 20 minutes".to_string(),
    ))
}
