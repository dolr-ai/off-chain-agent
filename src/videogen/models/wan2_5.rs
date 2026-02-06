use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info;
use videogen_common::types_v2::VideoUploadHandling;
use videogen_common::{ImageData, VideoGenError, VideoGenInput, VideoGenResponse};

use crate::app_state::AppState;
use crate::consts::{OFF_CHAIN_AGENT_URL, REPLICATE_API_URL, REPLICATE_WAN2_5_MODEL};
use crate::videogen::replicate_webhook::generate_webhook_url;

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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

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

pub async fn generate_with_context(
    input: VideoGenInput,
    app_state: &AppState,
    context: &crate::videogen::qstash_types::QstashVideoGenRequest,
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

    let video_upload_handling_str = match &context.handle_video_upload {
        Some(VideoUploadHandling::Client) => "Client",
        Some(VideoUploadHandling::ServerDraft) => "ServerDraft",
        None => "Client", // Default to Client if None
    };

    let webhook_url = generate_webhook_url(
        OFF_CHAIN_AGENT_URL.as_str(),
        &context.request_key.principal.to_string(),
        context.request_key.counter,
        video_upload_handling_str,
    );

    // Process image data - convert to URL or data URI
    let image_url = model.image.as_ref().map(|img_data| match img_data {
        ImageData::Url(url) => url.clone(),
        ImageData::Base64(image_input) => {
            format!("data:{};base64,{}", image_input.mime_type, image_input.data)
        }
    });

    let request = ReplicatePredictionRequest {
        version: REPLICATE_WAN2_5_MODEL.to_string(),
        input: Wan25Input {
            prompt: model.prompt.clone(),
            image: image_url,
            size: "720*1280".to_string(),
            duration: 5,
            seed: -1,
            negative_prompt: Some("".to_string()),
            enable_prompt_expansion: true,
        },
        webhook: Some(webhook_url),
        metadata: None,
    };

    // Submit prediction
    let submit_url = format!("{REPLICATE_API_URL}/predictions");

    let max_chars = 60;
    let prompt = &model.prompt;

    let truncated_prompt = prompt
        .char_indices()
        .nth(max_chars)
        .map(|(idx, _)| &prompt[..idx])
        .unwrap_or(prompt);

    info!(
        "Submitting Wan 2.5 generation prediction for prompt: {}",
        &truncated_prompt
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

    // With webhooks, return immediately - the webhook will handle completion
    info!(
        "Using webhook for Wan 2.5 prediction {}, returning immediately",
        prediction_response.id
    );
    Ok(VideoGenResponse {
        operation_id: prediction_response.id,
        video_url: String::new(), // Will be filled by webhook
        provider: "wan2_5".to_string(),
    })
}

#[allow(dead_code)]
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
