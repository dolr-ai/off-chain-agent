#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{error, info};
use videogen_common::{VideoGenError, VideoGenInput, VideoGenResponse};

use crate::app_state::AppState;
use crate::consts::STABLE_AUDIO_URL;

#[derive(Serialize)]
struct StableAudioRequest {
    input: StableAudioInput,
}

#[derive(Serialize)]
struct StableAudioInput {
    prompt: String,
    duration: u32, // Duration in seconds
}

#[derive(Deserialize)]
struct StableAudioSubmitResponse {
    id: String,
    status: String,
    urls: Option<StableAudioUrls>,
    output: Option<String>, // Direct audio URL if available
}

#[derive(Deserialize)]
struct StableAudioUrls {
    get: String,
    cancel: Option<String>,
}

#[derive(Deserialize)]
struct StableAudioStatusResponse {
    id: String,
    status: String,
    output: Option<String>, // Audio URL when completed
    error: Option<String>,
    metrics: Option<StableAudioMetrics>,
}

#[derive(Deserialize)]
struct StableAudioMetrics {
    predict_time: Option<f64>,
}

pub async fn generate(
    input: VideoGenInput,
    _app_state: &AppState,
) -> Result<VideoGenResponse, VideoGenError> {
    let VideoGenInput::StableAudio(model) = input else {
        return Err(VideoGenError::InvalidInput(
            "Only StableAudio input is supported".to_string(),
        ));
    };

    // Get API token from environment
    let api_token = std::env::var("REPLICATE_API_TOKEN").map_err(|_| {
        error!("REPLICATE_API_TOKEN not set in environment");
        VideoGenError::AuthError
    })?;

    let client = reqwest::Client::new();

    let request = StableAudioRequest {
        input: StableAudioInput {
            prompt: model.prompt.clone(),
            duration: model.duration,
        },
    };

    info!(
        "Submitting audio generation for prompt: {} (duration: {}s)",
        &model.prompt[..model.prompt.len().min(60)],
        model.duration
    );

    // Submit job to Replicate
    let response = client
        .post(STABLE_AUDIO_URL)
        .header("Authorization", format!("Bearer {}", api_token))
        .header("Content-Type", "application/json")
        .json(&request)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| {
            error!("Failed to submit audio generation job: {}", e);
            VideoGenError::NetworkError(format!("Failed to connect to Stable Audio: {}", e))
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());

        error!("Stable Audio API error: {} - {}", status, error_text);

        if status == 401 {
            return Err(VideoGenError::AuthError);
        }

        return Err(VideoGenError::ProviderError(format!(
            "Stable Audio error: {} - {}",
            status, error_text
        )));
    }

    let submit_response: StableAudioSubmitResponse = response.json().await.map_err(|e| {
        error!("Failed to parse Stable Audio response: {}", e);
        VideoGenError::ProviderError(format!("Invalid response from Stable Audio: {}", e))
    })?;

    info!(
        "Audio generation job submitted with ID: {}",
        submit_response.id
    );

    // Check if output is immediately available (for cached results)
    if let Some(audio_url) = submit_response.output {
        info!("Audio generation completed immediately (cached)");
        return Ok(VideoGenResponse {
            operation_id: submit_response.id,
            video_url: audio_url, // Using video_url field for audio URL
            provider: "stable_audio".to_string(),
        });
    }

    // Extract status URL from the response
    let status_url = submit_response
        .urls
        .ok_or_else(|| {
            VideoGenError::ProviderError("No status URL provided in response".to_string())
        })?
        .get;

    // Poll for completion
    let audio_url = poll_for_completion(&status_url, &api_token).await?;

    Ok(VideoGenResponse {
        operation_id: submit_response.id,
        video_url: audio_url, // Using video_url field for audio URL
        provider: "stable_audio".to_string(),
    })
}

async fn poll_for_completion(status_url: &str, api_token: &str) -> Result<String, VideoGenError> {
    let client = reqwest::Client::new();

    info!("Starting to poll for audio generation completion");

    let max_attempts = 60; // 10 minutes with 10s intervals
    let poll_interval = Duration::from_secs(10);

    for attempt in 0..max_attempts {
        let response = client
            .get(status_url)
            .header("Authorization", format!("Bearer {}", api_token))
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| {
                error!("Failed to check audio generation status: {}", e);
                VideoGenError::NetworkError(format!("Failed to check status: {}", e))
            })?;

        if !response.status().is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(VideoGenError::ProviderError(format!(
                "Failed to check status: {}",
                error_text
            )));
        }

        let status_response: StableAudioStatusResponse = response.json().await.map_err(|e| {
            error!("Failed to parse status response: {}", e);
            VideoGenError::ProviderError(format!("Failed to parse status response: {}", e))
        })?;

        match status_response.status.as_str() {
            "succeeded" => {
                if let Some(audio_url) = status_response.output {
                    info!("Audio generation completed successfully");

                    // Log metrics if available
                    if let Some(metrics) = status_response.metrics {
                        if let Some(predict_time) = metrics.predict_time {
                            info!("Audio generation took {:.2}s", predict_time);
                        }
                    }

                    return Ok(audio_url);
                } else {
                    return Err(VideoGenError::ProviderError(
                        "Generation succeeded but no audio URL found".to_string(),
                    ));
                }
            }
            "failed" | "canceled" => {
                let error = status_response
                    .error
                    .unwrap_or_else(|| format!("Job {}", status_response.status));
                return Err(VideoGenError::ProviderError(format!(
                    "Audio generation failed: {}",
                    error
                )));
            }
            "starting" | "processing" => {
                if attempt > 0 && attempt % 3 == 0 {
                    info!(
                        "Audio generation still in progress... ({} seconds elapsed)",
                        attempt * 10
                    );
                }
            }
            _ => {
                // Unknown status, continue polling
                info!("Unknown status: {}", status_response.status);
            }
        }

        if attempt < max_attempts - 1 {
            tokio::time::sleep(poll_interval).await;
        }
    }

    Err(VideoGenError::ProviderError(
        "Audio generation timed out after 10 minutes".to_string(),
    ))
}
