use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info;
use videogen_common::{VideoGenError, VideoGenInput, VideoGenResponse};

use crate::app_state::AppState;
use crate::consts::RUNPOD_WAN2_2_ENDPOINT;

#[derive(Serialize)]
struct Wan22Request {
    input: Wan22Input,
}

#[derive(Serialize)]
struct Wan22Input {
    prompt: String,

    // Hardcoded parameters from Python script and API spec
    #[serde(rename = "size")]
    size: String, // "720*1280"

    duration: u32,            // 5
    num_inference_steps: u32, // 80
    guidance: u32,            // 5
    seed: i32,                // -1

    #[serde(skip_serializing_if = "Option::is_none")]
    negative_prompt: Option<String>, // ""

    temperature: f32, // 0.7
    flow_shift: u32,  // 5
    max_tokens: u32,  // 256

    enable_prompt_optimization: bool, // true
    enable_safety_checker: bool,      // true
}

#[derive(Deserialize)]
struct Wan22SubmitResponse {
    id: String,
}

#[derive(Deserialize)]
struct Wan22StatusResponse {
    status: String,
    output: Option<Wan22Output>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct Wan22Output {
    video_url: Option<String>,
    // Alternative field names from API spec
    url: Option<String>,
    result: Option<String>,
    video: Option<String>,

    // Metadata
    _width: Option<u32>,
    _height: Option<u32>,
    _duration: Option<f32>,
    _seed: Option<u64>,
    generation_time: Option<f32>,
    cost: Option<f32>,
}

pub async fn generate(
    input: VideoGenInput,
    _app_state: &AppState,
) -> Result<VideoGenResponse, VideoGenError> {
    let VideoGenInput::Wan22(model) = input else {
        return Err(VideoGenError::InvalidInput(
            "Only Wan22 input is supported".to_string(),
        ));
    };

    let api_key = std::env::var("RUNPOD_API_KEY").map_err(|_| VideoGenError::AuthError)?;

    let client = reqwest::Client::new();

    // Build request with hardcoded parameters
    let request = Wan22Request {
        input: Wan22Input {
            prompt: model.prompt.clone(),
            size: "720*1280".to_string(),
            duration: 5,
            num_inference_steps: 80,
            guidance: 5,
            seed: -1,
            negative_prompt: Some("".to_string()),
            temperature: 0.7,
            flow_shift: 5,
            max_tokens: 256,
            enable_prompt_optimization: true,
            enable_safety_checker: true,
        },
    };

    // Submit job
    let submit_url = format!("https://api.runpod.ai/v2/{RUNPOD_WAN2_2_ENDPOINT}/run");

    info!(
        "Submitting Wan 2.2 generation job for prompt: {}",
        &model.prompt[..model.prompt.len().min(60)]
    );

    let response = client
        .post(&submit_url)
        .header("Authorization", format!("Bearer {api_key}"))
        .header("Content-Type", "application/json")
        .json(&request)
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to submit job: {e}")))?;

    if !response.status().is_success() {
        let error_text = response
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());
        return Err(VideoGenError::ProviderError(format!(
            "RunPod API error: {error_text}"
        )));
    }

    let submit_response: Wan22SubmitResponse = response.json().await.map_err(|e| {
        VideoGenError::ProviderError(format!("Failed to parse submit response: {e}"))
    })?;

    info!("Wan 2.2 job submitted with ID: {}", submit_response.id);

    // Poll for completion
    let video_url = poll_for_completion(&submit_response.id, &api_key).await?;

    Ok(VideoGenResponse {
        operation_id: submit_response.id,
        video_url,
        provider: "wan2_2".to_string(),
    })
}

async fn poll_for_completion(job_id: &str, api_key: &str) -> Result<String, VideoGenError> {
    let client = reqwest::Client::new();
    let status_url = format!("https://api.runpod.ai/v2/{RUNPOD_WAN2_2_ENDPOINT}/status/{job_id}");

    info!("Starting to poll for completion of Wan 2.2 job: {job_id}");

    let max_attempts = 120; // 20 minutes with 10s intervals
    let poll_interval = Duration::from_secs(10);

    for attempt in 0..max_attempts {
        let response = client
            .get(&status_url)
            .header("Authorization", format!("Bearer {api_key}"))
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| VideoGenError::NetworkError(format!("Failed to check job status: {e}")))?;

        if !response.status().is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(VideoGenError::ProviderError(format!(
                "Failed to check job status: {error_text}"
            )));
        }

        let status: Wan22StatusResponse = response.json().await.map_err(|e| {
            VideoGenError::ProviderError(format!("Failed to parse status response: {e}"))
        })?;

        match status.status.as_str() {
            "COMPLETED" => {
                if let Some(output) = status.output {
                    // Try different possible video URL fields
                    let video_url = output
                        .video_url
                        .or(output.url)
                        .or(output.result)
                        .or(output.video);

                    if let Some(url) = video_url {
                        info!("Wan 2.2 video generation completed");

                        // Log metadata if available
                        if let Some(gen_time) = output.generation_time {
                            info!("Generation time: {gen_time}s");
                        }
                        if let Some(cost) = output.cost {
                            info!("Cost: ${cost}");
                        }

                        return Ok(url);
                    } else {
                        return Err(VideoGenError::ProviderError(
                            "Generation completed but no video URL found".to_string(),
                        ));
                    }
                } else {
                    return Err(VideoGenError::ProviderError(
                        "Generation completed but output not available".to_string(),
                    ));
                }
            }
            "FAILED" | "CANCELLED" | "TIMED_OUT" => {
                let error = status
                    .error
                    .unwrap_or_else(|| format!("Job {}", status.status.to_lowercase()));
                return Err(VideoGenError::ProviderError(format!(
                    "Video generation failed: {error}"
                )));
            }
            "IN_QUEUE" | "IN_PROGRESS" => {
                if attempt > 0 && attempt % 6 == 0 {
                    info!(
                        "Wan 2.2 generation still in progress... ({} seconds elapsed)",
                        attempt * 10
                    );
                }
            }
            _ => {
                // Unknown status, continue polling
                info!("Unknown status: {}", status.status);
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
