use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info;
use uuid::Uuid;
use videogen_common::{
    LumaLabsDuration, LumaLabsResolution, VideoGenError, VideoGenInput, VideoGenResponse,
};

use crate::app_state::AppState;
use crate::consts::{LUMALABS_API_URL, LUMALABS_IMAGE_BUCKET};

#[derive(Serialize)]
struct LumaLabsRequest {
    prompt: String,
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolution: Option<LumaLabsResolution>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration: Option<LumaLabsDuration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    aspect_ratio: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "loop")]
    loop_video: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    keyframes: Option<LumaLabsKeyframes>,
}

#[derive(Serialize)]
struct LumaLabsKeyframes {
    frame0: LumaLabsFrame,
}

#[derive(Serialize)]
struct LumaLabsFrame {
    #[serde(rename = "type")]
    frame_type: String,
    url: String,
}

#[derive(Deserialize)]
struct LumaLabsResponse {
    id: String,
    state: String,
    created_at: String,
}

#[derive(Deserialize)]
struct LumaLabsGenerationStatus {
    id: String,
    state: String,
    assets: Option<LumaLabsAssets>,
    #[serde(rename = "failure_reason")]
    failure_reason: Option<String>,
}

#[derive(Deserialize)]
struct LumaLabsAssets {
    video: Option<String>,
    image: Option<String>,
    progress_video: Option<String>,
}

pub async fn generate(
    input: VideoGenInput,
    app_state: &AppState,
) -> Result<VideoGenResponse, VideoGenError> {
    let model = match input {
        VideoGenInput::LumaLabs(model) => model,
        _ => {
            return Err(VideoGenError::InvalidInput(
                "Only LumaLabs input is supported".to_string(),
            ))
        }
    };
    
    let prompt = model.prompt;
    let image = model.image;
    let resolution = model.resolution;
    let duration = model.duration;
    let aspect_ratio = model.aspect_ratio;
    let loop_video = model.loop_video;

    // Get LumaLabs API key from environment
    let api_key = std::env::var("LUMALABS_API_KEY").map_err(|_| VideoGenError::AuthError)?;

    let client = reqwest::Client::new();

    // Upload image to GCS if provided
    let keyframes = if let Some(img) = image {
        #[cfg(not(feature = "local-bin"))]
        {
            let image_url =
                upload_image_to_gcs(&app_state.gcs_client, &img.data, &img.mime_type).await?;
            Some(LumaLabsKeyframes {
                frame0: LumaLabsFrame {
                    frame_type: "image".to_string(),
                    url: image_url,
                },
            })
        }
        #[cfg(feature = "local-bin")]
        {
            return Err(VideoGenError::InvalidInput(
                "Image upload not supported in local mode".to_string(),
            ));
        }
    } else {
        None
    };

    // Build request
    let request = LumaLabsRequest {
        prompt: prompt.clone(),
        model: "ray-flash-2".to_string(),
        resolution: Some(resolution),
        duration: Some(duration),
        aspect_ratio,
        loop_video: if loop_video { Some(true) } else { None },
        keyframes,
    };

    // Create generation
    let create_url = format!("{}/generations", LUMALABS_API_URL);
    let response = client
        .post(&create_url)
        .bearer_auth(&api_key)
        .header("accept", "application/json")
        .header("content-type", "application/json")
        .json(&request)
        .send()
        .await
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to create generation: {}", e)))?;

    if !response.status().is_success() {
        let error_text = response
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());
        return Err(VideoGenError::ProviderError(format!(
            "LumaLabs API error: {}",
            error_text
        )));
    }

    let generation_response: LumaLabsResponse = response.json().await.map_err(|e| {
        VideoGenError::ProviderError(format!("Failed to parse generation response: {}", e))
    })?;

    info!(
        "LumaLabs generation created with ID: {}",
        generation_response.id
    );

    // Poll for completion
    let video_url = poll_for_completion(&generation_response.id, &api_key).await?;

    Ok(VideoGenResponse {
        operation_id: generation_response.id,
        video_url,
        provider: "lumalabs".to_string(),
    })
}

async fn poll_for_completion(generation_id: &str, api_key: &str) -> Result<String, VideoGenError> {
    let client = reqwest::Client::new();
    let status_url = format!("{}/generations/{}", LUMALABS_API_URL, generation_id);

    log::info!(
        "Starting to poll for completion of generation: {}",
        generation_id
    );

    let max_attempts = 120; // 10 minutes max
    let poll_interval = Duration::from_secs(5);

    for attempt in 0..max_attempts {
        let response = client
            .get(&status_url)
            .bearer_auth(&api_key)
            .send()
            .await
            .map_err(|e| {
                VideoGenError::NetworkError(format!("Failed to check generation status: {}", e))
            })?;

        if !response.status().is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(VideoGenError::ProviderError(format!(
                "Failed to check generation status: {}",
                error_text
            )));
        }

        let response_text = response.text().await.map_err(|e| {
            VideoGenError::NetworkError(format!("Failed to read response text: {}", e))
        })?;

        log::info!("Attempting to parse status response: {}", response_text);

        let status: LumaLabsGenerationStatus =
            serde_json::from_str(&response_text).map_err(|e| {
                VideoGenError::ProviderError(format!(
                    "Failed to parse status response: {}. Response was: {}",
                    e, response_text
                ))
            })?;

        match status.state.as_str() {
            "completed" => {
                if let Some(assets) = status.assets {
                    if let Some(video_url) = assets.video {
                        info!("LumaLabs video generation completed");
                        return Ok(video_url);
                    } else {
                        return Err(VideoGenError::ProviderError(
                            "Generation completed but no video URL found".to_string(),
                        ));
                    }
                } else {
                    return Err(VideoGenError::ProviderError(
                        "Generation completed but assets not available".to_string(),
                    ));
                }
            }
            "failed" => {
                let reason = status
                    .failure_reason
                    .unwrap_or_else(|| "Unknown error".to_string());
                return Err(VideoGenError::ProviderError(format!(
                    "Video generation failed: {}",
                    reason
                )));
            }
            "pending" | "processing" | "dreaming" => {
                // Continue polling
                if attempt > 0 && attempt % 12 == 0 {
                    info!(
                        "Video generation still in progress... ({} seconds elapsed)",
                        attempt * 5
                    );
                }
            }
            _ => {
                // Unknown state, continue polling
            }
        }

        // Wait before next poll
        if attempt < max_attempts - 1 {
            tokio::time::sleep(poll_interval).await;
        }
    }

    Err(VideoGenError::ProviderError(
        "Video generation timed out after 10 minutes".to_string(),
    ))
}

#[cfg(not(feature = "local-bin"))]
async fn upload_image_to_gcs(
    gcs_client: &cloud_storage::Client,
    image_data: &str,
    mime_type: &str,
) -> Result<String, VideoGenError> {
    // Decode base64 image
    let image_bytes = BASE64
        .decode(image_data)
        .map_err(|e| VideoGenError::InvalidInput(format!("Invalid base64 image data: {}", e)))?;

    // Generate unique filename
    let file_extension = match mime_type {
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/gif" => "gif",
        "image/webp" => "webp",
        _ => "jpg", // Default to jpg
    };
    let filename = format!("lumalabs/{}.{}", Uuid::new_v4(), file_extension);

    // Upload to GCS
    let _upload_result = gcs_client
        .object()
        .create(LUMALABS_IMAGE_BUCKET, image_bytes, &filename, mime_type)
        .await
        .map_err(|e| {
            VideoGenError::NetworkError(format!("Failed to upload image to GCS: {}", e))
        })?;

    // Return public URL
    let public_url = format!(
        "https://storage.googleapis.com/{}/{}",
        LUMALABS_IMAGE_BUCKET, filename
    );

    info!("Uploaded image to GCS: {}", public_url);
    Ok(public_url)
}
