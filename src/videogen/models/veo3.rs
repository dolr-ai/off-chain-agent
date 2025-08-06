use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;
use videogen_common::{Veo3AspectRatio, VideoGenError, VideoGenInput, VideoGenResponse};

use crate::app_state::AppState;
use crate::consts::{VEO3_LOCATION, VEO3_PROJECT_ID, VEO3_STORAGE_URI};
use crate::utils::gcs::image_data_to_input;

#[derive(Serialize)]
struct Veo3Request {
    instances: Vec<Veo3Instance>,
    parameters: Veo3Parameters,
}

#[derive(Serialize)]
struct Veo3Instance {
    prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    image: Option<Veo3Image>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Veo3Image {
    bytes_base64_encoded: String,
    mime_type: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Veo3Parameters {
    sample_count: u32,
    generate_audio: bool,
    aspect_ratio: Veo3AspectRatio,
    duration_seconds: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    negative_prompt: Option<String>,
}

#[derive(Deserialize)]
struct Veo3Response {
    name: String,
}

#[derive(Deserialize, Debug)]
struct Veo3OperationResponse {
    done: Option<bool>,
    response: Option<serde_json::Value>,
    error: Option<Veo3OperationError>,
}

#[derive(Deserialize, Debug)]
struct Veo3OperationError {
    code: i32,
    message: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Veo3GenerateVideoResponse {
    #[serde(rename = "@type")]
    #[allow(dead_code)]
    type_field: String,
    rai_media_filtered_count: Option<i32>,
    rai_media_filtered_reasons: Option<Vec<String>>,
    videos: Option<Vec<Veo3Video>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Veo3Video {
    gcs_uri: String,
    #[allow(dead_code)]
    mime_type: Option<String>,
}

pub async fn generate(
    input: VideoGenInput,
    app_state: &AppState,
) -> Result<VideoGenResponse, VideoGenError> {
    let VideoGenInput::Veo3(model) = input else {
        return Err(VideoGenError::InvalidInput(
            "Invalid input type for Veo3 provider".to_string(),
        ));
    };

    let prompt = model.prompt;
    let negative_prompt = model.negative_prompt;
    let image_data = model.image;
    let aspect_ratio = model.aspect_ratio;
    let duration_seconds = model.duration_seconds;
    let generate_audio = model.generate_audio;
    
    // Convert ImageData to ImageInput if needed
    let image = if let Some(ref img_data) = image_data {
        Some(image_data_to_input(img_data).await.map_err(|e| {
            VideoGenError::InvalidInput(format!("Failed to process image: {}", e))
        })?)
    } else {
        None
    };

    // Get access token using app_state
    let access_token = app_state
        .get_access_token(&["https://www.googleapis.com/auth/cloud-platform"])
        .await;

    let model_id = "veo-3.0-generate-preview";
    let url = format!(
        "https://{VEO3_LOCATION}-aiplatform.googleapis.com/v1/projects/{VEO3_PROJECT_ID}/locations/{VEO3_LOCATION}/publishers/google/models/{model_id}:predictLongRunning"
    );

    // Build instance
    let mut instance = Veo3Instance {
        prompt: prompt.clone(),
        image: None,
    };

    // Add image if provided
    if let Some(img) = image {
        instance.image = Some(Veo3Image {
            bytes_base64_encoded: img.data,
            mime_type: img.mime_type,
        });
    }

    // Build request
    let request = Veo3Request {
        instances: vec![instance],
        parameters: Veo3Parameters {
            sample_count: 1,
            generate_audio,
            aspect_ratio,
            duration_seconds,
            storage_uri: Some(VEO3_STORAGE_URI.to_string()),
            negative_prompt,
        },
    };

    // Make API call
    let client = reqwest::Client::new();
    let response = client
        .post(&url)
        .bearer_auth(&access_token)
        .header("Content-Type", "application/json; charset=utf-8")
        .json(&request)
        .send()
        .await
        .map_err(|e| VideoGenError::NetworkError(e.to_string()))?;

    if !response.status().is_success() {
        let error_text = response
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());
        return Err(VideoGenError::ProviderError(format!(
            "Veo3 API error: {error_text}"
        )));
    }

    let veo_response: Veo3Response = response
        .json()
        .await
        .map_err(|e| VideoGenError::ProviderError(format!("Failed to parse response: {e}")))?;

    info!(
        "Video generation started with operation: {}",
        veo_response.name
    );

    // Poll for completion
    let video_url = poll_for_completion(&veo_response.name, &access_token).await?;

    Ok(VideoGenResponse {
        operation_id: veo_response.name.clone(),
        video_url,
        provider: "veo3".to_string(),
    })
}

async fn poll_for_completion(
    operation_name: &str,
    access_token: &str,
) -> Result<String, VideoGenError> {
    let model_id = "veo-3.0-generate-preview";
    let poll_url = format!(
        "https://{VEO3_LOCATION}-aiplatform.googleapis.com/v1/projects/{VEO3_PROJECT_ID}/locations/{VEO3_LOCATION}/publishers/google/models/{model_id}:fetchPredictOperation"
    );

    let client = reqwest::Client::new();
    let max_attempts = 120; // 10 minutes with 5 second intervals
    let poll_interval = std::time::Duration::from_secs(5);

    info!("Starting to poll for video generation completion");

    for attempt in 0..max_attempts {
        let request_body = serde_json::json!({
            "operationName": operation_name
        });

        let response = client
            .post(&poll_url)
            .bearer_auth(access_token)
            .header("Content-Type", "application/json; charset=utf-8")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| VideoGenError::NetworkError(format!("Failed to poll operation: {e}")))?;

        if !response.status().is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(VideoGenError::ProviderError(format!(
                "Failed to check operation status: {error_text}"
            )));
        }

        let resjson = response
            .json::<Value>()
            .await
            .map_err(|e| VideoGenError::ProviderError(format!("Failed to parse response: {e}")))?;

        log::debug!("Operation status response: {resjson:?}");

        let operation_status: Veo3OperationResponse = serde_json::from_value(resjson.clone())
            .map_err(|e| {
                VideoGenError::ProviderError(format!("Failed to parse operation response: {e}"))
            })?;

        if operation_status.done == Some(true) {
            // Check for error first
            if let Some(error) = operation_status.error {
                // Check if this is a content policy violation
                if error.code == 3
                    && error
                        .message
                        .contains("violate Vertex AI's usage guidelines")
                {
                    return Err(VideoGenError::InvalidInput(format!(
                        "Content policy violation: {}",
                        error.message
                    )));
                }

                // Other errors
                return Err(VideoGenError::ProviderError(format!(
                    "Operation failed with error code {}: {}",
                    error.code, error.message
                )));
            }

            // Check for successful response
            if let Some(result_value) = operation_status.response {
                // Parse the response as Veo3GenerateVideoResponse
                match serde_json::from_value::<Veo3GenerateVideoResponse>(result_value.clone()) {
                    Ok(video_response) => {
                        // Check if content was filtered
                        if let Some(reasons) = video_response.rai_media_filtered_reasons {
                            if !reasons.is_empty() {
                                let reason_text = reasons.join("; ");
                                return Err(VideoGenError::InvalidInput(format!(
                                    "Content was filtered by Responsible AI practices: {reason_text}"
                                )));
                            }
                        }

                        // Check if we have videos
                        if let Some(videos) = video_response.videos {
                            if let Some(first_video) = videos.first() {
                                // Convert GCS URL to public HTTPS URL
                                let public_url = convert_gcs_to_public_url(&first_video.gcs_uri)?;
                                info!(
                                    "Video generation completed. GCS URL: {} -> Public URL: {}",
                                    first_video.gcs_uri, public_url
                                );
                                return Ok(public_url);
                            }
                        }

                        // Check if videos were filtered
                        if video_response.rai_media_filtered_count.unwrap_or(0) > 0 {
                            return Err(VideoGenError::InvalidInput(
                                "All generated videos were filtered by Responsible AI practices"
                                    .to_string(),
                            ));
                        }

                        return Err(VideoGenError::ProviderError(
                            "Operation completed but no videos were generated".to_string(),
                        ));
                    }
                    Err(e) => {
                        log::error!("Failed to parse video response: {e:?}, raw: {result_value:?}");
                        return Err(VideoGenError::ProviderError(format!(
                            "Failed to parse video generation response: {e}"
                        )));
                    }
                }
            }
            return Err(VideoGenError::ProviderError(
                "Operation completed but no response data found".to_string(),
            ));
        }

        // Still processing, log progress every 6 attempts (30 seconds)
        if attempt > 0 && attempt % 6 == 0 {
            info!(
                "Video generation still in progress... ({} seconds elapsed)",
                attempt * 5
            );
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

/// Convert GCS URL (gs://) to public HTTPS URL
fn convert_gcs_to_public_url(gcs_url: &str) -> Result<String, VideoGenError> {
    // Check if it's a GCS URL
    if !gcs_url.starts_with("gs://") {
        return Err(VideoGenError::ProviderError(format!(
            "Invalid GCS URL format: {gcs_url}"
        )));
    }

    // Remove the gs:// prefix and convert to public URL
    let path = gcs_url.strip_prefix("gs://").unwrap();
    let public_url = format!("https://storage.googleapis.com/{path}");

    Ok(public_url)
}
