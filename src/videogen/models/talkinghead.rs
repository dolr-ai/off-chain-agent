use crate::app_state::AppState;
use crate::consts::LUMALABS_IMAGE_BUCKET;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};
use uuid::Uuid;
use videogen_common::{AudioData, ImageData, VideoGenError, VideoGenInput, VideoGenResponse};

const TALKINGHEAD_API_BASE: &str = "https://talking-head-api.fly.dev/api/v1";
const MAX_POLL_ATTEMPTS: u32 = 120; // 10 minutes with 5 second intervals
const POLL_INTERVAL_SECS: u64 = 5;
const HEALTH_CHECK_MAX_RETRIES: u32 = 5; // Try up to 5 times
const HEALTH_CHECK_RETRY_DELAY_SECS: u64 = 20; // Wait 2 seconds between retries

#[derive(Serialize)]
struct GenerateRequest {
    image: String,
    audio: String,
    user_id: String,
}

#[derive(Deserialize)]
struct GenerateResponse {
    #[allow(dead_code)]
    status: String,
    task_id: String,
    #[allow(dead_code)]
    message: Option<String>,
}

#[derive(Deserialize)]
struct StatusResponse {
    status: String,
    #[allow(dead_code)]
    task_id: String,
    #[allow(dead_code)]
    created_at: Option<String>,
    #[allow(dead_code)]
    started_at: Option<String>,
    #[allow(dead_code)]
    completed_at: Option<String>,
    output_path: Option<String>,
    gcs_path: Option<String>,
    error_message: Option<String>,
    queue_position: Option<u32>,
}

#[derive(Deserialize)]
struct HealthResponse {
    status: String,
    #[allow(dead_code)]
    service: Option<String>,
    #[allow(dead_code)]
    tasks_count: Option<u32>,
    #[allow(dead_code)]
    queue_size: Option<u32>,
}

pub async fn generate(
    input: VideoGenInput,
    app_state: &AppState,
) -> Result<VideoGenResponse, VideoGenError> {
    let VideoGenInput::TalkingHead(model) = input else {
        return Err(VideoGenError::InvalidInput(
            "Only TalkingHead input is supported".to_string(),
        ));
    };

    info!("TalkingHead: Starting generation for user");

    // Check API health first
    check_health().await?;

    // Get user principal for GCS paths (you may need to pass this differently)
    let user_id = Uuid::new_v4().to_string(); // Using UUID as placeholder

    // Upload image to GCS
    let image_url = upload_image_to_gcs(&app_state.gcs_client, &model.image, &user_id).await?;

    // Upload audio to GCS
    let audio_url = upload_audio_to_gcs(&app_state.gcs_client, &model.audio, &user_id).await?;

    info!(
        "TalkingHead: Uploaded assets - Image: {}, Audio: {}",
        image_url, audio_url
    );

    // Call generate API
    let task_id = call_generate_api(&image_url, &audio_url, &user_id).await?;

    info!("TalkingHead: Task queued with ID: {}", task_id);

    // Poll for completion
    let status = poll_for_completion(&task_id).await?;

    // Convert output path to accessible URL, prioritizing GCS path
    let video_url = if let Some(gcs_path) = status.gcs_path {
        // Convert GCS path from gs:// format to public HTTPS URL
        // gs://bucket/path -> https://storage.googleapis.com/bucket/path
        if gcs_path.starts_with("gs://") {
            let public_url = gcs_path.replace("gs://", "https://storage.googleapis.com/");
            info!("TalkingHead: Using GCS path for video URL");
            public_url
        } else {
            // If it's already an HTTPS URL, use it directly
            info!("TalkingHead: Using provided GCS URL directly");
            gcs_path
        }
    } else if let Some(output_path) = status.output_path {
        // Fallback to local output path for backward compatibility
        // The output path is like: /workspace/ai-video-generation/MuseTalk/fastapi_server/storage/videos/{task_id}/generated_{task_id}.mp4
        // Convert to accessible URL
        info!("TalkingHead: Using local output path (GCS path not available)");
        format!("https://talking-head-api.fly.dev{}", output_path)
    } else {
        return Err(VideoGenError::ProviderError(
            "No output path or GCS path in completed response".to_string(),
        ));
    };

    info!("TalkingHead: Generation completed - Video: {}", video_url);

    Ok(VideoGenResponse {
        operation_id: task_id,
        video_url,
        provider: "talkinghead".to_string(),
    })
}

async fn check_health() -> Result<(), VideoGenError> {
    let client = reqwest::Client::new();
    let url = format!("{}/health", TALKINGHEAD_API_BASE);

    for attempt in 1..=HEALTH_CHECK_MAX_RETRIES {
        let response = match client
            .get(&url)
            .timeout(Duration::from_secs(120))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                if attempt < HEALTH_CHECK_MAX_RETRIES {
                    info!(
                        "TalkingHead: Health check attempt {}/{} failed: {}. Retrying...",
                        attempt, HEALTH_CHECK_MAX_RETRIES, e
                    );
                    sleep(Duration::from_secs(HEALTH_CHECK_RETRY_DELAY_SECS)).await;
                    continue;
                } else {
                    return Err(VideoGenError::NetworkError(format!(
                        "Health check failed after {} attempts: {}",
                        HEALTH_CHECK_MAX_RETRIES, e
                    )));
                }
            }
        };

        if response.status() == StatusCode::SERVICE_UNAVAILABLE {
            if attempt < HEALTH_CHECK_MAX_RETRIES {
                info!(
                    "TalkingHead: API unavailable on attempt {}/{}. Retrying...",
                    attempt, HEALTH_CHECK_MAX_RETRIES
                );
                sleep(Duration::from_secs(HEALTH_CHECK_RETRY_DELAY_SECS)).await;
                continue;
            } else {
                return Err(VideoGenError::ProviderError(format!(
                    "TalkingHead API is unavailable after {} attempts",
                    HEALTH_CHECK_MAX_RETRIES
                )));
            }
        }

        let health: HealthResponse = match response.json().await {
            Ok(h) => h,
            Err(e) => {
                if attempt < HEALTH_CHECK_MAX_RETRIES {
                    info!(
                        "TalkingHead: Failed to parse health response on attempt {}/{}: {}. Retrying...",
                        attempt, HEALTH_CHECK_MAX_RETRIES, e
                    );
                    sleep(Duration::from_secs(HEALTH_CHECK_RETRY_DELAY_SECS)).await;
                    continue;
                } else {
                    return Err(VideoGenError::NetworkError(format!(
                        "Failed to parse health response after {} attempts: {}",
                        HEALTH_CHECK_MAX_RETRIES, e
                    )));
                }
            }
        };

        if health.status != "healthy" {
            if attempt < HEALTH_CHECK_MAX_RETRIES {
                info!(
                    "TalkingHead: API status '{}' on attempt {}/{}. Retrying...",
                    health.status, attempt, HEALTH_CHECK_MAX_RETRIES
                );
                sleep(Duration::from_secs(HEALTH_CHECK_RETRY_DELAY_SECS)).await;
                continue;
            } else {
                return Err(VideoGenError::ProviderError(format!(
                    "TalkingHead API is not healthy after {} attempts: {}",
                    HEALTH_CHECK_MAX_RETRIES, health.status
                )));
            }
        }

        // API is healthy!
        if attempt > 1 {
            info!(
                "TalkingHead: API health check passed after {} attempts",
                attempt
            );
        } else {
            info!("TalkingHead: API health check passed");
        }
        return Ok(());
    }

    // This should never be reached due to the loop structure, but just in case
    Err(VideoGenError::ProviderError(
        "Health check failed unexpectedly".to_string(),
    ))
}

async fn call_generate_api(
    image_url: &str,
    audio_url: &str,
    user_id: &str,
) -> Result<String, VideoGenError> {
    let client = reqwest::Client::new();
    let url = format!("{}/generate", TALKINGHEAD_API_BASE);

    let request = GenerateRequest {
        image: image_url.to_string(),
        audio: audio_url.to_string(),
        user_id: user_id.to_string(),
    };

    let response = client
        .post(&url)
        .json(&request)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| VideoGenError::NetworkError(format!("Generate API call failed: {}", e)))?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());
        return Err(VideoGenError::ProviderError(format!(
            "Generate API returned {}: {}",
            status, error_text
        )));
    }

    let generate_response: GenerateResponse = response.json().await.map_err(|e| {
        VideoGenError::NetworkError(format!("Failed to parse generate response: {}", e))
    })?;

    Ok(generate_response.task_id)
}

async fn poll_for_completion(task_id: &str) -> Result<StatusResponse, VideoGenError> {
    let client = reqwest::Client::new();
    let url = format!("{}/status/{}", TALKINGHEAD_API_BASE, task_id);

    for attempt in 1..=MAX_POLL_ATTEMPTS {
        let response = client
            .get(&url)
            .timeout(Duration::from_secs(120))
            .send()
            .await
            .map_err(|e| VideoGenError::NetworkError(format!("Status check failed: {}", e)))?;

        // Allow 503 Service Unavailable and continue polling
        if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            info!(
                "TalkingHead: Status check returned 503 Service Unavailable for task {}, retrying...",
                task_id
            );
            sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
            continue;
        }

        if !response.status().is_success() {
            return Err(VideoGenError::ProviderError(format!(
                "Status API returned {}",
                response.status()
            )));
        }

        let status: StatusResponse = response.json().await.map_err(|e| {
            VideoGenError::NetworkError(format!("Failed to parse status response: {}", e))
        })?;

        match status.status.as_str() {
            "completed" => {
                info!("TalkingHead: Task {} completed successfully", task_id);
                return Ok(status);
            }
            "failed" => {
                let error_msg = status
                    .error_message
                    .unwrap_or_else(|| "Unknown error".to_string());
                error!("TalkingHead: Task {} failed: {}", task_id, error_msg);
                return Err(VideoGenError::ProviderError(format!(
                    "Generation failed: {}",
                    error_msg
                )));
            }
            "processing" | "queued" => {
                if attempt % 12 == 0 {
                    // Log every minute
                    info!(
                        "TalkingHead: Task {} still {}, position: {:?}",
                        task_id, status.status, status.queue_position
                    );
                }
                sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
            }
            _ => {
                error!("TalkingHead: Unknown status: {}", status.status);
                return Err(VideoGenError::ProviderError(format!(
                    "Unknown status: {}",
                    status.status
                )));
            }
        }
    }

    Err(VideoGenError::ProviderError(format!(
        "Generation timed out after {} seconds",
        MAX_POLL_ATTEMPTS as u64 * POLL_INTERVAL_SECS
    )))
}

async fn upload_image_to_gcs(
    gcs_client: &cloud_storage::Client,
    image_data: &ImageData,
    user_id: &str,
) -> Result<String, VideoGenError> {
    match image_data {
        ImageData::Url(url) => Ok(url.clone()),
        ImageData::Base64(image_input) => {
            // Decode base64
            let image_bytes = BASE64.decode(&image_input.data).map_err(|e| {
                VideoGenError::InvalidInput(format!("Failed to decode base64 image: {}", e))
            })?;

            // Generate unique filename
            let timestamp = chrono::Utc::now().timestamp_millis();
            let mut hasher = Sha256::new();
            hasher.update(&image_bytes);
            let hash = format!("{:x}", hasher.finalize());
            let hash_short = &hash[..8];

            // Extract file extension from mime type
            let extension = match image_input.mime_type.as_str() {
                "image/png" => "png",
                "image/jpeg" | "image/jpg" => "jpg",
                "image/gif" => "gif",
                "image/webp" => "webp",
                _ => "bin",
            };

            let filename = format!(
                "talkinghead/{}/image_{}_{}_{}.{}",
                user_id, user_id, timestamp, hash_short, extension
            );

            // Upload to GCS
            gcs_client
                .object()
                .create(
                    LUMALABS_IMAGE_BUCKET,
                    image_bytes,
                    &filename,
                    &image_input.mime_type,
                )
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
    }
}

async fn upload_audio_to_gcs(
    gcs_client: &cloud_storage::Client,
    audio_data: &AudioData,
    user_id: &str,
) -> Result<String, VideoGenError> {
    match audio_data {
        AudioData::Url(url) => Ok(url.clone()),
        AudioData::Base64(audio_input) => {
            // Decode base64
            let audio_bytes = BASE64.decode(&audio_input.data).map_err(|e| {
                VideoGenError::InvalidInput(format!("Failed to decode base64 audio: {}", e))
            })?;

            // Generate unique filename
            let timestamp = chrono::Utc::now().timestamp_millis();
            let mut hasher = Sha256::new();
            hasher.update(&audio_bytes);
            let hash = format!("{:x}", hasher.finalize());
            let hash_short = &hash[..8];

            // Extract file extension from mime type
            let extension = match audio_input.mime_type.as_str() {
                "audio/mpeg" | "audio/mp3" => "mp3",
                "audio/wav" => "wav",
                "audio/ogg" => "ogg",
                "audio/m4a" => "m4a",
                "audio/webm" => "webm",
                _ => "audio",
            };

            let filename = format!(
                "talkinghead/{}/audio_{}_{}_{}.{}",
                user_id, user_id, timestamp, hash_short, extension
            );

            // Upload to GCS
            gcs_client
                .object()
                .create(
                    LUMALABS_IMAGE_BUCKET,
                    audio_bytes,
                    &filename,
                    &audio_input.mime_type,
                )
                .await
                .map_err(|e| {
                    VideoGenError::NetworkError(format!("Failed to upload audio to GCS: {}", e))
                })?;

            // Return public URL
            let public_url = format!(
                "https://storage.googleapis.com/{}/{}",
                LUMALABS_IMAGE_BUCKET, filename
            );
            info!("Uploaded audio to GCS: {}", public_url);
            Ok(public_url)
        }
    }
}
