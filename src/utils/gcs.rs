use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use cloud_storage::Client;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::info;
use videogen_common::{ImageData, ImageInput};

/// Configuration for GCS image storage
pub struct GcsImageConfig {
    pub bucket: String,
    pub size_threshold_bytes: usize,
}

impl Default for GcsImageConfig {
    fn default() -> Self {
        Self {
            bucket: std::env::var("GCS_VIDEOGEN_BUCKET")
                .unwrap_or_else(|_| "videogen-images".to_string()),
            size_threshold_bytes: std::env::var("IMAGE_SIZE_THRESHOLD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1024 * 1024), // 1MB default
        }
    }
}

/// Upload an image to GCS if it exceeds the size threshold
pub async fn maybe_upload_image_to_gcs(
    client: Arc<Client>,
    image: ImageData,
    user_principal: &str,
) -> Result<ImageData, String> {
    let config = GcsImageConfig::default();

    match &image {
        ImageData::Base64(input) => {
            let size = input.data.len();

            if size <= config.size_threshold_bytes {
                info!(
                    "Image size ({} bytes) below threshold ({} bytes), keeping as base64",
                    size, config.size_threshold_bytes
                );
                return Ok(image);
            }

            info!(
                "Image size ({} bytes) exceeds threshold ({} bytes), uploading to GCS",
                size, config.size_threshold_bytes
            );

            let url = upload_image_to_gcs(client, input, user_principal).await?;
            Ok(ImageData::Url(url))
        }
        ImageData::Url(_) => {
            // Already a URL, nothing to do
            Ok(image)
        }
    }
}

/// Upload a base64 encoded image to GCS and return the public URL
pub async fn upload_image_to_gcs(
    client: Arc<Client>,
    image: &ImageInput,
    user_principal: &str,
) -> Result<String, String> {
    let config = GcsImageConfig::default();

    // Decode base64
    let image_bytes = BASE64
        .decode(&image.data)
        .map_err(|e| format!("Failed to decode base64 image: {e}"))?;

    // Generate unique filename
    let timestamp = chrono::Utc::now().timestamp_millis();
    let mut hasher = Sha256::new();
    hasher.update(&image_bytes);
    let hash = format!("{:x}", hasher.finalize());
    let hash_short = &hash[..8];

    // Extract file extension from mime type
    let extension = match image.mime_type.as_str() {
        "image/png" => "png",
        "image/jpeg" | "image/jpg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        _ => "bin",
    };

    let object_name = format!(
        "videogen/{user_principal}/{timestamp}-{hash_short}.{extension}"
    );

    info!("Uploading image to GCS: {}/{}", config.bucket, object_name);

    // Upload to GCS
    client
        .object()
        .create(&config.bucket, image_bytes, &object_name, &image.mime_type)
        .await
        .map_err(|e| format!("Failed to upload image to GCS: {e}"))?;

    // Generate public URL
    let public_url = format!(
        "https://storage.googleapis.com/{}/{}",
        config.bucket, object_name
    );

    info!("Image uploaded successfully: {}", public_url);

    Ok(public_url)
}

/// Download an image from a URL and convert to ImageInput
pub async fn download_image_from_url(url: &str) -> Result<ImageInput, String> {
    info!("Downloading image from URL: {}", url);

    // Download the image
    let response = reqwest::get(url)
        .await
        .map_err(|e| format!("Failed to download image from {url}: {e}"))?;

    if !response.status().is_success() {
        return Err(format!(
            "Failed to download image from {}: HTTP {}",
            url,
            response.status()
        ));
    }

    // Get content type
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    // Get the bytes
    let bytes = response
        .bytes()
        .await
        .map_err(|e| format!("Failed to read image bytes: {e}"))?;

    // Encode to base64
    let base64_data = BASE64.encode(&bytes);

    info!(
        "Downloaded image successfully: {} bytes, type: {}",
        bytes.len(),
        content_type
    );

    Ok(ImageInput {
        data: base64_data,
        mime_type: content_type,
    })
}

/// Convert ImageData to ImageInput, downloading from URL if necessary
pub async fn image_data_to_input(image: &ImageData) -> Result<ImageInput, String> {
    match image {
        ImageData::Base64(input) => Ok(input.clone()),
        ImageData::Url(url) => download_image_from_url(url).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_image_size_estimate() {
        let small_image = ImageData::Base64(ImageInput {
            data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==".to_string(),
            mime_type: "image/png".to_string(),
        });

        assert!(small_image.size_estimate() < 1024 * 1024);

        let url_image = ImageData::Url("https://example.com/image.png".to_string());
        assert!(url_image.size_estimate() < 1024);
    }
}
