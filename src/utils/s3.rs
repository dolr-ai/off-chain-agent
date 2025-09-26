use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{config::Credentials, primitives::ByteStream, Client};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use image::{DynamicImage, ImageFormat};
use std::env;
use std::io::Cursor;
use tracing::info;

// Hetzner Object Storage configuration constants
pub const HETZNER_S3_ENDPOINT: &str = "https://hel1.your-objectstorage.com";
pub const HETZNER_S3_BUCKET: &str = "yral-profile";
pub const HETZNER_S3_REGION: &str = "hel1";
pub const HETZNER_S3_PUBLIC_URL_BASE: &str = "https://yral-profile.hel1.your-objectstorage.com";

/// Configuration for S3 storage
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub public_url_base: String,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: env::var("HETZNER_S3_ENDPOINT")
                .unwrap_or_else(|_| HETZNER_S3_ENDPOINT.to_string()),
            bucket: env::var("HETZNER_S3_BUCKET").unwrap_or_else(|_| HETZNER_S3_BUCKET.to_string()),
            region: env::var("HETZNER_S3_REGION").unwrap_or_else(|_| HETZNER_S3_REGION.to_string()),
            public_url_base: env::var("HETZNER_S3_PUBLIC_URL_BASE")
                .unwrap_or_else(|_| HETZNER_S3_PUBLIC_URL_BASE.to_string()),
        }
    }
}

/// Create an S3 client configured for Hetzner Object Storage
pub async fn create_s3_client() -> Result<Client, String> {
    let config = S3Config::default();

    // Get credentials from environment variables
    let access_key = env::var("HETZNER_S3_ACCESS_KEY")
        .map_err(|_| "Missing HETZNER_S3_ACCESS_KEY environment variable".to_string())?;
    let secret_key = env::var("HETZNER_S3_SECRET_KEY")
        .map_err(|_| "Missing HETZNER_S3_SECRET_KEY environment variable".to_string())?;

    // Create credentials
    let credentials = Credentials::new(access_key, secret_key, None, None, "hetzner-s3");

    // Configure the S3 client for Hetzner
    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(config.region))
        .credentials_provider(credentials)
        .endpoint_url(&config.endpoint)
        .load()
        .await;

    Ok(Client::new(&aws_config))
}

/// Process and optimize image data
fn process_image(image_bytes: Vec<u8>) -> Result<Vec<u8>, String> {
    // Load the image
    let img = image::load_from_memory(&image_bytes)
        .map_err(|e| format!("Failed to load image: {e}"))?;

    // Check dimensions and resize if needed
    const MAX_SIZE: u32 = 1000;
    let (width, height) = (img.width(), img.height());

    let processed_img = if width > MAX_SIZE || height > MAX_SIZE {
        // Calculate new dimensions maintaining aspect ratio
        let ratio = (MAX_SIZE as f32 / width.max(height) as f32).min(1.0);
        let new_width = (width as f32 * ratio) as u32;
        let new_height = (height as f32 * ratio) as u32;

        info!(
            "Resizing image from {}x{} to {}x{}",
            width, height, new_width, new_height
        );

        img.resize(new_width, new_height, image::imageops::FilterType::Lanczos3)
    } else {
        img
    };

    // Convert to RGB8 (remove alpha channel if present) and encode as JPEG
    let rgb_img = DynamicImage::ImageRgb8(processed_img.to_rgb8());

    // Encode as JPEG with 85% quality
    let mut output = Vec::new();
    let mut cursor = Cursor::new(&mut output);

    rgb_img
        .write_to(&mut cursor, ImageFormat::Jpeg)
        .map_err(|e| format!("Failed to encode image as JPEG: {e}"))?;

    Ok(output)
}

/// Upload a profile image to S3 and return the public URL
pub async fn upload_profile_image_to_s3(
    image_data_base64: &str,
    user_principal: &str,
) -> Result<String, String> {
    let config = S3Config::default();

    // Decode base64 image data
    let image_bytes = BASE64
        .decode(image_data_base64)
        .map_err(|e| format!("Failed to decode base64 image: {e}"))?;

    // Process and optimize the image
    let processed_bytes = process_image(image_bytes)?;

    info!(
        "Processed image size: {} bytes (~{}KB)",
        processed_bytes.len(),
        processed_bytes.len() / 1024
    );

    // Create S3 client
    let client = create_s3_client().await?;

    // Use timestamp to prevent caching issues
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("Failed to get timestamp: {e}"))?
        .as_secs();

    // Use user principal and timestamp for unique object key
    let object_key = format!("users/{}/profile-{}.jpg", user_principal, timestamp);

    info!(
        "Uploading profile image to S3: {}/{}",
        config.bucket, object_key
    );

    // Upload the processed image to S3
    let put_request = client
        .put_object()
        .bucket(&config.bucket)
        .key(&object_key)
        .body(ByteStream::from(processed_bytes))
        .content_type("image/jpeg")
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead);

    put_request
        .send()
        .await
        .map_err(|e| format!("Failed to upload image to S3: {e}"))?;

    info!(
        "Successfully uploaded profile image for user: {}",
        user_principal
    );

    // Return the public URL of the uploaded image
    let public_url = format!("{}/{}", config.public_url_base, object_key);
    Ok(public_url)
}

/// Delete old profile images from S3 for a user
/// Since we now use timestamp-based names, we list and delete all profile images for the user
pub async fn delete_profile_image_from_s3(user_principal: &str) -> Result<(), String> {
    let config = S3Config::default();

    // Create S3 client
    let client = create_s3_client().await?;

    // List all profile images for this user
    let prefix = format!("users/{}/profile-", user_principal);

    let list_response = client
        .list_objects_v2()
        .bucket(&config.bucket)
        .prefix(&prefix)
        .send()
        .await
        .map_err(|e| format!("Failed to list objects from S3: {e}"))?;

    // Delete all found profile images for this user
    let objects = list_response.contents();
    for object in objects {
        if let Some(key) = object.key() {
            info!("Deleting old profile image from S3: {}/{}", config.bucket, key);

            client
                .delete_object()
                .bucket(&config.bucket)
                .key(key)
                .send()
                .await
                .map_err(|e| format!("Failed to delete image from S3: {e}"))?;
        }
    }

    info!(
        "Successfully deleted profile images for user: {}",
        user_principal
    );

    Ok(())
}
