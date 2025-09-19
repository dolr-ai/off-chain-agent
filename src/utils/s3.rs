use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{
    config::Credentials,
    primitives::ByteStream,
    Client,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use std::env;
use tracing::info;

// Hetzner Object Storage configuration constants
pub const HETZNER_S3_ENDPOINT: &str = "https://fsn1.your-objectstorage.com";
pub const HETZNER_S3_BUCKET: &str = "yral-profile-images";
pub const HETZNER_S3_REGION: &str = "fsn1";
pub const HETZNER_S3_PUBLIC_URL_BASE: &str = "https://yral-profile-images.fsn1.your-objectstorage.com";

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
            bucket: env::var("HETZNER_S3_BUCKET")
                .unwrap_or_else(|_| HETZNER_S3_BUCKET.to_string()),
            region: env::var("HETZNER_S3_REGION")
                .unwrap_or_else(|_| HETZNER_S3_REGION.to_string()),
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
    let credentials = Credentials::new(
        access_key,
        secret_key,
        None,
        None,
        "hetzner-s3",
    );

    // Configure the S3 client for Hetzner
    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(config.region))
        .credentials_provider(credentials)
        .endpoint_url(&config.endpoint)
        .load()
        .await;

    Ok(Client::new(&aws_config))
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

    // Create S3 client
    let client = create_s3_client().await?;

    // Use user principal as the object key with .jpg extension
    let object_key = format!("users/{}/profile.jpg", user_principal);

    info!(
        "Uploading profile image to S3: {}/{}",
        config.bucket, object_key
    );

    // Upload the image to S3
    let put_request = client
        .put_object()
        .bucket(&config.bucket)
        .key(&object_key)
        .body(ByteStream::from(image_bytes))
        .content_type("image/jpeg")
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead);

    put_request
        .send()
        .await
        .map_err(|e| format!("Failed to upload image to S3: {e}"))?;

    info!("Successfully uploaded profile image for user: {}", user_principal);

    // Return the public URL of the uploaded image
    let public_url = format!("{}/{}", config.public_url_base, object_key);
    Ok(public_url)
}

/// Delete a profile image from S3
pub async fn delete_profile_image_from_s3(user_principal: &str) -> Result<(), String> {
    let config = S3Config::default();

    // Create S3 client
    let client = create_s3_client().await?;

    // Use user principal as the object key
    let object_key = format!("users/{}/profile.jpg", user_principal);

    info!(
        "Deleting profile image from S3: {}/{}",
        config.bucket, object_key
    );

    // Delete the image from S3
    client
        .delete_object()
        .bucket(&config.bucket)
        .key(&object_key)
        .send()
        .await
        .map_err(|e| format!("Failed to delete image from S3: {e}"))?;

    info!("Successfully deleted profile image for user: {}", user_principal);

    Ok(())
}