use anyhow::{anyhow, Error};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::env;
use tracing::instrument;

use crate::consts::CLOUDFLARE_ACCOUNT_ID;

#[derive(Debug, Serialize, Deserialize)]
struct CloudflareStreamResponse {
    success: bool,
    errors: Vec<CloudflareError>,
    messages: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CloudflareError {
    code: i32,
    message: String,
}

#[instrument]
pub async fn delete_cloudflare_video(video_id: &str) -> Result<(), Error> {
    let cloudflare_api_token = env::var("CLOUDFLARE_API_TOKEN")
        .map_err(|_| anyhow!("CLOUDFLARE_API_TOKEN environment variable not set"))?;

    let client = Client::new();
    let url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{}/stream/{}",
        CLOUDFLARE_ACCOUNT_ID, video_id
    );

    log::info!("Deleting Cloudflare Stream video: {}", video_id);

    let response = client
        .delete(&url)
        .header("Authorization", format!("Bearer {}", cloudflare_api_token))
        .send()
        .await?;

    if !response.status().is_success() {
        let error_text = response.text().await?;
        return Err(anyhow!(
            "Failed to delete Cloudflare video {}: {}",
            video_id,
            error_text
        ));
    }

    let cf_response: CloudflareStreamResponse = response.json().await?;
    
    if !cf_response.success {
        let error_messages: Vec<String> = cf_response
            .errors
            .into_iter()
            .map(|e| format!("{}: {}", e.code, e.message))
            .collect();
        
        return Err(anyhow!(
            "Cloudflare API returned errors for video {}: {}",
            video_id,
            error_messages.join(", ")
        ));
    }

    log::info!("Successfully deleted Cloudflare Stream video: {}", video_id);
    Ok(())
}

// Function to check if a video exists on Cloudflare Stream
#[instrument]
pub async fn check_cloudflare_video_exists(video_id: &str) -> Result<bool, Error> {
    let cloudflare_api_token = env::var("CLOUDFLARE_API_TOKEN")
        .map_err(|_| anyhow!("CLOUDFLARE_API_TOKEN environment variable not set"))?;

    let client = Client::new();
    let url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{}/stream/{}",
        CLOUDFLARE_ACCOUNT_ID, video_id
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", cloudflare_api_token))
        .send()
        .await?;

    Ok(response.status().is_success())
}