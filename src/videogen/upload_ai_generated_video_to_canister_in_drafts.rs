use std::{env, error::Error};

use candid::Principal;
use http::header::AUTHORIZATION;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::consts::YRAL_UPLOAD_VIDEO_WORKER_URL;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UploadAiVideoToCanisterRequest {
    pub ai_video_url: String,
    pub user_id: Principal,
}

pub async fn upload_ai_generated_video_to_canister_impl(
    ai_video_url: &str,
    user_id: Principal,
) -> Result<(), Box<dyn Error>> {
    let video_fetch_response = reqwest::get(ai_video_url).await?;
    let yral_cloudflare_worker_token = env::var("YRAL_CLOUDFLARE_WORKER_GRPC_AUTH_TOKEN")?;

    if !video_fetch_response.status().is_success() {
        let status = video_fetch_response.status();
        let error_body = video_fetch_response
            .text()
            .await
            .unwrap_or_else(|_| "Could not read response body".to_string());
        return Err(format!(
            "Failed to fetch video from URL: {}. Status: {}. Response body: {}",
            ai_video_url, status, error_body
        )
        .into());
    }

    let video_bytes = video_fetch_response.bytes().await?;

    let get_video_upload_url =
        YRAL_UPLOAD_VIDEO_WORKER_URL.join("/create_video_url_for_ai_draft")?;
    let client = reqwest::Client::new();
    let get_video_upload_res = client
        .post(get_video_upload_url)
        .header(
            AUTHORIZATION,
            format!("Bearer {}", yral_cloudflare_worker_token),
        )
        .json(&json!({
            "user-id": user_id.to_string(),
        }))
        .send()
        .await?;

    if !get_video_upload_res.status().is_success() {
        let status = get_video_upload_res.status();
        let error_body = get_video_upload_res
            .text()
            .await
            .unwrap_or_else(|_| "Could not read response body".to_string());

        return Err(format!(
            "Failed to get video upload URL. Status: {}. Response body: {}",
            status, error_body
        )
        .into());
    }

    // Internal types for server function
    #[derive(Deserialize)]
    pub struct UploadUrlResponse {
        pub data: Option<UploadUrlData>,
        pub success: bool,
        pub message: Option<String>,
    }

    #[derive(Deserialize)]
    pub struct UploadUrlData {
        pub uid: Option<String>,
        #[serde(rename = "uploadURL")]
        pub upload_url: Option<String>,
    }

    let yral_upload_video_get_url_result = get_video_upload_res.json::<UploadUrlResponse>().await?;

    if !yral_upload_video_get_url_result.success {
        return Err(format!(
            "Yral upload worker get url request failed: {}",
            yral_upload_video_get_url_result.message.unwrap_or_default()
        )
        .into());
    }

    let cloudflare_video_upload_url = yral_upload_video_get_url_result
        .data
        .and_then(|data| data.upload_url)
        .ok_or_else(|| "Upload URL not found in response".to_string())?;

    let cloudflare_stream_upload_form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes(video_bytes.to_vec())
            .file_name("ai_generated_video.mp4")
            .mime_str("video/mp4")
            .map_err(|e| Box::<dyn Error>::from(format!("Failed to set MIME type: {e}")))?,
    );

    let cloudflare_stream_upload_result = client
        .post(&cloudflare_video_upload_url)
        .multipart(cloudflare_stream_upload_form)
        .send()
        .await
        .map_err(|e| Box::<dyn Error>::from(format!("Failed to upload to Cloudflare: {e}")))?;

    if !cloudflare_stream_upload_result.status().is_success() {
        let error_text = cloudflare_stream_upload_result
            .text()
            .await
            .unwrap_or_default();
        return Err(format!("Cloudflare upload failed with error: {}", error_text).into());
    }

    Ok(())
}
