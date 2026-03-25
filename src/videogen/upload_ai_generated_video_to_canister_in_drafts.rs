use std::error::Error;

#[allow(unused_imports)]
use crate::{
    app_state::AppState,
    consts::{STORJ_INTERFACE_TOKEN, USER_POST_SERVICE_CANISTER_ID, YRAL_UPLOAD_SERVICE},
};
use candid::Principal;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UploadAiVideoToCanisterRequest {
    pub ai_video_url: String,
    pub user_id: Principal,
    pub delegated_identity: Option<yral_types::delegated_identity::DelegatedIdentityWire>,
}

pub async fn upload_ai_generated_video_to_canister_impl(
    ai_video_url: &str,
    user_id: Principal,
    delegated_identity: Option<yral_types::delegated_identity::DelegatedIdentityWire>,
) -> Result<(), Box<dyn Error>> {
    let video_fetch_response = reqwest::get(ai_video_url).await?;

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

    let content_type = video_fetch_response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    let video_bytes = video_fetch_response.bytes().await?;
    let get_video_upload_url = YRAL_UPLOAD_SERVICE.join("/get-upload-url")?;
    let client = reqwest::Client::new();
    let get_video_upload_res = client
        .post(get_video_upload_url)
        .json(&json!({
            "publisher_user_id": user_id.to_string(),
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
        pub error_message: Option<String>,
    }

    #[derive(Deserialize)]
    pub struct UploadUrlData {
        pub video_id: Option<String>,
        pub upload_url: Option<String>,
    }

    let yral_upload_video_get_url_result = get_video_upload_res.json::<UploadUrlResponse>().await?;

    if !yral_upload_video_get_url_result.success {
        return Err(format!(
            "Yral upload service get url request failed: {}",
            yral_upload_video_get_url_result
                .error_message
                .unwrap_or_default()
        )
        .into());
    }

    let video_upload_data = yral_upload_video_get_url_result
        .data
        .ok_or_else(|| "Upload data not found in response".to_string())?;

    let video_upload_url = video_upload_data
        .upload_url
        .ok_or_else(|| "Upload URL not found in response".to_string())?;
    log::info!("Video upload url is {}", video_upload_url);
    let video_id = video_upload_data
        .video_id
        .ok_or_else(|| "Video ID not found in response".to_string())?;

    let stream_upload_form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes(video_bytes.to_vec())
            .file_name(format!("{}.mp4", video_id))
            .mime_str("video/mp4")
            .map_err(|e| Box::<dyn Error>::from(format!("Failed to set MIME type: {e}")))?,
    );

    let stream_upload_result = client
        .post(&video_upload_url)
        .multipart(stream_upload_form)
        .send()
        .await
        .map_err(|e| Box::<dyn Error>::from(format!("Failed to upload video: {e}")))?;

    let status = stream_upload_result.status();
    let body_text = stream_upload_result.text().await.unwrap_or_default();
    log::info!(
        "Video upload response status: {}. Body: {}",
        status,
        body_text
    );

    if !status.is_success() {
        return Err(format!("Video upload failed with error: {}", body_text).into());
    }

    // Call upload service to update metadata and register post
    let update_metadata_url = YRAL_UPLOAD_SERVICE.join("/update-video-metadata")?;

    if let Some(identity) = delegated_identity {
        log::info!(
            "Calling /update-video-metadata for video_id: {} and user_id: {}",
            video_id,
            user_id
        );

        let post_details = json!({
            "id": video_id.clone(),
            "video_uid": video_id,
            "creator_principal": user_id.to_string(),
            "status": "Draft",
            "hashtags": Vec::<String>::new(),
            "description": ""
        });

        let update_req = json!({
            "delegated_identity_wire": identity,
            "meta": {},
            "post_details": post_details
        });

        let update_res = client
            .post(update_metadata_url)
            .json(&update_req)
            .send()
            .await?;

        if !update_res.status().is_success() {
            let error_text = update_res.text().await.unwrap_or_default();
            return Err(format!("Failed to update video metadata: {}", error_text).into());
        }

        log::info!(
            "Successfully updated video metadata and registered in canister via upload service"
        );
    } else {
        log::warn!(
            "No delegated identity found, skipping canister registration via upload service"
        );
    }

    Ok(())
}
