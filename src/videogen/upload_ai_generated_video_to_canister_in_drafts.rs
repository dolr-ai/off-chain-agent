use std::error::Error;

use candid::Principal;
use serde::{Deserialize, Serialize};
use serde_json::json;
#[allow(unused_imports)]
use yral_canisters_client::user_post_service::UserPostService;
#[allow(unused_imports)]
use crate::{
    app_state::AppState,
    consts::{USER_POST_SERVICE_CANISTER_ID, YRAL_UPLOAD_SERVICE},
};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UploadAiVideoToCanisterRequest {
    pub ai_video_url: String,
    pub user_id: Principal,
}

#[allow(unused_variables)]
pub async fn upload_ai_generated_video_to_canister_impl(
    ai_video_url: &str,
    user_id: Principal,
    app_state: &AppState,
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
    log::info!("Video upload url is {}",video_upload_url);
    let video_id = video_upload_data
        .video_id
        .ok_or_else(|| "Video ID not found in response".to_string())?;

    let stream_upload_form = reqwest::multipart::Form::new().part(
        "file",
        reqwest::multipart::Part::bytes(video_bytes.to_vec())
            .file_name("ai_generated_video.mp4")
            .mime_str("video/mp4")
            .map_err(|e| Box::<dyn Error>::from(format!("Failed to set MIME type: {e}")))?,
    );

    let stream_upload_result = client
        .post(&video_upload_url)
        .multipart(stream_upload_form)
        .send()
        .await
        .map_err(|e| Box::<dyn Error>::from(format!("Failed to upload video: {e}")))?;

    if !stream_upload_result.status().is_success() {
        let error_text = stream_upload_result.text().await.unwrap_or_default();
        return Err(format!("Video upload failed with error: {}", error_text).into());
    }

    // // Call user post service to add the post
    // let user_post_service =
    //     UserPostService(*USER_POST_SERVICE_CANISTER_ID, &app_state.agent);

    // log::info!(
    //     "Calling add_post_v1 for video_id: {} and user_id: {}",
    //     video_id,
    //     user_id
    // );

    // use yral_canisters_client::user_post_service::{
    //     PostDetailsFromFrontendV1, PostStatusFromFrontend, Result_ as CanisterResult,
    // };

    // let post_details = PostDetailsFromFrontendV1 {
    //     id: uuid::Uuid::new_v4().to_string(),
    //     status: PostStatusFromFrontend::Draft,
    //     hashtags: vec![],
    //     description: "".to_string(),
    //     video_uid: video_id,
    //     creator_principal: user_id,
    // };

    // match user_post_service.add_post_v1(post_details).await {
    //     Ok(CanisterResult::Ok) => {
    //         log::info!("Successfully registered video in canister");
    //         Ok(())
    //     }
    //     Ok(CanisterResult::Err(e)) => Err(Box::<dyn Error>::from(format!(
    //         "Failed to register video in canister: {e}"
    //     ))),
    //     Err(e) => Err(Box::<dyn Error>::from(format!(
    //         "Failed to call canister: {e}"
    //     ))),
    // }
    Ok(())
}
