use std::sync::Arc;

use crate::{app_state::AppState, AppError};
use anyhow::Error;
use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::instrument;

use super::cloudflare::delete_cloudflare_video;
use super::event::UploadVideoInfo;
use super::hls::HlsProcessingResponse;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VideoFinalizeV2Request {
    pub video_id: String,
    pub video_info: UploadVideoInfo,
    pub is_nsfw: bool,
    pub hls_response: HlsProcessingResponse,
}

#[instrument(skip(state))]
pub async fn finalize_video_v2(
    State(state): State<Arc<AppState>>,
    Json(request): Json<VideoFinalizeV2Request>,
) -> Result<Json<serde_json::Value>, AppError> {
    let video_id = &request.video_id;
    let video_info = &request.video_info;

    log::info!("Finalizing video v2 for: {}", video_id);

    // Get HLS response directly from the request
    let hls_response = &request.hls_response;

    log::info!("Processing finalization with HLS response for video: {}", video_id);

    let video_to_upload_url = {
               // Video was ≤1080p, need to upload the original
               log::info!("Uploading original video (≤1080p) to Storj");
        
               // Download the original from Cloudflare
               let video_url = format!(
                   "https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{}/downloads/default.mp4",
                   video_id
               );
               
               let client = reqwest::Client::new();
               let response = client.get(&video_url).send().await?;
               let video_bytes = response.bytes().await?;
               
               // Create temp file
               let temp_path = format!("/tmp/{}_original.mp4", video_id);
               std::fs::write(&temp_path, &video_bytes)?;
       
               // Upload original video to Storj
               let video_args = storj_interface::duplicate::Args {
                   publisher_user_id: video_info.publisher_user_id.clone(),
                   video_id: format!("videos/{}.mp4", video_id),
                   is_nsfw: request.is_nsfw,
                   metadata: [
                       ("post_id".into(), video_info.post_id.to_string()),
                       ("timestamp".into(), video_info.timestamp.clone()),
                       ("file_type".into(), "video_original".into()),
                       ("resolution".into(), format!("{}x{}", 
                           hls_response.original_resolution.0, 
                           hls_response.original_resolution.1)),
                   ]
                   .into(),
               };
               
               state.qstash_client.duplicate_to_storj(video_args).await?;
               
               // Clean up temp file
               std::fs::remove_file(&temp_path).ok();
               
               Some(format!("https://link.storjshare.io/raw/videos/{}.mp4", video_id))
    };

    #[cfg(not(feature = "local-bin"))]
    {
        store_final_video_urls(
            &state.bigquery_client,
            video_id,
            video_to_upload_url.as_deref(),
            &hls_response.hls_url,
        ).await?;
    }

    log::info!("Video processing finalized for: {}", video_id);

    Ok(Json(serde_json::json!({
        "message": "Video processing finalized via callback",
        "video_id": video_id,
        "video_url": video_to_upload_url,
        "hls_url": hls_response.hls_url,
    })))
}

#[cfg(not(feature = "local-bin"))]
async fn store_final_video_urls(
    bigquery_client: &google_cloud_bigquery::client::Client,
    video_id: &str,
    video_url: Option<&str>,
    hls_url: &str,
) -> Result<(), Error> {
    use google_cloud_bigquery::http::job::query::QueryRequest;

    let query = format!(
        "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.video_final_urls` 
         (video_id, video_url, hls_url, created_at) 
         VALUES ('{}', '{}', '{}', CURRENT_TIMESTAMP())",
        video_id, 
        video_url.unwrap_or(""),
        hls_url
    );

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await?;

    Ok(())
}