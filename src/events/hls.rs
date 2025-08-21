use std::sync::Arc;
use std::process::Command;
use std::fs;

use crate::{app_state::AppState, events::event::UploadVideoInfoV2, AppError};
use anyhow::{anyhow, Error};
use axum::{extract::State, Json};
use hlskit::{
    models::{
        hls_video::HlsVideo,
        hls_video_processing_settings::{
            FfmpegVideoProcessingPreset, HlsVideoProcessingSettings,
            HlsVideoAudioBitrate, HlsVideoAudioCodec,
        },
    },
    process_video,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::instrument;


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HlsProcessingRequest {
    pub video_id: String,
    pub video_info: UploadVideoInfoV2,
    pub is_nsfw: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HlsProcessingResponse {
    pub original_resolution: (u32, u32),
    pub processed_resolution: (u32, u32),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VideoMetadata {
    pub width: u32,
    pub height: u32,
    pub duration: f64,
    pub bitrate: u64,
}

#[instrument]
async fn get_video_metadata(video_url: &str) -> Result<VideoMetadata, Error> {
    let output = tokio::task::spawn_blocking({
        let url = video_url.to_string();
        move || {
            Command::new("ffprobe")
                .args(&[
                    "-v", "error",
                    "-select_streams", "v:0",
                    "-show_entries", "stream=width,height,duration,bit_rate",
                    "-of", "json",
                    &url,
                ])
                .output()
        }
    })
    .await??;

    if !output.status.success() {
        return Err(anyhow!("Failed to get video metadata"));
    }

    #[derive(Deserialize)]
    struct FfprobeOutput {
        streams: Vec<FfprobeStream>,
    }

    #[derive(Deserialize)]
    struct FfprobeStream {
        width: u32,
        height: u32,
        duration: Option<String>,
        bit_rate: Option<String>,
    }

    let json: FfprobeOutput = serde_json::from_slice(&output.stdout)?;
    let stream = json.streams.into_iter().next()
        .ok_or_else(|| anyhow!("No video stream found"))?;

    Ok(VideoMetadata {
        width: stream.width,
        height: stream.height,
        duration: stream.duration
            .and_then(|d| d.parse::<f64>().ok())
            .unwrap_or(0.0),
        bitrate: stream.bit_rate
            .and_then(|b| b.parse::<u64>().ok())
            .unwrap_or(0),
    })
}

#[instrument]
async fn download_video(video_url: &str) -> Result<Vec<u8>, Error> {
    let client = Client::new();
    let response = client
        .get(video_url)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(anyhow!("Failed to download video: {}", response.status()));
    }

    let mut buf = Vec::new();
    let mut stream = response.bytes_stream();
    use futures::StreamExt;
    
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buf.extend_from_slice(&chunk);
    }

    Ok(buf)
}

#[instrument(skip(hls_data, qstash_client))]
async fn upload_hls_to_storj(
    video_id: &str,
    hls_data: HlsVideo,
    video_info: &UploadVideoInfoV2,
    qstash_client: &crate::qstash::client::QStashClient,
    is_nsfw: bool
) -> Result<(), Error> {
    // Create temporary directory for HLS files
    let temp_dir = format!("/tmp/hls_{}", video_id);
    fs::create_dir_all(&temp_dir)?;
    
    let base_path = format!("{}/hls", video_id);

    // Write and upload master playlist
    let master_path = format!("{}/master.m3u8", temp_dir);
    fs::write(&master_path, &hls_data.master_m3u8_data)?;
    
    // Upload master playlist via QStash/Storj duplicate mechanism
    let master_args = storj_interface::duplicate::Args {
        publisher_user_id: video_info.publisher_user_id.clone(),
        video_id: format!("{}/master.m3u8", base_path),
        is_nsfw,
        metadata: []
        .into(),
    };
    
    qstash_client.duplicate_to_storj(master_args).await?;

    // Process each resolution
    for resolution in hls_data.resolutions {
        // Write and upload playlist
        let playlist_path = format!("{}/{}", temp_dir, resolution.playlist_name);
        fs::write(&playlist_path, &resolution.playlist_data)?;
        
        let playlist_args = storj_interface::duplicate::Args {
            publisher_user_id: video_info.publisher_user_id.clone(),
            video_id: format!("{}/{}", base_path, resolution.playlist_name),
            is_nsfw: false,
            metadata: [
                ("post_id".into(), video_info.post_id.to_string()),
                ("timestamp".into(), video_info.timestamp.clone()),
                ("file_type".into(), "hls_playlist".into()),
                ("original_video_id".into(), video_id.to_string()),
            ]
            .into(),
        };
        
        qstash_client.duplicate_to_storj(playlist_args).await?;

        // Process segments
        for segment in resolution.segments {
            let segment_path = format!("{}/{}", temp_dir, segment.segment_name);
            fs::write(&segment_path, &segment.segment_data)?;
            
            let segment_args = storj_interface::duplicate::Args {
                publisher_user_id: video_info.publisher_user_id.clone(),
                video_id: format!("{}/{}", base_path, segment.segment_name),
                is_nsfw: false,
                metadata: [
                    ("post_id".into(), video_info.post_id.to_string()),
                    ("timestamp".into(), video_info.timestamp.clone()),
                    ("file_type".into(), "hls_segment".into()),
                    ("original_video_id".into(), video_id.to_string()),
                ]
                .into(),
            };
            
            qstash_client.duplicate_to_storj(segment_args).await?;
        }
    }

    fs::remove_dir_all(&temp_dir).ok();

    Ok(())
}

#[instrument(skip(state))]
pub async fn process_hls(
    State(state): State<Arc<AppState>>,
    Json(request): Json<HlsProcessingRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let video_url = format!(
        "https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{}/downloads/default.mp4",
        request.video_id
    );

    log::info!("Starting HLS processing for video: {}", request.video_id);

    // Get video metadata
    let metadata = get_video_metadata(&video_url).await?;
    log::info!("Video metadata: {:?}", metadata);

    // Calculate scaled dimensions with max 1080p on the longer side
    let original_width = metadata.width as f64;
    let original_height = metadata.height as f64;
    let aspect_ratio = original_width / original_height;
    
    // Determine max resolution while maintaining aspect ratio
    let (max_width, max_height) = if metadata.height > metadata.width {
        // Portrait: limit height to 1080
        let height = original_height.min(1080.0);
        let width = height * aspect_ratio;
        (width as i32, height as i32)
    } else {
        // Landscape: limit width to 1080  
        let width = original_width.min(1080.0);
        let height = width / aspect_ratio;
        (width as i32, height as i32)
    };

    // Download video
    log::info!("Downloading video...");
    let video_bytes = download_video(&video_url).await?;

    // Process video with HLS
    log::info!("Processing video with HLS...");
    
    // Helper function to calculate resolution
    let calc_resolution = |target_size: i32| -> (i32, i32) {
        if metadata.height > metadata.width {
            // Portrait: target_size is height
            let width = (target_size as f64 * aspect_ratio) as i32;
            (width, target_size)
        } else {
            // Landscape: target_size is width
            let height = (target_size as f64 / aspect_ratio) as i32;
            (target_size, height)
        }
    };
    
    let profiles = vec![
        HlsVideoProcessingSettings {
            resolution: (max_width, max_height),
            constant_rate_factor: 23,
            preset: FfmpegVideoProcessingPreset::Medium,
            audio_bitrate: HlsVideoAudioBitrate::Medium,  // 256k for 1080p
            audio_codec: HlsVideoAudioCodec::Aac,
        },
        HlsVideoProcessingSettings {
            resolution: calc_resolution(720),
            constant_rate_factor: 25,
            preset: FfmpegVideoProcessingPreset::Fast,
            audio_bitrate: HlsVideoAudioBitrate::Low,     // 128k for 720p
            audio_codec: HlsVideoAudioCodec::Aac,
        },
        HlsVideoProcessingSettings {
            resolution: calc_resolution(480),
            constant_rate_factor: 28,
            preset: FfmpegVideoProcessingPreset::Fast,
            audio_bitrate: HlsVideoAudioBitrate::Low,     // 128k for 480p
            audio_codec: HlsVideoAudioCodec::Aac,
        },
    ];

    let hls_result = process_video(video_bytes.clone(), profiles).await
        .map_err(|e| anyhow!("HLS processing failed: {}", e))?;

    // Upload HLS to Storj
    log::info!("Uploading HLS files to Storj...");
    upload_hls_to_storj(&request.video_id, hls_result, &request.video_info, &state.qstash_client, request.is_nsfw).await?;

    // Upload raw video to Storj
    log::info!("Uploading raw video to Storj...");
    
    // Create temp file for raw video
    let temp_path = format!("/tmp/{}_raw.mp4", request.video_id);
    fs::write(&temp_path, &video_bytes)?;
    
    // Upload raw video via QStash/Storj duplicate mechanism
    let video_args = storj_interface::duplicate::Args {
        publisher_user_id: request.video_info.publisher_user_id.clone(),
        video_id: format!("videos/{}.mp4", request.video_id),
        is_nsfw: request.is_nsfw,
        metadata: [
            ("post_id".into(), request.video_info.post_id.to_string()),
            ("timestamp".into(), request.video_info.timestamp.clone()),
            ("file_type".into(), "video_original".into()),
            ("resolution".into(), format!("{}x{}", metadata.width, metadata.height)),
        ]
        .into(),
    };
    
    state.qstash_client.duplicate_to_storj(video_args).await?;
    
    // Clean up temp file
    fs::remove_file(&temp_path).ok();
    

    log::info!("HLS processing completed for video: {}", request.video_id);

    // Return the response with all URLs
    let response = HlsProcessingResponse {
        original_resolution: (metadata.width, metadata.height),
        processed_resolution: (max_width as u32, max_height as u32),
    };


    log::info!("HLS processing complete for video: {}", request.video_id);

    Ok(Json(serde_json::to_value(&response)?))
}