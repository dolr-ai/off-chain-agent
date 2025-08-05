use std::sync::Arc;
use std::process::Command;
use std::fs;

use crate::{app_state::AppState, AppError};
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

use super::event::UploadVideoInfo;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HlsProcessingRequest {
    pub video_id: String,
    pub video_info: UploadVideoInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HlsProcessingResponse {
    pub hls_url: String,
    pub video_1080p_url: Option<String>,  // Only present if video was >1080p
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

#[instrument]
async fn reencode_video_to_1080p(
    video_bytes: &[u8],
    target_width: i32,
    target_height: i32,
) -> Result<Vec<u8>, Error> {
    // Create temp files for input and output
    let temp_dir = tempfile::tempdir()?;
    let input_path = temp_dir.path().join("input.mp4");
    let output_path = temp_dir.path().join("output_1080p.mp4");

    // Write input video
    fs::write(&input_path, video_bytes)?;

    // Re-encode to 1080p using ffmpeg
    let status = tokio::task::spawn_blocking({
        let input_path = input_path.clone();
        let output_path = output_path.clone();
        move || {
            Command::new("ffmpeg")
                .args(&[
                    "-i", input_path.to_str().unwrap(),
                    "-vf", &format!("scale={}:{}", target_width, target_height),
                    "-c:v", "libx264",
                    "-crf", "23",
                    "-preset", "medium",
                    "-c:a", "aac",
                    "-b:a", "256k",
                    "-movflags", "+faststart",
                    output_path.to_str().unwrap(),
                ])
                .output()
        }
    })
    .await??;

    if !status.status.success() {
        return Err(anyhow!("Failed to re-encode video to 1080p: {}", 
            String::from_utf8_lossy(&status.stderr)));
    }

    // Read the output file
    let output_bytes = fs::read(&output_path)?;
    Ok(output_bytes)
}

#[instrument(skip(qstash_client))]
async fn upload_1080p_video_to_storj(
    video_id: &str,
    video_bytes: Vec<u8>,
    video_info: &UploadVideoInfo,
    qstash_client: &crate::qstash::client::QStashClient,
) -> Result<String, Error> {
    // Create temp file for 1080p video
    let temp_path = format!("/tmp/{}_1080p.mp4", video_id);
    fs::write(&temp_path, &video_bytes)?;

    // Upload 1080p video via QStash/Storj duplicate mechanism
    let video_args = storj_interface::duplicate::Args {
        publisher_user_id: video_info.publisher_user_id.clone(),
        video_id: format!("videos/{}_1080p.mp4", video_id),
        is_nsfw: false,
        metadata: [
            ("post_id".into(), video_info.post_id.to_string()),
            ("timestamp".into(), video_info.timestamp.clone()),
            ("file_type".into(), "video_1080p".into()),
            ("original_video_id".into(), video_id.to_string()),
        ]
        .into(),
    };
    
    qstash_client.duplicate_to_storj(video_args).await?;

    // Clean up temp file
    fs::remove_file(&temp_path).ok();

    // Return the URL for the 1080p video
    Ok(format!("https://link.storjshare.io/raw/videos/{}_1080p.mp4", video_id))
}

#[instrument(skip(hls_data, qstash_client))]
async fn upload_hls_to_storj(
    video_id: &str,
    hls_data: HlsVideo,
    video_info: &UploadVideoInfo,
    qstash_client: &crate::qstash::client::QStashClient,
) -> Result<String, Error> {
    // Create temporary directory for HLS files
    let temp_dir = format!("/tmp/hls_{}", video_id);
    fs::create_dir_all(&temp_dir)?;
    
    let base_path = format!("hls/{}", video_id);

    // Write and upload master playlist
    let master_path = format!("{}/master.m3u8", temp_dir);
    fs::write(&master_path, &hls_data.master_m3u8_data)?;
    
    // Upload master playlist via QStash/Storj duplicate mechanism
    let master_args = storj_interface::duplicate::Args {
        publisher_user_id: video_info.publisher_user_id.clone(),
        video_id: format!("{}/master.m3u8", base_path),
        is_nsfw: false,
        metadata: [
            ("post_id".into(), video_info.post_id.to_string()),
            ("timestamp".into(), video_info.timestamp.clone()),
            ("file_type".into(), "hls_master".into()),
            ("original_video_id".into(), video_id.to_string()),
        ]
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

    // Clean up temporary directory
    fs::remove_dir_all(&temp_dir).ok();

    // Return the CDN URL for the master playlist
    // This URL pattern should match how Storj serves files
    Ok(format!("https://link.storjshare.io/raw/{}/master.m3u8", base_path))
}

#[instrument]
async fn store_hls_info(
    video_id: &str,
    hls_url: &str,
    original_resolution: (u32, u32),
    processed_resolution: (u32, u32),
) -> Result<(), Error> {
    // upload to canister

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

    // Determine target resolution (max 1080p)
    let target_height = metadata.height.min(1080) as i32;
    let target_width = if metadata.height > 1080 {
        (metadata.width as f64 * (1080.0 / metadata.height as f64)) as i32
    } else {
        metadata.width as i32
    };

    // Download video
    log::info!("Downloading video...");
    let video_bytes = download_video(&video_url).await?;

    // Check if we need to re-encode to 1080p
    let (video_1080p_url, video_bytes_for_hls) = if metadata.height > 1080 {
        log::info!("Video is higher than 1080p, re-encoding...");
        let video_1080p_bytes = reencode_video_to_1080p(&video_bytes, target_width, target_height).await?;
        
        // Upload 1080p video to Storj
        log::info!("Uploading 1080p video to Storj...");
        let video_1080p_url = upload_1080p_video_to_storj(
            &request.video_id,
            video_1080p_bytes.clone(),
            &request.video_info,
            &state.qstash_client
        ).await?;
        
        (Some(video_1080p_url), video_1080p_bytes)
    } else {
        log::info!("Video is 1080p or lower, using original");
        (None, video_bytes)
    };

    // Process video with HLS
    log::info!("Processing video with HLS...");
    let profiles = vec![
        HlsVideoProcessingSettings {
            resolution: (target_width, target_height),
            constant_rate_factor: 23,
            preset: FfmpegVideoProcessingPreset::Medium,
            audio_bitrate: HlsVideoAudioBitrate::Medium,  // 256k for 1080p
            audio_codec: HlsVideoAudioCodec::Aac,
        },
        HlsVideoProcessingSettings {
            resolution: (target_width * 720 / target_height, 720),
            constant_rate_factor: 25,
            preset: FfmpegVideoProcessingPreset::Fast,
            audio_bitrate: HlsVideoAudioBitrate::Low,     // 128k for 720p
            audio_codec: HlsVideoAudioCodec::Aac,
        },
        HlsVideoProcessingSettings {
            resolution: (target_width * 480 / target_height, 480),
            constant_rate_factor: 28,
            preset: FfmpegVideoProcessingPreset::Fast,
            audio_bitrate: HlsVideoAudioBitrate::Low,     // 128k for 480p
            audio_codec: HlsVideoAudioCodec::Aac,
        },
    ];

    let hls_result = process_video(video_bytes_for_hls, profiles).await
        .map_err(|e| anyhow!("HLS processing failed: {}", e))?;

    // Upload HLS to Storj
    log::info!("Uploading HLS files to Storj...");
    let hls_url = upload_hls_to_storj(&request.video_id, hls_result, &request.video_info, &state.qstash_client).await?;

    // Store HLS info
    #[cfg(not(feature = "local-bin"))]
    store_hls_info(
        &request.video_id,
        &hls_url,
        (metadata.width, metadata.height),
        (target_width as u32, target_height as u32),
    ).await?;

    log::info!("HLS processing completed for video: {}", request.video_id);

    // Return the response with all URLs
    let response = HlsProcessingResponse {
        hls_url,
        video_1080p_url,
        original_resolution: (metadata.width, metadata.height),
        processed_resolution: (target_width as u32, target_height as u32),
    };

    log::info!("HLS processing complete for video: {}", request.video_id);

    Ok(Json(serde_json::to_value(&response)?))
}