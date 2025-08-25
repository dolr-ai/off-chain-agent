use anyhow::{Context, Result};
use candid::Principal;
use cloud_storage::Client;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use tempfile::TempDir;
use tracing::{error, info};
use videogen_common::VideoGenError;

/// Downloads a file from a URL to a local path
async fn download_file(url: &str, dest_path: &Path) -> Result<()> {
    info!("Downloading file from: {}", url);
    
    let response = reqwest::get(url)
        .await
        .context("Failed to fetch file from URL")?;
    
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Failed to download file: HTTP {}",
            response.status()
        ));
    }
    
    let bytes = response
        .bytes()
        .await
        .context("Failed to read response bytes")?;
    
    tokio::fs::write(dest_path, bytes)
        .await
        .context("Failed to write file to disk")?;
    
    info!("Downloaded file to: {:?}", dest_path);
    Ok(())
}

/// Stitches video and audio together using FFmpeg and uploads to GCS
/// 
/// # Arguments
/// * `gcs_client` - Google Cloud Storage client
/// * `video_url` - URL of the video file to download
/// * `audio_url` - URL of the audio file to download
/// * `user_principal` - User principal for organizing files in GCS
/// * `output_filename` - Optional custom filename for the output
/// 
/// # Returns
/// The public GCS URL of the uploaded stitched video
pub async fn stitch_video_audio_and_upload(
    gcs_client: Arc<Client>,
    video_url: &str,
    audio_url: &str,
    user_principal: Principal,
    output_filename: Option<String>,
) -> Result<String, VideoGenError> {
    // Create temporary directory for processing
    let temp_dir = TempDir::new().map_err(|e| {
        error!("Failed to create temp directory: {}", e);
        VideoGenError::NetworkError(format!("Failed to create temp directory: {}", e))
    })?;
    
    let video_path = temp_dir.path().join("input_video.mp4");
    let audio_path = temp_dir.path().join("input_audio.mp3");
    let output_path = temp_dir.path().join("output.mp4");
    
    // Download video and audio files
    info!("Downloading video from: {}", video_url);
    download_file(video_url, &video_path).await.map_err(|e| {
        error!("Failed to download video: {}", e);
        VideoGenError::NetworkError(format!("Failed to download video: {}", e))
    })?;
    
    info!("Downloading audio from: {}", audio_url);
    download_file(audio_url, &audio_path).await.map_err(|e| {
        error!("Failed to download audio: {}", e);
        VideoGenError::NetworkError(format!("Failed to download audio: {}", e))
    })?;
    
    // Run FFmpeg to stitch video and audio
    info!("Running FFmpeg to stitch video and audio");
    let ffmpeg_output = Command::new("ffmpeg")
        .args([
            "-i", video_path.to_str().unwrap(),
            "-i", audio_path.to_str().unwrap(),
            "-c:v", "copy",           // Copy video codec (no re-encoding)
            "-c:a", "aac",            // Encode audio as AAC
            "-strict", "experimental", // Allow experimental features
            "-shortest",               // Stop when shortest input ends
            "-y",                      // Overwrite output file
            output_path.to_str().unwrap(),
        ])
        .output()
        .map_err(|e| {
            error!("Failed to execute FFmpeg: {}", e);
            VideoGenError::NetworkError(format!("Failed to execute FFmpeg: {}", e))
        })?;
    
    if !ffmpeg_output.status.success() {
        let stderr = String::from_utf8_lossy(&ffmpeg_output.stderr);
        error!("FFmpeg failed: {}", stderr);
        return Err(VideoGenError::NetworkError(format!(
            "FFmpeg processing failed: {}",
            stderr
        )));
    }
    
    info!("FFmpeg processing completed successfully");
    
    // Read the output file
    let video_bytes = tokio::fs::read(&output_path).await.map_err(|e| {
        error!("Failed to read output video: {}", e);
        VideoGenError::NetworkError(format!("Failed to read output video: {}", e))
    })?;
    
    // Generate GCS object name
    let timestamp = chrono::Utc::now().timestamp_millis();
    let filename = output_filename.unwrap_or_else(|| format!("stitched_{}.mp4", timestamp));
    let object_name = format!("stitch-output/{}/{}", user_principal, filename);
    
    // Use the yral_ai_generated_videos bucket
    let bucket = "yral_ai_generated_videos".to_string();
    
    info!("Uploading to GCS: {}/{}", bucket, object_name);
    
    // Upload to GCS
    gcs_client
        .object()
        .create(&bucket, video_bytes, &object_name, "video/mp4")
        .await
        .map_err(|e| {
            error!("Failed to upload to GCS: {}", e);
            VideoGenError::NetworkError(format!("Failed to upload to GCS: {}", e))
        })?;
    
    // Generate public URL
    let public_url = format!("https://storage.googleapis.com/{}/{}", bucket, object_name);
    
    info!("Video uploaded successfully: {}", public_url);
    
    // Cleanup is automatic when temp_dir goes out of scope
    
    Ok(public_url)
}

/// Stitches video and audio from local file paths and uploads to GCS
/// 
/// This is a variant that works with local files instead of URLs
pub async fn stitch_local_video_audio_and_upload(
    gcs_client: Arc<Client>,
    video_path: &Path,
    audio_path: &Path,
    user_principal: Principal,
    output_filename: Option<String>,
) -> Result<String, VideoGenError> {
    // Create temporary directory for processing
    let temp_dir = TempDir::new().map_err(|e| {
        error!("Failed to create temp directory: {}", e);
        VideoGenError::NetworkError(format!("Failed to create temp directory: {}", e))
    })?;
    
    let output_path = temp_dir.path().join("output.mp4");
    
    // Run FFmpeg to stitch video and audio
    info!("Running FFmpeg to stitch video and audio");
    let ffmpeg_output = Command::new("ffmpeg")
        .args([
            "-i", video_path.to_str().unwrap(),
            "-i", audio_path.to_str().unwrap(),
            "-c:v", "copy",           // Copy video codec (no re-encoding)
            "-c:a", "aac",            // Encode audio as AAC
            "-strict", "experimental", // Allow experimental features
            "-shortest",               // Stop when shortest input ends
            "-y",                      // Overwrite output file
            output_path.to_str().unwrap(),
        ])
        .output()
        .map_err(|e| {
            error!("Failed to execute FFmpeg: {}", e);
            VideoGenError::NetworkError(format!("Failed to execute FFmpeg: {}", e))
        })?;
    
    if !ffmpeg_output.status.success() {
        let stderr = String::from_utf8_lossy(&ffmpeg_output.stderr);
        error!("FFmpeg failed: {}", stderr);
        return Err(VideoGenError::NetworkError(format!(
            "FFmpeg processing failed: {}",
            stderr
        )));
    }
    
    info!("FFmpeg processing completed successfully");
    
    // Read the output file
    let video_bytes = tokio::fs::read(&output_path).await.map_err(|e| {
        error!("Failed to read output video: {}", e);
        VideoGenError::NetworkError(format!("Failed to read output video: {}", e))
    })?;
    
    // Generate GCS object name
    let timestamp = chrono::Utc::now().timestamp_millis();
    let filename = output_filename.unwrap_or_else(|| format!("stitched_{}.mp4", timestamp));
    let object_name = format!("stitch-output/{}/{}", user_principal, filename);
    
    // Use the yral_ai_generated_videos bucket
    let bucket = "yral_ai_generated_videos".to_string();
    
    info!("Uploading to GCS: {}/{}", bucket, object_name);
    
    // Upload to GCS
    gcs_client
        .object()
        .create(&bucket, video_bytes, &object_name, "video/mp4")
        .await
        .map_err(|e| {
            error!("Failed to upload to GCS: {}", e);
            VideoGenError::NetworkError(format!("Failed to upload to GCS: {}", e))
        })?;
    
    // Generate public URL
    let public_url = format!("https://storage.googleapis.com/{}/{}", bucket, object_name);
    
    info!("Video uploaded successfully: {}", public_url);
    
    Ok(public_url)
}