use anyhow::{Context, Result};
use image::DynamicImage;
use image_hasher::{HasherConfig, ImageHash};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Perceptual hash implementation that extracts frames and concatenates their phashes
#[derive(Debug, Clone)]
pub struct PHasher {
    num_frames: usize,
    hash_size: u32,
}

impl PHasher {
    pub fn new() -> Self {
        Self {
            num_frames: 10,
            hash_size: 8,
        }
    }

    #[allow(dead_code)]
    pub fn with_params(num_frames: usize, hash_size: u32) -> Self {
        Self {
            num_frames,
            hash_size,
        }
    }

    /// Compute phash for a video file - returns binary string (e.g., "0101101...")
    pub fn compute_hash(&self, video_path: &Path) -> Result<String> {
        let frames = self
            .extract_frames(video_path)
            .context("Failed to extract frames")?;

        if frames.is_empty() {
            anyhow::bail!("No frames extracted from video");
        }

        let hashes: Vec<String> = frames
            .iter()
            .map(|frame| {
                self.compute_image_hash(frame)
                    .map(|hash| self.hash_to_binary_string(&hash))
            })
            .collect::<Result<Vec<_>>>()?;

        // Concatenate all hashes
        Ok(hashes.join(""))
    }

    /// Convert ImageHash to binary string
    fn hash_to_binary_string(&self, hash: &ImageHash) -> String {
        let bytes = hash.as_bytes();
        let mut binary = String::with_capacity(bytes.len() * 8);

        for byte in bytes {
            for i in (0..8).rev() {
                binary.push(if (byte >> i) & 1 == 1 { '1' } else { '0' });
            }
        }

        binary
    }

    /// Extract frames at equal intervals from video
    fn extract_frames(&self, video_path: &Path) -> Result<Vec<DynamicImage>> {
        let mut ictx =
            ffmpeg_next::format::input(video_path).context("Failed to open video file")?;

        let stream = ictx
            .streams()
            .best(ffmpeg_next::media::Type::Video)
            .context("No video stream found")?;
        let stream_index = stream.index();

        let context_decoder =
            ffmpeg_next::codec::context::Context::from_parameters(stream.parameters())
                .context("Failed to create codec context")?;
        let mut decoder = context_decoder
            .decoder()
            .video()
            .context("Failed to create video decoder")?;

        let total_frames = stream.frames() as usize;

        // Calculate frame indices to extract
        let frame_interval = if total_frames > 1 {
            (total_frames - 1) as f64 / (self.num_frames - 1) as f64
        } else {
            0.0
        };

        let target_indices: Vec<usize> = (0..self.num_frames)
            .map(|i| (i as f64 * frame_interval).round() as usize)
            .collect();

        let mut frames = Vec::new();
        let mut frame_count = 0;
        let mut decoded_frame = ffmpeg_next::util::frame::video::Video::empty();

        for (stream, packet) in ictx.packets() {
            if stream.index() == stream_index {
                decoder
                    .send_packet(&packet)
                    .context("Failed to send packet")?;

                while decoder.receive_frame(&mut decoded_frame).is_ok() {
                    if target_indices.contains(&frame_count) {
                        let rgb_frame = self.convert_to_rgb(&decoded_frame)?;
                        frames.push(rgb_frame);

                        if frames.len() >= self.num_frames {
                            return Ok(frames);
                        }
                    }
                    frame_count += 1;
                }
            }
        }

        // Flush decoder
        decoder.send_eof().context("Failed to send EOF")?;
        while decoder.receive_frame(&mut decoded_frame).is_ok() {
            if target_indices.contains(&frame_count) {
                let rgb_frame = self.convert_to_rgb(&decoded_frame)?;
                frames.push(rgb_frame);

                if frames.len() >= self.num_frames {
                    break;
                }
            }
            frame_count += 1;
        }

        Ok(frames)
    }

    /// Convert ffmpeg frame to RGB DynamicImage
    fn convert_to_rgb(
        &self,
        frame: &ffmpeg_next::util::frame::video::Video,
    ) -> Result<DynamicImage> {
        let width = frame.width();
        let height = frame.height();

        let mut scaler = ffmpeg_next::software::scaling::context::Context::get(
            frame.format(),
            width,
            height,
            ffmpeg_next::format::Pixel::RGB24,
            width,
            height,
            ffmpeg_next::software::scaling::flag::Flags::BILINEAR,
        )
        .context("Failed to create scaler")?;

        let mut rgb_frame = ffmpeg_next::util::frame::video::Video::empty();
        scaler
            .run(frame, &mut rgb_frame)
            .context("Failed to scale frame")?;

        let data = rgb_frame.data(0);
        let stride = rgb_frame.stride(0);

        // Copy frame data accounting for stride
        let mut img_data = Vec::with_capacity((width * height * 3) as usize);
        for y in 0..height {
            let row_start = (y as usize) * stride;
            let row_end = row_start + (width as usize * 3);
            img_data.extend_from_slice(&data[row_start..row_end]);
        }

        let img = image::RgbImage::from_raw(width, height, img_data)
            .context("Failed to create image from raw data")?;

        Ok(DynamicImage::ImageRgb8(img))
    }

    /// Compute perceptual hash for an image
    fn compute_image_hash(&self, img: &DynamicImage) -> Result<ImageHash> {
        let hasher = HasherConfig::new()
            .hash_size(self.hash_size, self.hash_size)
            .to_hasher();

        // Convert to Luma8 for hashing
        let gray = img.to_luma8();
        Ok(hasher.hash_image(&gray))
    }
}

impl Default for PHasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Video metadata extracted during hash computation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoMetadata {
    pub video_id: String,
    pub duration: f64,
    pub width: u32,
    pub height: u32,
    pub fps: f64,
}

/// Extract video metadata using ffmpeg
pub fn extract_metadata(video_path: &Path, video_id: String) -> Result<VideoMetadata> {
    let ictx = ffmpeg_next::format::input(video_path).context("Failed to open video file")?;

    let stream = ictx
        .streams()
        .best(ffmpeg_next::media::Type::Video)
        .context("No video stream found")?;

    let context_decoder =
        ffmpeg_next::codec::context::Context::from_parameters(stream.parameters())
            .context("Failed to create codec context")?;
    let decoder = context_decoder
        .decoder()
        .video()
        .context("Failed to create video decoder")?;

    let duration = stream.duration() as f64 * f64::from(stream.time_base());
    let fps = f64::from(stream.avg_frame_rate());

    Ok(VideoMetadata {
        video_id,
        duration,
        width: decoder.width(),
        height: decoder.height(),
        fps,
    })
}

/// Download video from Cloudflare stream URL
pub async fn download_video_from_cloudflare(video_id: &str, output_path: &Path) -> Result<()> {
    let url = format!(
        "https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{}/downloads/default.mp4",
        video_id
    );

    log::info!("Downloading video from Cloudflare: {}", url);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .context("Failed to create HTTP client")?;

    let response = client
        .get(&url)
        .send()
        .await
        .context("Failed to send request")?;

    if !response.status().is_success() {
        anyhow::bail!("Failed to download video: HTTP {}", response.status());
    }

    let bytes = response
        .bytes()
        .await
        .context("Failed to read response bytes")?;

    tokio::fs::write(output_path, &bytes)
        .await
        .context("Failed to write video file")?;

    log::info!("Video downloaded successfully to: {:?}", output_path);

    Ok(())
}

/// Download video from any URL to local path
pub async fn download_video_from_url(url: &str, output_path: &Path) -> Result<()> {
    log::info!("Downloading video from URL: {}", url);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .context("Failed to create HTTP client")?;

    let response = client
        .get(url)
        .send()
        .await
        .context("Failed to send request")?;

    if !response.status().is_success() {
        anyhow::bail!("Failed to download video: HTTP {}", response.status());
    }

    let bytes = response
        .bytes()
        .await
        .context("Failed to read response bytes")?;

    tokio::fs::write(output_path, &bytes)
        .await
        .context("Failed to write video file")?;

    log::info!("Video downloaded successfully to: {:?}", output_path);

    Ok(())
}

/// Compute phash for a video by downloading from any URL
/// Downloads video, computes hash and extracts metadata, cleans up temp files automatically
/// Returns tuple of (phash, metadata)
pub async fn compute_phash_from_url(url: &str) -> Result<(String, VideoMetadata)> {
    log::info!("Computing phash for video from URL: {}", url);

    // Create temp directory
    let temp_dir = std::env::temp_dir().join(format!("dedup_{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&temp_dir)
        .await
        .context("Failed to create temp directory")?;

    // Generate unique filename using UUID
    let video_path = temp_dir.join(format!("{}.mp4", uuid::Uuid::new_v4()));

    // Download from URL
    let download_result = download_video_from_url(url, &video_path).await;
    if let Err(e) = download_result {
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        return Err(e);
    }

    // Compute phash and extract metadata in blocking task
    let video_path_clone = video_path.clone();
    let url_clone = url.to_string();
    let result = tokio::task::spawn_blocking(move || {
        let phash = PHasher::new().compute_hash(&video_path_clone)?;
        let metadata = extract_metadata(&video_path_clone, url_clone)?;
        Ok::<_, anyhow::Error>((phash, metadata))
    })
    .await;

    // Cleanup temp directory
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;

    // Handle result
    let (phash, metadata) = result
        .context("Task join error")?
        .context("Failed to compute phash and metadata")?;

    log::info!(
        "Successfully computed phash for video from URL: {} chars",
        phash.len()
    );
    Ok((phash, metadata))
}

/// Compute phash for a video by downloading from Cloudflare
/// Downloads video, computes hash and extracts metadata, cleans up temp files automatically
/// Returns tuple of (phash, metadata)
pub async fn compute_phash_from_cloudflare(video_id: &str) -> Result<(String, VideoMetadata)> {
    log::info!("Computing phash for video ID from Cloudflare: {}", video_id);

    // Create temp directory
    let temp_dir = std::env::temp_dir().join(format!("dedup_{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&temp_dir)
        .await
        .context("Failed to create temp directory")?;

    let video_path = temp_dir.join(format!("{}.mp4", video_id));

    // Download from Cloudflare
    let download_result = download_video_from_cloudflare(video_id, &video_path).await;
    if let Err(e) = download_result {
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        return Err(e);
    }

    // Compute phash and extract metadata in blocking task
    let video_path_clone = video_path.clone();
    let video_id_clone = video_id.to_string();
    let result = tokio::task::spawn_blocking(move || {
        let phash = PHasher::new().compute_hash(&video_path_clone)?;
        let metadata = extract_metadata(&video_path_clone, video_id_clone)?;
        Ok::<_, anyhow::Error>((phash, metadata))
    })
    .await;

    // Cleanup temp directory
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;

    // Handle result
    let (phash, metadata) = result
        .context("Task join error")?
        .context("Failed to compute phash and metadata")?;

    log::info!(
        "Successfully computed phash for video {}: {} chars",
        video_id,
        phash.len()
    );
    Ok((phash, metadata))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_phash_creation() {
        let hasher = PHasher::new();
        assert_eq!(hasher.num_frames, 10);
        assert_eq!(hasher.hash_size, 8);
    }

    #[test]
    fn test_phash_with_params() {
        let hasher = PHasher::with_params(5, 16);
        assert_eq!(hasher.num_frames, 5);
        assert_eq!(hasher.hash_size, 16);
    }
}
