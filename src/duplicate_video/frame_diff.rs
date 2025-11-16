use anyhow::{Context, Result};
use cloud_storage::Client;
use image::{DynamicImage, ImageBuffer, RgbaImage};
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use tracing::log;

use super::phash::download_video_from_cloudflare;

const GCS_BUCKET: &str = "yral-dedup-analysis";

/// Extended hasher that returns per-frame hashes
pub struct PHasherWithFrames {}

impl PHasherWithFrames {
    pub fn new() -> Self {
        Self {}
    }

    /// Compute individual frame hashes (returns vector of 10 binary strings, each 64 chars)
    pub fn compute_frame_hashes(&self, video_path: &Path) -> Result<Vec<String>> {
        let frames = self.extract_frames(video_path)?;

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

        Ok(hashes)
    }

    /// Extract frames from video - reuses logic from PHasher
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
        let num_frames = 10;

        // Calculate frame indices to extract
        let frame_interval = if total_frames > 1 {
            (total_frames - 1) as f64 / (num_frames - 1) as f64
        } else {
            0.0
        };

        let target_indices: Vec<usize> = (0..num_frames)
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

                        if frames.len() >= num_frames {
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

                if frames.len() >= num_frames {
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
            ffmpeg_next::software::scaling::Flags::BILINEAR,
        )
        .context("Failed to create scaler")?;

        let mut rgb_frame = ffmpeg_next::util::frame::video::Video::empty();
        scaler
            .run(frame, &mut rgb_frame)
            .context("Failed to scale frame")?;

        let stride = rgb_frame.stride(0);
        let rgb_data = rgb_frame.data(0);

        // Handle stride/padding: copy only the actual image data without padding
        let mut image_data = Vec::with_capacity((width * height * 3) as usize);
        for y in 0..height {
            let row_start = (y as usize) * stride;
            let row_end = row_start + (width as usize * 3);
            image_data.extend_from_slice(&rgb_data[row_start..row_end]);
        }

        let rgb_image = image::RgbImage::from_raw(width, height, image_data)
            .context("Failed to create RGB image")?;

        Ok(DynamicImage::ImageRgb8(rgb_image))
    }

    /// Compute hash for a single image
    fn compute_image_hash(&self, img: &DynamicImage) -> Result<image_hasher::ImageHash> {
        let hasher = image_hasher::HasherConfig::new()
            .hash_size(8, 8)
            .to_hasher();

        // Convert to Luma8 for hashing (matches PHasher logic)
        let gray = img.to_luma8();
        Ok(hasher.hash_image(&gray))
    }

    /// Convert ImageHash to binary string
    fn hash_to_binary_string(&self, hash: &image_hasher::ImageHash) -> String {
        let bytes = hash.as_bytes();
        let mut binary = String::with_capacity(bytes.len() * 8);

        for byte in bytes {
            for i in (0..8).rev() {
                binary.push(if (byte >> i) & 1 == 1 { '1' } else { '0' });
            }
        }

        binary
    }
}

/// Join two frames horizontally for visual comparison (currently unused, kept for future use)
#[allow(dead_code)]
pub fn join_frames_horizontally(frame1: &DynamicImage, frame2: &DynamicImage) -> DynamicImage {
    let (w1, h1) = (frame1.width(), frame1.height());
    let (w2, h2) = (frame2.width(), frame2.height());
    let new_width = w1 + w2;
    let new_height = h1.max(h2);

    let mut combined: RgbaImage = ImageBuffer::new(new_width, new_height);

    // Copy frame1 to left side
    let frame1_rgba = frame1.to_rgba8();
    for y in 0..h1 {
        for x in 0..w1 {
            let pixel = frame1_rgba.get_pixel(x, y);
            combined.put_pixel(x, y, *pixel);
        }
    }

    // Copy frame2 to right side
    let frame2_rgba = frame2.to_rgba8();
    for y in 0..h2 {
        for x in 0..w2 {
            let pixel = frame2_rgba.get_pixel(x, y);
            combined.put_pixel(w1 + x, y, *pixel);
        }
    }

    DynamicImage::ImageRgba8(combined)
}

/// Calculate hamming distance between two binary strings
fn hamming_distance(hash1: &str, hash2: &str) -> u32 {
    hash1
        .chars()
        .zip(hash2.chars())
        .filter(|(c1, c2)| c1 != c2)
        .count() as u32
}

/// Compare two videos and return indices where frames differ along with hamming distances and concatenated phashes
pub async fn compare_videos(
    publisher_user_id_1: &str,
    video_id_1: &str,
    publisher_user_id_2: &str,
    video_id_2: &str,
) -> Result<(
    Vec<(usize, u32)>,
    Vec<DynamicImage>,
    Vec<DynamicImage>,
    String,
    String,
)> {
    log::info!("Comparing videos: {} vs {}", video_id_1, video_id_2);

    // Download both videos
    let temp_dir = std::env::temp_dir().join(format!("frame_diff_{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&temp_dir).await?;

    let video_path_1 = temp_dir.join(format!("{}.mp4", video_id_1));
    let video_path_2 = temp_dir.join(format!("{}.mp4", video_id_2));

    // Download videos concurrently
    tokio::try_join!(
        download_video_from_cloudflare(publisher_user_id_1, video_id_1, &video_path_1),
        download_video_from_cloudflare(publisher_user_id_2, video_id_2, &video_path_2)
    )?;

    // Compute frame hashes and extract frames concurrently
    let video_path_1_clone = video_path_1.clone();
    let video_path_2_clone = video_path_2.clone();

    let (result1, result2) = tokio::join!(
        tokio::task::spawn_blocking(move || {
            let hasher = PHasherWithFrames::new();
            let hashes = hasher.compute_frame_hashes(&video_path_1_clone)?;
            let frames = hasher.extract_frames(&video_path_1_clone)?;
            Ok::<_, anyhow::Error>((hashes, frames))
        }),
        tokio::task::spawn_blocking(move || {
            let hasher = PHasherWithFrames::new();
            let hashes = hasher.compute_frame_hashes(&video_path_2_clone)?;
            let frames = hasher.extract_frames(&video_path_2_clone)?;
            Ok::<_, anyhow::Error>((hashes, frames))
        })
    );

    let (hashes1, frames1) = result1??;
    let (hashes2, frames2) = result2??;

    // Clean up temp files
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;

    // Concatenate hashes for full video phash
    let phash1 = hashes1.join("");
    let phash2 = hashes2.join("");

    // Compare hashes and find differing indices with hamming distances
    let mut differing_frames = Vec::new();
    for (i, (hash1, hash2)) in hashes1.iter().zip(hashes2.iter()).enumerate() {
        if hash1 != hash2 {
            let distance = hamming_distance(hash1, hash2);
            differing_frames.push((i, distance));
        }
    }

    log::info!(
        "Found {} differing frames out of {}",
        differing_frames.len(),
        hashes1.len()
    );

    Ok((differing_frames, frames1, frames2, phash1, phash2))
}

/// Upload a single frame to GCS
pub async fn upload_frame_to_gcs(
    client: Arc<Client>,
    frame: &DynamicImage,
    video_id_1: &str,
    video_id_2: &str,
    frame_index: usize,
    video_num: u8, // 1 or 2
) -> Result<String> {
    // Convert image to PNG bytes
    let mut png_bytes = Vec::new();
    frame
        .write_to(&mut Cursor::new(&mut png_bytes), image::ImageFormat::Png)
        .context("Failed to encode image as PNG")?;

    let object_name = format!(
        "frame-diff/{}-vs-{}/frame-{}-video{}.png",
        video_id_1, video_id_2, frame_index, video_num
    );

    log::info!("Uploading frame to GCS: {}/{}", GCS_BUCKET, object_name);

    // Upload to GCS
    client
        .object()
        .create(GCS_BUCKET, png_bytes, &object_name, "image/png")
        .await
        .context("Failed to upload frame to GCS")?;

    let public_url = format!(
        "https://storage.googleapis.com/{}/{}",
        GCS_BUCKET, object_name
    );

    log::info!("Frame uploaded successfully: {}", public_url);

    Ok(public_url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::duplicate_video::phash::compute_phash_from_cloudflare;

    #[tokio::test]
    async fn test_phash_consistency_between_functions() {
        // Test video IDs
        let video_id_1 = "056599298729e3541ed8ae7244aa5d95";
        let video_id_2 = "10c48ceabac721cb6cab3a37e8c6dc1f";

        // Compute using compute_phash_from_cloudflare (original function)
        let (phash1_original, _) = compute_phash_from_cloudflare(video_id_1)
            .await
            .expect("Failed to compute phash for video 1");

        let (phash2_original, _) = compute_phash_from_cloudflare(video_id_2)
            .await
            .expect("Failed to compute phash for video 2");

        // Compute using compare_videos (new function)
        let (_, _, _, phash1_compare, phash2_compare) = compare_videos(video_id_1, video_id_2)
            .await
            .expect("Failed to compare videos");

        // Assert both methods produce identical hashes
        assert_eq!(
            phash1_original, phash1_compare,
            "Video 1 phash mismatch:\nOriginal: {}\nCompare:  {}",
            phash1_original, phash1_compare
        );

        assert_eq!(
            phash2_original, phash2_compare,
            "Video 2 phash mismatch:\nOriginal: {}\nCompare:  {}",
            phash2_original, phash2_compare
        );

        println!("✅ Both functions produce identical phashes");
        println!("Video 1 phash: {}", phash1_original);
        println!("Video 2 phash: {}", phash2_original);
    }
}
