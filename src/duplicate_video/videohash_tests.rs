use super::videohash::HASH_SIZE;
use crate::duplicate_video::videohash::VideoHash;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::Command;

fn create_test_video(
    path: &str,
    duration: u32,
    color: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dir_path = Path::new(path).parent().unwrap();
    fs::create_dir_all(dir_path)?;

    let status = Command::new("ffmpeg")
        .args([
            "-f",
            "lavfi",
            "-i",
            &format!("color=c={color}:s=320x240:d={duration}"),
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-y",
            path,
        ])
        .status()?;

    if !status.success() {
        return Err("Failed to create test video".into());
    }

    Ok(())
}

#[tokio::test]
async fn test_hash_generation() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let video_path = Path::new("tests/resources/sample_video.mp4");
    if !video_path.exists() {
        println!("Test video file not found. Skipping test.");
        return Ok(());
    }

    // Use await with the now-async method
    let hash = VideoHash::new(video_path).await?;

    println!("Generated hash: {}", hash.hash);
    assert_eq!(hash.hash.len(), HASH_SIZE);
    assert!(hash.hash.chars().all(|c| c == '0' || c == '1'));

    Ok(())
}

#[tokio::test]
async fn test_identical_videos_have_identical_hashes(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let video1_path = "tests/resources/sample_video.mp4";
    let video2_path = "tests/resources/sample_video_copy.mp4";

    if !Path::new(video1_path).exists() || !Path::new(video2_path).exists() {
        println!("Test video files not found. Skipping test.");
        return Ok(());
    }
    let hash1 = VideoHash::new(Path::new(&video1_path)).await?;
    let hash2 = VideoHash::new(Path::new(&video2_path)).await?;

    println!("Hash 1: {}", hash1.hash);
    println!("Hash 2: {}", hash2.hash);

    let similarity = hash1.similarity(&hash2);
    println!("Similarity: {similarity}%");

    assert!(
        similarity > 99.0,
        "Identical videos should have a similarity > 99% (got {similarity}%)"
    );

    Ok(())
}

#[tokio::test]
async fn test_similar_videos_have_similar_hashes(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let base_video_path = "tests/resources/sample_video.mp4";
    let similar_video_path = "tests/resources/sample_video_similar.mp4";

    if !Path::new(base_video_path).exists() || !Path::new(similar_video_path).exists() {
        println!("Test video files not found. Skipping test.");
        return Ok(());
    }

    // Use await for both async calls
    let hash1 = VideoHash::new(Path::new(&base_video_path)).await?;
    let hash2 = VideoHash::new(Path::new(&similar_video_path)).await?;

    println!("Hash 1: {}", hash1.hash);
    println!("Hash 2: {}", hash2.hash);

    let similarity = hash1.similarity(&hash2);
    println!("Similarity: {similarity}%");

    assert!(
        similarity >= 85.0,
        "Similar videos should have a similarity >= 85% (got {similarity}%)"
    );

    assert!(
        similarity < 99.0,
        "Similar but not identical videos should have a similarity < 99% (got {similarity}%)"
    );

    Ok(())
}

#[tokio::test]
async fn test_hash_consistency() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let video_path = "tests/resources/sample_video.mp4";
    let path = Path::new(video_path);

    if !path.exists() {
        println!("Test video file not found. Skipping test.");
        return Ok(());
    }

    // Use await for all three async calls
    let hash1 = VideoHash::new(path).await?;
    let hash2 = VideoHash::new(path).await?;
    let hash3 = VideoHash::new(path).await?;

    println!("Hash 1: {}", hash1.hash);
    println!("Hash 2: {}", hash2.hash);
    println!("Hash 3: {}", hash3.hash);

    assert_eq!(hash1.hash, hash2.hash);
    assert_eq!(hash2.hash, hash3.hash);

    Ok(())
}

#[tokio::test]
async fn test_invalid_video_path() {
    let invalid_path = Path::new("non_existent_video.mp4");
    let result = VideoHash::new(invalid_path).await;
    assert!(result.is_err());
}

#[test]
fn test_hamming_distance_and_similarity() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let hash1 = VideoHash {
        hash: "0".repeat(64),
    };
    let hash2 = VideoHash {
        hash: "1".repeat(64),
    };
    let hash3 = VideoHash {
        hash: "0".repeat(32) + &"1".repeat(32),
    };

    assert_eq!(hash1.hamming_distance(&hash1), 0);
    assert_eq!(hash1.hamming_distance(&hash2), 64);
    assert_eq!(hash1.hamming_distance(&hash3), 32);

    assert_eq!(hash1.similarity(&hash1), 100.0);
    assert_eq!(hash1.similarity(&hash2), 0.0);
    assert_eq!(hash1.similarity(&hash3), 50.0);

    Ok(())
}

#[tokio::test]
async fn test_error_handling_invalid_video() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let test_dir = "target/test_videos";
    fs::create_dir_all(test_dir)?;
    let invalid_path = format!("{test_dir}/not_a_video.mp4");

    let mut file = fs::File::create(&invalid_path)?;
    file.write_all(b"This is not a video file")?;

    let result = VideoHash::new(Path::new(&invalid_path));
    assert!(result.await.is_err());

    fs::remove_file(&invalid_path)?;

    Ok(())
}
