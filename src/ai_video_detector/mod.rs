use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::env;

static AI_VIDEO_DETECTOR_URL: Lazy<String> = Lazy::new(|| {
    env::var("AI_VIDEO_DETECTOR_URL")
        .unwrap_or_else(|_| "https://ai-video-detector.fly.dev".to_string())
});

static AI_VIDEO_DETECTOR_API_KEY: Lazy<String> =
    Lazy::new(|| env::var("AI_VIDEO_DETECTOR_API_KEY").unwrap_or_default());

/// Verdict from the AI video detector
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Verdict {
    /// AI-generated content detected; upload proceeds
    Allow,
    /// Real footage identified with high confidence; upload rejected
    Block,
    /// Uncertain classification; queued for manual review
    Review,
}

/// Response from the AI video detector API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectionResponse {
    pub verdict: Verdict,
    pub confidence: f64,
    pub c2pa_detected: bool,
    #[serde(default)]
    pub npr_mean_score: Option<f64>,
    #[serde(default)]
    pub npr_frame_scores: Option<Vec<f64>>,
    #[serde(default)]
    pub frames_analyzed: Option<i32>,
}

/// AI Video Detector client
pub struct AiVideoDetectorClient {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
}

impl Default for AiVideoDetectorClient {
    fn default() -> Self {
        Self::new()
    }
}

impl AiVideoDetectorClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: AI_VIDEO_DETECTOR_URL.clone(),
            api_key: AI_VIDEO_DETECTOR_API_KEY.clone(),
        }
    }

    /// Detect if a video is AI-generated or real footage
    ///
    /// # Arguments
    /// * `video_url` - URL of the video to analyze (supports Cloudflare Stream and Storj links)
    ///
    /// # Returns
    /// * `DetectionResponse` with verdict (ALLOW/BLOCK/REVIEW), confidence, and other metadata
    pub async fn detect_video(&self, video_url: &str) -> Result<DetectionResponse> {
        if self.api_key.is_empty() {
            return Err(anyhow::anyhow!("AI_VIDEO_DETECTOR_API_KEY not configured"));
        }

        let url = format!("{}/detect", self.base_url);

        log::info!("Calling AI video detector for URL: {}", video_url);

        let form = reqwest::multipart::Form::new().text("url", video_url.to_string());

        let response = self
            .client
            .post(&url)
            .header("x-api-key", &self.api_key)
            .multipart(form)
            .send()
            .await
            .context("Failed to send request to AI video detector")?;

        let status = response.status();

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "AI video detector returned error: {} - {}",
                status,
                error_text
            ));
        }

        let detection_response: DetectionResponse = response
            .json()
            .await
            .context("Failed to parse AI video detector response")?;

        log::info!(
            "AI video detector result: verdict={:?}, confidence={:.2}, c2pa_detected={}",
            detection_response.verdict,
            detection_response.confidence,
            detection_response.c2pa_detected
        );

        Ok(detection_response)
    }

    /// Check if the API key is configured
    pub fn is_configured(&self) -> bool {
        !self.api_key.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verdict_deserialization() {
        let allow: Verdict = serde_json::from_str("\"ALLOW\"").unwrap();
        assert_eq!(allow, Verdict::Allow);

        let block: Verdict = serde_json::from_str("\"BLOCK\"").unwrap();
        assert_eq!(block, Verdict::Block);

        let review: Verdict = serde_json::from_str("\"REVIEW\"").unwrap();
        assert_eq!(review, Verdict::Review);
    }

    #[test]
    fn test_detection_response_deserialization() {
        let json = r#"{
            "verdict": "ALLOW",
            "confidence": 0.95,
            "c2pa_detected": true,
            "npr_mean_score": 0.8,
            "npr_frame_scores": [0.7, 0.8, 0.9],
            "frames_analyzed": 10
        }"#;

        let response: DetectionResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.verdict, Verdict::Allow);
        assert!((response.confidence - 0.95).abs() < 0.001);
        assert!(response.c2pa_detected);
    }
}
