use anyhow::{Context, Result};
use hmac::{Hmac, Mac};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct NsfwApiClient {
    client: Client,
    base_url: Url,
    secret: String,
}

#[derive(Debug, Error)]
pub enum NsfwApiError {
    #[error("retryable NSFW API error: {0}")]
    Retryable(String),
    #[error("terminal NSFW API error: {0}")]
    Terminal(String),
}

impl NsfwApiError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct VideoDetectRequest {
    pub job_id: String,
    pub video_id: String,
    pub publisher_user_id: String,
    pub source_video_uri: String,
    pub post_id: Option<String>,
    pub canister_id: Option<String>,
    pub source_object_version: String,
    pub upload_event_id: Option<String>,
    pub upload_created_at: Option<String>,
    pub policy_version: String,
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VideoDetectResponse {
    pub job_id: String,
    pub video_id: String,
    pub status: String,
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VideoStatusResponse {
    pub job_id: String,
    pub video_id: String,
    pub status: String,
    pub trace_id: Option<String>,
    pub attempts: u32,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub final_result: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VideoBanRequest {
    pub publisher_user_id: String,
    pub post_id: String,
    pub canister_id: String,
    pub reason: String,
    pub source: String,
    pub moderator_id: Option<String>,
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VideoBanResponse {
    pub video_id: String,
    pub status: String,
    pub excluded_videos_written: bool,
    pub legacy_nsfw_agg_written: bool,
    pub trace_id: Option<String>,
}

impl NsfwApiClient {
    pub fn from_env() -> Result<Self> {
        let base_url = std::env::var("NSFW_API_BASE_URL")
            .unwrap_or_else(|_| "https://nsfw.ansuman.yral.com".to_string());
        let base_url = if base_url.ends_with('/') {
            base_url
        } else {
            format!("{base_url}/")
        };

        let secret = std::env::var("NSFW_INTERNAL_REQUEST_HMAC_SECRET")
            .context("NSFW_INTERNAL_REQUEST_HMAC_SECRET env var is required")?;

        Ok(Self {
            client: Client::new(),
            base_url: Url::parse(&base_url).context("invalid NSFW_API_BASE_URL")?,
            secret,
        })
    }

    pub async fn detect_video(
        &self,
        request: &VideoDetectRequest,
    ) -> std::result::Result<VideoDetectResponse, NsfwApiError> {
        let path = "/v1/videos/detect";
        // The HMAC covers the exact request bytes, so serialize once and send the same bytes we sign.
        let body = serde_json::to_vec(request)
            .map_err(|e| NsfwApiError::Terminal(format!("failed to serialize request: {e}")))?;
        let url = self
            .base_url
            .join(path.trim_start_matches('/'))
            .map_err(|e| NsfwApiError::Terminal(format!("invalid detect URL: {e}")))?;

        let response = self
            .client
            .post(url)
            .header("content-type", "application/json")
            .headers(self.signed_headers("POST", path, &body)?)
            .body(body)
            .send()
            .await
            .map_err(|e| NsfwApiError::Retryable(format!("detect request failed: {e}")))?;

        parse_response(response, "detect").await
    }

    pub async fn video_status(
        &self,
        video_id: &str,
    ) -> std::result::Result<VideoStatusResponse, NsfwApiError> {
        let encoded_video_id = urlencoding::encode(video_id);
        let path = format!("/v1/videos/{encoded_video_id}/status");
        let url = self
            .base_url
            .join(path.trim_start_matches('/'))
            .map_err(|e| NsfwApiError::Terminal(format!("invalid status URL: {e}")))?;

        let response = self
            .client
            .get(url)
            .headers(self.signed_headers("GET", &path, b"")?)
            .send()
            .await
            .map_err(|e| NsfwApiError::Retryable(format!("status request failed: {e}")))?;

        parse_response(response, "status").await
    }

    pub async fn ban_video(
        &self,
        video_id: &str,
        request: &VideoBanRequest,
    ) -> std::result::Result<VideoBanResponse, NsfwApiError> {
        let encoded_video_id = urlencoding::encode(video_id);
        let path = format!("/v1/videos/{encoded_video_id}/ban");
        // The manual-ban endpoint uses the same exact-byte signing contract as detection.
        let body = serde_json::to_vec(request)
            .map_err(|e| NsfwApiError::Terminal(format!("failed to serialize ban request: {e}")))?;
        let url = self
            .base_url
            .join(path.trim_start_matches('/'))
            .map_err(|e| NsfwApiError::Terminal(format!("invalid ban URL: {e}")))?;

        let response = self
            .client
            .post(url)
            .header("content-type", "application/json")
            .headers(self.signed_headers("POST", &path, &body)?)
            .body(body)
            .send()
            .await
            .map_err(|e| NsfwApiError::Retryable(format!("ban request failed: {e}")))?;

        parse_response(response, "ban").await
    }

    fn signed_headers(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> std::result::Result<reqwest::header::HeaderMap, NsfwApiError> {
        let timestamp = chrono::Utc::now().timestamp().to_string();
        let signature = sign_request(&self.secret, &timestamp, method, path, body)?;

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "x-internal-timestamp",
            timestamp
                .parse()
                .map_err(|e| NsfwApiError::Terminal(format!("invalid timestamp header: {e}")))?,
        );
        headers.insert(
            "x-internal-signature",
            signature
                .parse()
                .map_err(|e| NsfwApiError::Terminal(format!("invalid signature header: {e}")))?,
        );
        Ok(headers)
    }
}

async fn parse_response<T: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
    action: &str,
) -> std::result::Result<T, NsfwApiError> {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();

    if status.is_success() {
        return serde_json::from_str(&body).map_err(|e| {
            // The enqueue probably reached the server; retry polling/enqueue rather than marking the video terminal.
            NsfwApiError::Retryable(format!(
                "{action} response could not be decoded as JSON: {e}; body={body}"
            ))
        });
    }

    let message = format!("{action} returned HTTP {status}: {body}");
    Err(if is_retryable_status(status) {
        NsfwApiError::Retryable(message)
    } else {
        NsfwApiError::Terminal(message)
    })
}

fn is_retryable_status(status: StatusCode) -> bool {
    status.is_server_error()
        || matches!(
            status,
            StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
        )
}

fn sign_request(
    secret: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    body: &[u8],
) -> std::result::Result<String, NsfwApiError> {
    let body_hash = hex::encode(Sha256::digest(body));
    let message = format!(
        "{}\n{}\n{}\n{}",
        timestamp,
        method.to_uppercase(),
        path,
        body_hash
    );

    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|e| NsfwApiError::Terminal(format!("invalid HMAC secret: {e}")))?;
    mac.update(message.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_empty_body_with_expected_shape() {
        let signature =
            sign_request("secret", "123", "GET", "/v1/videos/a/status", b"").expect("signature");
        assert_eq!(
            signature,
            "3bc58c50db2f993a521f93c3e1ecd5aa8b95de594558de5265f10108c0407bc0"
        );
        assert_eq!(signature.len(), 64);
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn signs_manual_ban_path_with_expected_shape() {
        let body = br#"{"publisher_user_id":"user","post_id":"post","canister_id":"canister","reason":"user_report_approved","source":"google_chat","moderator_id":null,"trace_id":null}"#;
        let signature = sign_request("secret", "123", "POST", "/v1/videos/video-1/ban", body)
            .expect("signature");

        assert_eq!(
            signature,
            "c68650ea212ce5ee1b502d7c3065179c8e28b0d1371b88cb631e581e51d46cd5"
        );
        assert_eq!(signature.len(), 64);
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
