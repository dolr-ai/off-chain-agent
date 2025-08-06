use candid::Principal;
use serde::{Deserialize, Serialize};
use videogen_common::{VideoGenInput, VideoGenResponse};

/// Request structure for queueing video generation to Qstash
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QstashVideoGenRequest {
    /// User principal making the request
    pub user_principal: Principal,
    /// The video generation input
    pub input: VideoGenInput,
    /// Rate limit canister request key for status updates
    pub request_key: VideoGenRequestKey,
    /// Property name for rate limiting (e.g., "VIDEOGEN")
    pub property: String,
}

/// Key structure matching rate limit canister
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoGenRequestKey {
    pub principal: Principal,
    pub counter: u64,
}

/// Callback request structure for Qstash completion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QstashVideoGenCallback {
    /// The original request key
    pub request_key: VideoGenRequestKey,
    /// Result of the video generation
    pub result: VideoGenCallbackResult,
    /// Property name for rate limiting (needed for decrement on failure)
    pub property: String,
}

/// Result types for callback
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VideoGenCallbackResult {
    Success(VideoGenResponse),
    Failure(String),
}
