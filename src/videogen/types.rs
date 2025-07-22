use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// Re-export videogen_common types with ToSchema implementations
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(tag = "provider", content = "data")]
pub enum VideoGenInput {
    Veo3 {
        prompt: String,
        negative_prompt: Option<String>,
        image: Option<ImageInput>,
        aspect_ratio: Veo3AspectRatio,
        duration_seconds: u8,
        generate_audio: bool,
    },
    FalAi {
        prompt: String,
        model: String,
        seed: Option<u64>,
        num_frames: Option<u32>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct ImageInput {
    pub data: Vec<u8>,
    pub mime_type: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub enum Veo3AspectRatio {
    #[serde(rename = "16:9")]
    Ratio16x9,
    #[serde(rename = "9:16")]
    Ratio9x16,
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct VideoGenResponse {
    pub operation_id: String,
    pub video_url: String,
    pub provider: String,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub enum VideoGenError {
    #[schema(example = "Provider error: API rate limit exceeded")]
    ProviderError(String),
    #[schema(example = "Invalid input: Missing required field 'prompt'")]
    InvalidInput(String),
    #[schema(example = "Authentication failed")]
    AuthError,
    #[schema(example = "Network error: Connection timeout")]
    NetworkError(String),
}