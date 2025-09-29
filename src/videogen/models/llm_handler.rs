use serde::{Deserialize, Serialize};
use tracing::{error, info};
use videogen_common::{VideoGenError, VideoGenInput};

use crate::app_state::AppState;
use crate::consts::LLM_HANDLER_URL;

#[derive(Serialize)]
struct LlmHandlerRequest {
    prompt: String,
}

#[derive(Deserialize)]
pub struct LlmHandlerResponse {
    pub audio_description: String,
    pub video_description: String,
}

pub async fn generate(
    input: VideoGenInput,
    _app_state: &AppState,
) -> Result<LlmHandlerResponse, VideoGenError> {
    let VideoGenInput::LlmHandler(model) = input else {
        return Err(VideoGenError::InvalidInput(
            "Only LlmHandler input is supported".to_string(),
        ));
    };

    // Get JWT token from environment
    let jwt_token = std::env::var("LLM_HANDLER_JWT_TOKEN").map_err(|_| {
        error!("LLM_HANDLER_JWT_TOKEN not set in environment");
        VideoGenError::AuthError
    })?;

    let client = reqwest::Client::new();

    let request = LlmHandlerRequest {
        prompt: model.user_prompt.clone(),
    };

    info!(
        "Sending prompt to LLM handler: {}",
        &model.user_prompt[..model.user_prompt.len().min(100)]
    );

    // Send request to LLM handler service
    let response = client
        .post(LLM_HANDLER_URL)
        .header("Authorization", format!("Bearer {}", jwt_token))
        .header("Content-Type", "application/json")
        .json(&request)
        .send()
        .await
        .map_err(|e| {
            error!("Failed to send request to LLM handler: {}", e);
            VideoGenError::NetworkError(format!("Failed to connect to LLM handler: {}", e))
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());

        error!("LLM handler returned error: {} - {}", status, error_text);

        if status == 401 {
            return Err(VideoGenError::AuthError);
        }

        return Err(VideoGenError::ProviderError(format!(
            "LLM handler error: {} - {}",
            status, error_text
        )));
    }

    let llm_response: LlmHandlerResponse = response.json().await.map_err(|e| {
        error!("Failed to parse LLM handler response: {}", e);
        VideoGenError::ProviderError(format!("Invalid response from LLM handler: {}", e))
    })?;

    info!(
        "LLM handler generated prompts - Audio: {} chars, Video: {} chars",
        llm_response.audio_description.len(),
        llm_response.video_description.len()
    );

    Ok(llm_response)
}
