use tracing::info;
use videogen_common::types::ImageData;
use videogen_common::types_v2::VideoUploadHandling;
use videogen_common::{VideoGenError, VideoGenInput, VideoGenResponse};

use crate::app_state::AppState;
use crate::consts::OFF_CHAIN_AGENT_URL;
use crate::videogen::comfyui_client::VideoGenMode;
use crate::videogen::comfyui_webhook::generate_comfyui_webhook_url;

/// Generate video using LTX-2 on self-hosted ComfyUI
pub async fn generate_with_context(
    input: VideoGenInput,
    app_state: &AppState,
    context: &crate::videogen::qstash_types::QstashVideoGenRequest,
) -> Result<VideoGenResponse, VideoGenError> {
    let VideoGenInput::Ltx2(model) = input else {
        return Err(VideoGenError::InvalidInput(
            "Only Ltx2 input is supported".to_string(),
        ));
    };

    let comfyui_client = app_state
        .comfyui_client
        .as_ref()
        .ok_or_else(|| VideoGenError::ProviderError("ComfyUI client not configured".to_string()))?;

    // Determine generation mode based on inputs
    let mode = match (&model.image, model.prompt.is_empty()) {
        (Some(image), true) => {
            // Image only - image-to-video
            let image_url = get_image_url(image)?;
            VideoGenMode::ImageToVideo {
                image_url,
                prompt: None,
            }
        }
        (Some(image), false) => {
            // Image + prompt - image+text-to-video
            let image_url = get_image_url(image)?;
            VideoGenMode::ImageTextToVideo {
                image_url,
                prompt: model.prompt.clone(),
            }
        }
        (None, false) => {
            // Prompt only - text-to-video
            VideoGenMode::TextToVideo {
                prompt: model.prompt.clone(),
            }
        }
        (None, true) => {
            return Err(VideoGenError::InvalidInput(
                "Either prompt or image must be provided".to_string(),
            ));
        }
    };

    let video_upload_handling_str = match &context.handle_video_upload {
        Some(VideoUploadHandling::Client) => "Client",
        Some(VideoUploadHandling::ServerDraft) => "ServerDraft",
        None => "Client",
    };

    let webhook_url = generate_comfyui_webhook_url(
        OFF_CHAIN_AGENT_URL.as_str(),
        &context.request_key.principal.to_string(),
        context.request_key.counter,
        video_upload_handling_str,
    );

    let max_chars = 60;
    let prompt = &model.prompt;
    let truncated_prompt = prompt
        .char_indices()
        .nth(max_chars)
        .map(|(idx, _)| &prompt[..idx])
        .unwrap_or(prompt);

    info!(
        "Submitting LTX-2 generation for prompt: {}",
        truncated_prompt
    );

    // Extra params to include in webhook callback
    let extra_params = serde_json::json!({
        "model": "ltx2",
        "principal": context.request_key.principal.to_string(),
        "counter": context.request_key.counter,
    });

    let response = comfyui_client
        .submit_video_generation(mode, &webhook_url, extra_params)
        .await?;

    info!(
        "LTX-2 generation submitted with ID: {} (webhook will be called on completion)",
        response.id
    );

    // Return immediately - webhook will handle completion
    Ok(VideoGenResponse {
        operation_id: response.id,
        video_url: String::new(), // Will be filled by webhook
        provider: "ltx2".to_string(),
    })
}

/// Extract URL from ImageData (either direct URL or convert base64 to data URI)
fn get_image_url(image: &ImageData) -> Result<String, VideoGenError> {
    match image {
        ImageData::Url(url) => Ok(url.clone()),
        ImageData::Base64(input) => {
            // Convert to data URI for ComfyUI LoadImage node
            Ok(format!("data:{};base64,{}", input.mime_type, input.data))
        }
    }
}
