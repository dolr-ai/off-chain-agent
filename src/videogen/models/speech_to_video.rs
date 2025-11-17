use std::time::Duration;

use futures::StreamExt;
use log::info;
use serde_json::Value;
use videogen_common::{
    types_v2::VideoUploadHandling, AudioData, VideoGenError, VideoGenInput, VideoGenResponse,
};

use crate::{
    app_state::AppState,
    consts::{OFF_CHAIN_AGENT_URL, REPLICATE_API_URL, REPLICATE_WAN2_5_FAST_MODEL},
    videogen::{
        models::wan2_5::{ReplicatePredictionRequest, ReplicatePredictionResponse, Wan25Input},
        replicate_webhook::generate_webhook_url,
    },
};

pub async fn generate_prompt_from_speech(
    client: &reqwest::Client,
    replicate_token: &str,
    audio: AudioData,
) -> Result<String, VideoGenError> {
    // 1️⃣ POST to create a prediction
    let payload = serde_json::json!({
        "stream": true,
        "input": {
            "language": "en",
            "audio_file": audio.to_url()
        }
    });

    let url = format!("{REPLICATE_API_URL}/models/openai/gpt-4o-transcribe/predictions");

    let prediction_resp = client
        .post(url)
        .bearer_auth(replicate_token)
        .json(&payload)
        .send()
        .await
        .map_err(|e| VideoGenError::NetworkError(e.to_string()))?;

    let prediction_json: Value = prediction_resp
        .json()
        .await
        .map_err(|e| VideoGenError::NetworkError(e.to_string()))?;

    let stream_url = prediction_json["urls"]["stream"]
        .as_str()
        .ok_or(VideoGenError::ProviderError(
            "Missing stream URL in prediction response".to_string(),
        ))?
        .to_string();

    let mut stream = client
        .get(&stream_url)
        .header("Accept", "text/event-stream")
        .header("Cache-Control", "no-store")
        .send()
        .await
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to connect to stream: {e}")))?
        .bytes_stream();

    let mut prompt = String::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                let text = String::from_utf8_lossy(&bytes);
                prompt += &text
            }
            Err(e) => {
                log::error!("Stream error: {e}");
                break;
            }
        }
    }

    Ok(prompt)
}

pub async fn generate_with_context(
    input: VideoGenInput,
    app_state: &AppState,
    context: &crate::videogen::qstash_types::QstashVideoGenRequest,
) -> Result<VideoGenResponse, VideoGenError> {
    let VideoGenInput::SpeechToVideo(model) = input else {
        return Err(VideoGenError::InvalidInput(
            "Only SpeechToVideo input is supported".to_string(),
        ));
    };

    let prompt = generate_prompt_from_speech(
        &reqwest::Client::new(),
        &app_state.replicate_api_token,
        model.audio.clone(),
    )
    .await?;

    let api_key = &app_state.replicate_api_token;
    if api_key.is_empty() {
        return Err(VideoGenError::AuthError);
    }

    let client = reqwest::Client::new();

    // Check if we should use webhook (when context is provided)
    let video_upload_handling_str = match &context.handle_video_upload {
        Some(VideoUploadHandling::Client) => "Client",
        Some(VideoUploadHandling::ServerDraft) => "ServerDraft",
        None => "Client", // Default to Client if None
    };

    let webhook_url = generate_webhook_url(
        OFF_CHAIN_AGENT_URL.as_str(),
        &context.request_key.principal.to_string(),
        context.request_key.counter,
        video_upload_handling_str,
    );

    // Build request with hardcoded parameters
    let request = ReplicatePredictionRequest {
        version: REPLICATE_WAN2_5_FAST_MODEL.to_string(),
        input: Wan25Input {
            prompt: prompt.clone(),
            size: "720*1280".to_string(),
            duration: 5,
            seed: -1,
            negative_prompt: Some("".to_string()),
            enable_prompt_expansion: true,
        },
        webhook: Some(webhook_url),
        metadata: None,
    };

    // Submit prediction
    let submit_url = format!("{REPLICATE_API_URL}/predictions");

    info!(
        "Submitting Wan 2.5 Fast generation prediction for prompt: {}",
        &prompt[..prompt.len().min(60)]
    );

    let response = client
        .post(submit_url)
        .bearer_auth(api_key)
        .header("Content-Type", "application/json")
        .json(&request)
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to submit prediction: {e}")))?;

    if !response.status().is_success() {
        let error_text = response
            .text()
            .await
            .unwrap_or_else(|_| "Unknown error".to_string());
        return Err(VideoGenError::ProviderError(format!(
            "Replicate API error: {error_text}"
        )));
    }

    let prediction_response: ReplicatePredictionResponse = response.json().await.map_err(|e| {
        VideoGenError::ProviderError(format!("Failed to parse prediction response: {e}"))
    })?;

    info!(
        "Wan 2.5 Fast prediction submitted with ID: {}",
        prediction_response.id
    );

    // With webhooks, return immediately - the webhook will handle completion
    info!(
        "Using webhook for Wan 2.5 Fast prediction {}, returning immediately",
        prediction_response.id
    );
    Ok(VideoGenResponse {
        operation_id: prediction_response.id,
        video_url: String::new(), // Will be filled by webhook
        provider: "wan2_5_fast".to_string(),
    })
}
