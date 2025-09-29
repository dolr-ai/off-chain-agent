use candid::Principal;
use std::sync::Arc;
use tracing::{error, info};
use videogen_common::{
    models::{LlmHandlerModel, StableAudioModel, Wan22Model},
    ImageData, VideoGenError, VideoGenInput, VideoGenResponse,
};

use crate::app_state::AppState;
use crate::videogen::models::{llm_handler, stable_audio, wan2_2};
use crate::videogen::utils::stitch_video_audio_and_upload;

/// Orchestrates the complete video generation pipeline with audio
/// 1. Call LLM handler to generate prompts
/// 2. Generate video with WAN 2.2
/// 3. Generate audio with Stable Audio
/// 4. Stitch video and audio together
pub async fn generate_video_with_audio(
    app_state: Arc<AppState>,
    user_prompt: String,
    input_image: Option<ImageData>,
    user_principal: Principal,
) -> Result<VideoGenResponse, VideoGenError> {
    info!(
        "Starting orchestrated video generation for user: {}",
        user_principal
    );

    // Step 1: Call LLM Handler to generate prompts
    info!("Step 1: Generating prompts with LLM handler");
    let llm_input = VideoGenInput::LlmHandler(LlmHandlerModel {
        user_prompt: user_prompt.clone(),
    });

    let llm_response = llm_handler::generate(llm_input, &app_state)
        .await
        .map_err(|e| {
            error!("Failed to generate prompts: {}", e);
            e
        })?;

    info!(
        "Generated prompts - Video: {} chars, Audio: {} chars",
        llm_response.video_description.len(),
        llm_response.audio_description.len()
    );

    // Step 2: Generate video with WAN 2.2
    info!("Step 2: Generating video with WAN 2.2");
    let is_i2v = input_image.is_some();
    let wan_input = VideoGenInput::Wan22(Wan22Model {
        prompt: llm_response.video_description,
        image: input_image,
        is_i2v,
    });

    // Run video and audio generation in parallel
    let video_future = wan2_2::generate(wan_input, &app_state);

    // Step 3: Generate audio with Stable Audio (in parallel)
    info!("Step 3: Generating audio with Stable Audio (in parallel)");
    let audio_input = VideoGenInput::StableAudio(StableAudioModel {
        prompt: llm_response.audio_description,
        duration: 90, // Default duration
    });

    let audio_future = stable_audio::generate(audio_input, &app_state);

    // Wait for both to complete
    let (video_result, audio_result) = tokio::join!(video_future, audio_future);

    let video_response = video_result.map_err(|e| {
        error!("Failed to generate video: {}", e);
        e
    })?;

    let audio_response = audio_result.map_err(|e| {
        error!("Failed to generate audio: {}", e);
        e
    })?;

    info!(
        "Video generated: {}, Audio generated: {}",
        video_response.video_url, audio_response.video_url
    );

    // Step 4: Stitch video and audio together
    info!("Step 4: Stitching video and audio");
    let stitched_url = stitch_video_audio_and_upload(
        app_state.gcs_client.clone(),
        &video_response.video_url,
        &audio_response.video_url, // Using video_url field for audio URL
        user_principal,
        Some(format!(
            "orchestrated_{}_{}",
            video_response.operation_id,
            chrono::Utc::now().timestamp_millis()
        )),
    )
    .await
    .map_err(|e| {
        error!("Failed to stitch video and audio: {}", e);
        e
    })?;

    info!("Successfully generated video with audio: {}", stitched_url);

    Ok(VideoGenResponse {
        operation_id: format!(
            "{}_{}",
            video_response.operation_id, audio_response.operation_id
        ),
        video_url: stitched_url,
        provider: "orchestrated_wan2_2_stable_audio".to_string(),
    })
}

/// Simplified version that takes just a prompt and generates everything
pub async fn generate_from_prompt(
    app_state: Arc<AppState>,
    user_prompt: String,
    user_principal: Principal,
) -> Result<VideoGenResponse, VideoGenError> {
    generate_video_with_audio(app_state, user_prompt, None, user_principal).await
}
