use crate::app_state::AppState;
use videogen_common::{VideoGenError, VideoGenInput, VideoGenResponse};

pub async fn generate(
    input: VideoGenInput,
    _app_state: &AppState,
) -> Result<VideoGenResponse, VideoGenError> {
    let VideoGenInput::IntTest(model) = input else {
        return Err(VideoGenError::InvalidInput(
            "Only IntTest input is supported".to_string(),
        ));
    };

    // tokio::time::sleep(std::time::Duration::from_secs(15)).await; // Simulate processing delay

    log::info!("IntTest: Generating video for prompt: {}", model.prompt);
    
    // Log if image is present (for testing)
    if model.image.is_some() {
        log::info!("IntTest: Image provided (ignored for test)");
    }

    // Always return the same URL
    Ok(VideoGenResponse {
        operation_id: "inttest-static-id".to_string(),
        video_url: "https://storage.cdn-luma.com/dream_machine/7f48468f-e704-44db-ba86-7bb9fa754352/b1696749-f2e3-4b9b-b5db-404daf160e11_resulte6b5644af53da416.mp4".to_string(),
        provider: "inttest".to_string(),
    })
}
