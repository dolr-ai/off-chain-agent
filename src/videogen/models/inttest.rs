use videogen_common::{VideoGenError, VideoGenInput, VideoGenResponse};
use crate::app_state::AppState;

pub async fn generate(
    input: VideoGenInput,
    _app_state: &AppState,
) -> Result<VideoGenResponse, VideoGenError> {
    match input {
        VideoGenInput::IntTest { prompt, image: _ } => {
            log::info!("IntTest: Generating video for prompt: {}", prompt);
            
            // Always return the same URL
            Ok(VideoGenResponse {
                operation_id: "inttest-static-id".to_string(),
                video_url: "https://storage.cdn-luma.com/dream_machine/7f48468f-e704-44db-ba86-7bb9fa754352/b1696749-f2e3-4b9b-b5db-404daf160e11_resulte6b5644af53da416.mp4".to_string(),
                provider: "inttest".to_string(),
            })
        }
        _ => Err(VideoGenError::InvalidInput(
            "Only IntTest input is supported".to_string(),
        )),
    }
}