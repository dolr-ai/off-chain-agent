use crate::app_state::AppState;
use crate::videogen::qstash_types::QstashVideoGenRequest;
use videogen_common::models::ltx2::Ltx2Model;
use videogen_common::{VideoGenError, VideoGenInput, VideoGenResponse};

pub async fn generate_with_context(
    input: VideoGenInput,
    app_state: &AppState,
    context: &QstashVideoGenRequest,
) -> Result<VideoGenResponse, VideoGenError> {
    let VideoGenInput::IntTest(model) = input else {
        return Err(VideoGenError::InvalidInput(
            "Only IntTest input is supported".to_string(),
        ));
    };

    log::info!("IntTest: Routing to LTX2 for prompt: {}", model.prompt);

    // Convert IntTest input to LTX2 input and run LTX2 under the hood
    let ltx2_input = VideoGenInput::Ltx2(Ltx2Model {
        prompt: model.prompt,
        image: model.image,
    });

    // Create a modified context with LTX2 input
    let ltx2_context = QstashVideoGenRequest {
        user_principal: context.user_principal,
        input: ltx2_input.clone(),
        request_key: context.request_key.clone(),
        property: context.property.clone(),
        deducted_amount: context.deducted_amount,
        token_type: context.token_type.clone(),
        handle_video_upload: context.handle_video_upload.clone(),
    };

    // Call LTX2 handler
    super::ltx2::generate_with_context(ltx2_input, app_state, &ltx2_context).await
}
