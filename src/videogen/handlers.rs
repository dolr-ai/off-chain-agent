use crate::app_state::AppState;
use crate::utils::gcs::maybe_upload_image_to_gcs;
use axum::{extract::State, http::StatusCode, Json};
use cloud_storage::Client;
use std::sync::Arc;
use videogen_common::VideoGenerator;

/// Helper function to process images in VideoGenInput
/// Uploads large images to GCS and replaces them with URLs
async fn process_input_image(
    input: &mut videogen_common::VideoGenInput,
    gcs_client: Arc<Client>,
    user_principal: &str,
) -> Result<(), (StatusCode, Json<videogen_common::VideoGenError>)> {
    // Get mutable reference to image if it exists
    if let Some(image_data) = input.get_image_mut() {
        // Process the image and update it in place
        *image_data = maybe_upload_image_to_gcs(gcs_client, image_data.clone(), user_principal)
            .await
            .map_err(|e| {
                log::error!("Failed to upload image to GCS: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(videogen_common::VideoGenError::NetworkError(format!(
                        "Failed to upload image: {e}"
                    ))),
                )
            })?;
    }

    Ok(())
}

/// Generate a video using delegated identity for authentication and balance deduction
#[utoipa::path(
    post,
    path = "/generate_with_identity",
    request_body = videogen_common::VideoGenRequestWithIdentity,
    responses(
        (status = 200, description = "Video generation started successfully", body = videogen_common::VideoGenQueuedResponse),
        (status = 400, description = "Invalid input", body = videogen_common::VideoGenError),
        (status = 401, description = "Authentication failed - Invalid identity", body = videogen_common::VideoGenError),
        (status = 402, description = "Insufficient balance", body = videogen_common::VideoGenError),
        (status = 429, description = "Rate limit exceeded", body = videogen_common::VideoGenError),
        (status = 502, description = "Provider error", body = videogen_common::VideoGenError),
        (status = 503, description = "Service unavailable", body = videogen_common::VideoGenError),
    ),
    tag = "VideoGen"
)]
pub async fn generate_video_with_identity(
    State(app_state): State<Arc<AppState>>,
    Json(identity_request): Json<videogen_common::VideoGenRequestWithIdentity>,
) -> Result<
    Json<videogen_common::VideoGenQueuedResponse>,
    (StatusCode, Json<videogen_common::VideoGenError>),
> {
    // Validate identity and extract user principal
    let user_principal = super::utils::validate_delegated_identity(&identity_request)?;

    // Extract request metadata
    let metadata = super::utils::extract_request_metadata(&identity_request.request);

    // Process image if present - upload large images to GCS
    let mut input = identity_request.request.input;
    process_input_image(
        &mut input,
        app_state.gcs_client.clone(),
        &user_principal.to_string(),
    )
    .await?;

    // Use common processing function
    let request_key = super::utils::process_video_generation(
        &app_state,
        user_principal,
        input,
        metadata.token_type,
        identity_request.delegated_identity.clone(),
        None,
    )
    .await?;

    // Build and return response
    Ok(Json(super::utils::build_queued_response(
        request_key,
        metadata.provider,
    )))
}
