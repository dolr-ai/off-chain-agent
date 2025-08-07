use axum::{extract::State, http::StatusCode, Json};
use ic_agent::identity::DelegatedIdentity;
use std::sync::Arc;
use videogen_common::VideoGenerator;

use super::rate_limit::verify_rate_limit_and_create_request_v1;
use super::token_operations::deduct_balance_with_cleanup;
use crate::app_state::AppState;
use crate::utils::gcs::maybe_upload_image_to_gcs;
use cloud_storage::Client;

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

    // Determine payment amount for rate limit
    let payment_amount = if matches!(metadata.token_type, videogen_common::TokenType::Free) {
        None
    } else {
        Some(metadata.cost)
    };

    // Verify rate limit
    let request_key = verify_rate_limit_and_create_request_v1(
        user_principal,
        &metadata.model_name,
        &metadata.prompt,
        &metadata.property,
        &metadata.token_type,
        payment_amount,
        &app_state,
    )
    .await
    .map_err(|(status, error)| (status, Json(error)))?;

    // Create delegated identity for agent creation
    let identity: DelegatedIdentity = identity_request
        .delegated_identity
        .clone()
        .try_into()
        .map_err(|e: k256::elliptic_curve::Error| {
            log::error!("Failed to create delegated identity: {e}");
            (
                StatusCode::UNAUTHORIZED,
                Json(videogen_common::VideoGenError::AuthError),
            )
        })?;

    // Create user agent if needed for DOLR operations
    let user_agent = super::utils::create_user_agent_if_dolr(identity, &metadata.token_type)?;

    // Get JWT token
    let jwt_token = super::utils::get_hon_worker_jwt_token()?;

    // Deduct balance with automatic cleanup on failure
    let deducted_amount = deduct_balance_with_cleanup(
        user_principal,
        metadata.cost,
        &metadata.token_type,
        jwt_token.clone(),
        &app_state.agent,
        &request_key,
        &metadata.property,
        user_agent.as_ref(),
    )
    .await?;

    // Process image if present - upload large images to GCS
    let mut input = identity_request.request.input;
    process_input_image(
        &mut input,
        app_state.gcs_client.clone(),
        &user_principal.to_string(),
    )
    .await?;

    // Prepare Qstash request
    let qstash_request = super::qstash_types::QstashVideoGenRequest {
        user_principal,
        input,
        request_key: request_key.clone(),
        property: metadata.property.clone(),
        deducted_amount,
        token_type: metadata.token_type,
    };

    // Queue to Qstash with automatic rollback on failure
    super::utils::queue_to_qstash_with_rollback(
        &app_state,
        qstash_request,
        jwt_token,
        user_principal,
    )
    .await?;

    // Build and return response
    Ok(Json(super::utils::build_queued_response(
        request_key,
        metadata.provider,
    )))
}
