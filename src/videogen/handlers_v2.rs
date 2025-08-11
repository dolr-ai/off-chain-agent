use axum::{extract::State, http::StatusCode, Json};
use ic_agent::identity::{DelegatedIdentity, Identity};
use std::sync::Arc;
use videogen_common::{
    ProvidersResponse, VideoGenError, VideoGenQueuedResponseV2, VideoGenRequestWithIdentityV2,
    VideoGenerator, ADAPTER_REGISTRY,
};

use crate::app_state::AppState;
use crate::utils::gcs::maybe_upload_image_to_gcs;
use cloud_storage::Client;

/// Helper function to process images in unified request
/// Uploads large images to GCS and replaces them with URLs
async fn process_input_image_v2(
    image: &mut Option<videogen_common::ImageData>,
    gcs_client: Arc<Client>,
    user_principal: &str,
) -> Result<(), (StatusCode, Json<VideoGenError>)> {
    if let Some(image_data) = image {
        *image_data = maybe_upload_image_to_gcs(gcs_client, image_data.clone(), user_principal)
            .await
            .map_err(|e| {
                log::error!("Failed to upload image to GCS: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(VideoGenError::NetworkError(format!(
                        "Failed to upload image: {e}"
                    ))),
                )
            })?;
    }
    Ok(())
}

/// Get all available video generation providers
#[utoipa::path(
    get,
    path = "/providers",
    responses(
        (status = 200, description = "List of available providers", body = ProvidersResponse),
    ),
    tag = "VideoGen V2"
)]
pub async fn get_providers() -> Json<ProvidersResponse> {
    Json(ADAPTER_REGISTRY.get_all_prod_providers())
}

/// Get all available video generation providers
#[utoipa::path(
    get,
    path = "/providers-all",
    responses(
        (status = 200, description = "List of available providers", body = ProvidersResponse),
    ),
    tag = "VideoGen V2"
)]
pub async fn get_providers_all() -> Json<ProvidersResponse> {
    Json(ADAPTER_REGISTRY.get_all_providers())
}

/// Generate a video using unified request structure (V2 API)
#[utoipa::path(
    post,
    path = "/generate",
    request_body = VideoGenRequestWithIdentityV2,
    responses(
        (status = 200, description = "Video generation started successfully", body = VideoGenQueuedResponseV2),
        (status = 400, description = "Invalid input", body = VideoGenError),
        (status = 401, description = "Authentication failed - Invalid identity", body = VideoGenError),
        (status = 402, description = "Insufficient balance", body = VideoGenError),
        (status = 429, description = "Rate limit exceeded", body = VideoGenError),
        (status = 502, description = "Provider error", body = VideoGenError),
        (status = 503, description = "Service unavailable", body = VideoGenError),
    ),
    tag = "VideoGen V2"
)]
pub async fn generate_video_with_identity_v2(
    State(app_state): State<Arc<AppState>>,
    Json(identity_request): Json<VideoGenRequestWithIdentityV2>,
) -> Result<Json<VideoGenQueuedResponseV2>, (StatusCode, Json<VideoGenError>)> {
    // Validate identity and extract user principal
    let user_principal = validate_delegated_identity_v2(&identity_request)?;

    // Check if model is available
    if !ADAPTER_REGISTRY.is_model_available(&identity_request.request.model_id) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(VideoGenError::InvalidInput(format!(
                "Model '{}' is not available",
                identity_request.request.model_id
            ))),
        ));
    }

    // Process image if present - upload large images to GCS
    let mut unified_request = identity_request.request.clone();
    process_input_image_v2(
        &mut unified_request.image,
        app_state.gcs_client.clone(),
        &user_principal.to_string(),
    )
    .await?;

    // Adapt unified request to model-specific format
    let video_gen_input = ADAPTER_REGISTRY
        .adapt_request(unified_request.clone())
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(e)))?;

    // Get provider for response
    let provider = video_gen_input.provider();

    // Use common processing function
    let request_key = super::utils::process_video_generation(
        &app_state,
        user_principal,
        video_gen_input,
        unified_request.token_type,
        identity_request.delegated_identity.clone(),
    )
    .await?;

    // Build and return response
    Ok(Json(VideoGenQueuedResponseV2 {
        operation_id: format!("{}_{}", request_key.principal, request_key.counter),
        provider: provider.to_string(),
        request_key: videogen_common::VideoGenRequestKey {
            principal: request_key.principal,
            counter: request_key.counter,
        },
    }))
}

/// Validates the delegated identity and returns the user principal (V2 version)
fn validate_delegated_identity_v2(
    identity_request: &VideoGenRequestWithIdentityV2,
) -> Result<candid::Principal, (StatusCode, Json<VideoGenError>)> {
    // Parse delegated identity
    let identity: DelegatedIdentity = identity_request
        .delegated_identity
        .clone()
        .try_into()
        .map_err(|e: k256::elliptic_curve::Error| {
            log::error!("Failed to create delegated identity: {e}");
            (StatusCode::UNAUTHORIZED, Json(VideoGenError::AuthError))
        })?;

    // Extract user principal
    let user_principal = identity
        .sender()
        .map_err(|_| (StatusCode::UNAUTHORIZED, Json(VideoGenError::AuthError)))?;

    // Verify the principal matches the request
    if user_principal != identity_request.request.principal {
        return Err((StatusCode::UNAUTHORIZED, Json(VideoGenError::AuthError)));
    }

    log::info!("Identity verified for user {user_principal} (V2 API)");
    Ok(user_principal)
}
