use axum::debug_handler;
use axum::{extract::State, http::StatusCode, Json};
use ic_agent::identity::{DelegatedIdentity, Identity};
use std::sync::Arc;
use videogen_common::{
    ProvidersResponse, VideoGenError, VideoGenQueuedResponseV2, VideoGenRequestWithIdentityV2,
    VideoGenerator, ADAPTER_REGISTRY,
};

use crate::app_state::AppState;
use crate::utils::gcs::{maybe_upload_image_to_gcs, upload_audio_if_needed};
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

/// Helper function to process audio in unified request
/// Uploads large audio to GCS and replaces them with URLs
async fn process_input_audio(
    audio: &mut Option<videogen_common::AudioData>,
    gcs_client: Arc<Client>,
    user_principal: &str,
) -> Result<(), (StatusCode, Json<VideoGenError>)> {
    if let Some(audio_data) = audio {
        *audio_data = upload_audio_if_needed(gcs_client, audio_data.clone(), user_principal)
            .await
            .map_err(|e| {
                log::error!("Failed to upload audio to GCS: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(VideoGenError::NetworkError(format!(
                        "Failed to upload audio: {e}"
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
#[debug_handler]
pub async fn generate_video_with_identity_v2(
    State(app_state): State<Arc<AppState>>,
    Json(mut identity_request): Json<VideoGenRequestWithIdentityV2>,
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

    // process audio if present - upload large audio to GCS
    process_input_audio(
        &mut identity_request.request.audio,
        app_state.gcs_client.clone(),
        &user_principal.to_string(),
    )
    .await?;

    // Process image if present - upload large images to GCS
    process_input_image_v2(
        &mut identity_request.request.image,
        app_state.gcs_client.clone(),
        &user_principal.to_string(),
    )
    .await?;

    // Adapt unified request to model-specific format
    let video_gen_input = ADAPTER_REGISTRY
        .adapt_request(identity_request.request.clone())
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(e)))?;

    // Get provider for response
    let provider = video_gen_input.provider();

    // Use common processing function
    let request_key = super::utils::process_video_generation(
        &app_state,
        user_principal,
        video_gen_input,
        identity_request.request.token_type,
        identity_request.delegated_identity.clone(),
        identity_request.upload_handling,
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

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct InProgressVideoItem {
    pub counter: u64,
    pub model_name: String,
    pub prompt: String,
    pub created_at: u64,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct InProgressVideoResponse {
    pub videos: Vec<InProgressVideoItem>,
}

/// Get in-progress video generations for a given principal
#[utoipa::path(
    get,
    path = "/in-progress/{principal}",
    params(
        ("principal" = String, Path, description = "User Principal ID")
    ),
    responses(
        (status = 200, description = "List of in-progress videos", body = InProgressVideoResponse),
        (status = 400, description = "Invalid principal", body = VideoGenError),
        (status = 502, description = "Canister error", body = VideoGenError),
    ),
    tag = "VideoGen V2"
)]
#[debug_handler]
pub async fn get_in_progress_videos(
    State(app_state): State<Arc<AppState>>,
    axum::extract::Path(principal): axum::extract::Path<String>,
) -> Result<Json<InProgressVideoResponse>, (StatusCode, Json<VideoGenError>)> {
    use crate::consts::RATE_LIMITS_CANISTER_ID;
    use std::str::FromStr;
    use yral_canisters_client::rate_limits::{RateLimits, VideoGenRequestStatus};

    let user_principal = candid::Principal::from_str(&principal).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(VideoGenError::InvalidInput(format!(
                "Invalid principal: {e}"
            ))),
        )
    })?;

    let rate_limits_client = RateLimits(*RATE_LIMITS_CANISTER_ID, &app_state.agent);

    let (requests,) = rate_limits_client
        .get_user_video_generation_requests(user_principal, None, None)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                Json(VideoGenError::NetworkError(format!(
                    "Failed to fetch video generation requests: {e}"
                ))),
            )
        })?;

    let in_progress_videos = requests
        .into_iter()
        .filter(|(_, req)| {
            matches!(
                req.status,
                VideoGenRequestStatus::Pending | VideoGenRequestStatus::Processing
            )
        })
        .map(|(key, req)| InProgressVideoItem {
            counter: key.counter,
            model_name: req.model_name,
            prompt: req.prompt,
            created_at: req.created_at,
        })
        .collect();

    Ok(Json(InProgressVideoResponse {
        videos: in_progress_videos,
    }))
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct VideoStatusItem {
    pub counter: u64,
    pub model_name: String,
    pub prompt: String,
    pub status: String,
    pub created_at: u64,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct AllVideoStatusResponse {
    pub videos: Vec<VideoStatusItem>,
}

/// Get all video generation statuses for a given principal
#[utoipa::path(
    get,
    path = "/status/{principal}/all",
    params(
        ("principal" = String, Path, description = "User Principal ID")
    ),
    responses(
        (status = 200, description = "List of all video statuses", body = AllVideoStatusResponse),
        (status = 400, description = "Invalid principal", body = VideoGenError),
        (status = 502, description = "Canister error", body = VideoGenError),
    ),
    tag = "VideoGen V2"
)]
#[debug_handler]
pub async fn get_all_video_status(
    State(app_state): State<Arc<AppState>>,
    axum::extract::Path(principal): axum::extract::Path<String>,
) -> Result<Json<AllVideoStatusResponse>, (StatusCode, Json<VideoGenError>)> {
    use crate::consts::RATE_LIMITS_CANISTER_ID;
    use std::str::FromStr;
    use yral_canisters_client::rate_limits::{RateLimits, VideoGenRequestStatus};

    let user_principal = candid::Principal::from_str(&principal).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(VideoGenError::InvalidInput(format!(
                "Invalid principal: {e}"
            ))),
        )
    })?;

    let rate_limits_client = RateLimits(*RATE_LIMITS_CANISTER_ID, &app_state.agent);

    let (requests,) = rate_limits_client
        .get_user_video_generation_requests(user_principal, None, None)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                Json(VideoGenError::NetworkError(format!(
                    "Failed to fetch video generation requests: {e}"
                ))),
            )
        })?;

    let all_videos = requests
        .into_iter()
        .map(|(key, req)| {
            let status_str = match req.status {
                VideoGenRequestStatus::Failed(reason) => format!("Failed: {}", reason),
                VideoGenRequestStatus::Complete(url) => format!("Complete: {}", url),
                VideoGenRequestStatus::Processing => "Processing".to_string(),
                VideoGenRequestStatus::Pending => "Pending".to_string(),
            };
            VideoStatusItem {
                counter: key.counter,
                model_name: req.model_name,
                prompt: req.prompt,
                status: status_str,
                created_at: req.created_at,
            }
        })
        .collect();

    Ok(Json(AllVideoStatusResponse { videos: all_videos }))
}
