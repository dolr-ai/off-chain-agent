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

/// Generate a video with audio using the orchestrated pipeline
#[utoipa::path(
    post,
    path = "/generate-with-audio",
    request_body = OrchestratedVideoRequest,
    responses(
        (status = 200, description = "Video with audio generation started successfully", body = VideoGenQueuedResponseV2),
        (status = 400, description = "Invalid input", body = VideoGenError),
        (status = 401, description = "Authentication failed", body = VideoGenError),
        (status = 402, description = "Insufficient balance", body = VideoGenError),
        (status = 429, description = "Rate limit exceeded", body = VideoGenError),
        (status = 502, description = "Provider error", body = VideoGenError),
        (status = 503, description = "Service unavailable", body = VideoGenError),
    ),
    tag = "VideoGen V2"
)]
pub async fn generate_video_with_audio(
    State(app_state): State<Arc<AppState>>,
    Json(request): Json<OrchestratedVideoRequest>,
) -> Result<Json<VideoGenQueuedResponseV2>, (StatusCode, Json<VideoGenError>)> {
    use crate::videogen::orchestrator;

    // Validate identity and extract user principal
    let user_principal = validate_delegated_identity_v2(&VideoGenRequestWithIdentityV2 {
        request: videogen_common::VideoGenRequestV2 {
            principal: request.principal,
            prompt: request.prompt.clone(),
            model_id: "orchestrated".to_string(),
            token_type: request.token_type,
            negative_prompt: None,
            image: request.image.clone(),
            audio: None,
            aspect_ratio: None,
            duration_seconds: None,
            resolution: None,
            generate_audio: Some(true),
            seed: None,
            extra_params: Default::default(),
        },
        delegated_identity: request.delegated_identity,
    })?;

    // Process image if present
    let mut processed_image = request.image.clone();
    if let Some(ref mut image) = processed_image {
        process_input_image_v2(
            &mut Some(image.clone()),
            app_state.gcs_client.clone(),
            &user_principal.to_string(),
        )
        .await?;
    }

    // Run the orchestration pipeline
    let result = orchestrator::generate_video_with_audio(
        app_state,
        request.prompt,
        processed_image,
        request.principal,
    )
    .await
    .map_err(|e| {
        log::error!("Orchestrated generation failed: {}", e);
        match e {
            VideoGenError::AuthError => (StatusCode::UNAUTHORIZED, Json(e)),
            VideoGenError::InvalidInput(_) => (StatusCode::BAD_REQUEST, Json(e)),
            VideoGenError::NetworkError(_) => (StatusCode::BAD_GATEWAY, Json(e)),
            VideoGenError::ProviderError(_) => (StatusCode::BAD_GATEWAY, Json(e)),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, Json(e)),
        }
    })?;

    Ok(Json(VideoGenQueuedResponseV2 {
        operation_id: result.operation_id,
        provider: result.provider,
        request_key: videogen_common::VideoGenRequestKey {
            principal: request.principal,
            counter: chrono::Utc::now().timestamp_millis() as u64,
        },
    }))
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, utoipa::ToSchema)]
pub struct OrchestratedVideoRequest {
    #[serde(rename = "user_id")]
    #[schema(value_type = String, example = "xkbqi-2qaaa-aaaah-qbpqq-cai")]
    pub principal: candid::Principal,

    /// The user's prompt that will be sent to LLM for enhancement
    #[schema(example = "A serene sunset over mountains")]
    pub prompt: String,

    /// Optional input image for I2V mode
    pub image: Option<videogen_common::ImageData>,

    /// Token type for payment
    #[serde(default)]
    pub token_type: videogen_common::TokenType,

    #[schema(value_type = Object)]
    pub delegated_identity: yral_types::delegated_identity::DelegatedIdentityWire,
}
