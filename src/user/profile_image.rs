use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use utoipa::ToSchema;

use crate::{
    app_state::AppState,
    types::DelegatedIdentityWire,
    utils::delegated_identity::get_user_info_from_delegated_identity_wire,
    utils::s3::upload_profile_image_to_s3,
};

#[derive(Serialize, Deserialize, ToSchema)]
pub struct UploadProfileImageRequest {
    pub delegated_identity_wire: DelegatedIdentityWire,
    pub image_data: String, // Base64 encoded image data
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct UploadProfileImageResponse {
    pub profile_image_url: String,
}

#[utoipa::path(
    post,
    path = "/profile-image",
    request_body = UploadProfileImageRequest,
    tag = "user",
    responses(
        (status = 200, description = "Profile image uploaded successfully", body = UploadProfileImageResponse),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    )
)]
#[instrument(skip(state, request))]
pub async fn handle_upload_profile_image(
    State(state): State<Arc<AppState>>,
    Json(request): Json<UploadProfileImageRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Verify the user identity and get user info
    let user_info =
        get_user_info_from_delegated_identity_wire(&state, request.delegated_identity_wire.clone())
            .await
            .map_err(|e| {
                (
                    StatusCode::UNAUTHORIZED,
                    format!("Failed to get user info: {e}"),
                )
            })?;

    let user_principal = user_info.user_principal;

    // Remove data URL prefix if present
    let base64_data = if let Some(comma_pos) = request.image_data.find(',') {
        &request.image_data[comma_pos + 1..]
    } else {
        &request.image_data
    };

    // Validate image data size (optional, can add more validation)
    if base64_data.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Image data is empty".to_string(),
        ));
    }

    // Maximum allowed size for base64 string (e.g., ~10MB)
    const MAX_BASE64_SIZE: usize = 14 * 1024 * 1024; // ~10MB when decoded
    if base64_data.len() > MAX_BASE64_SIZE {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("Image data too large. Maximum size is {} bytes", MAX_BASE64_SIZE),
        ));
    }

    // Upload image to S3
    let profile_image_url = upload_profile_image_to_s3(
        base64_data,
        &user_principal.to_text(),
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to upload profile image: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to upload profile image: {e}"),
        )
    })?;

    tracing::info!(
        "Successfully uploaded profile image for user {}: {}",
        user_principal,
        profile_image_url
    );

    Ok(Json(UploadProfileImageResponse {
        profile_image_url,
    }))
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct DeleteProfileImageRequest {
    pub delegated_identity_wire: DelegatedIdentityWire,
}

#[utoipa::path(
    delete,
    path = "/profile-image",
    request_body = DeleteProfileImageRequest,
    tag = "user",
    responses(
        (status = 200, description = "Profile image deleted successfully"),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    )
)]
#[instrument(skip(state, request))]
pub async fn handle_delete_profile_image(
    State(state): State<Arc<AppState>>,
    Json(request): Json<DeleteProfileImageRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Verify the user identity and get user info
    let user_info =
        get_user_info_from_delegated_identity_wire(&state, request.delegated_identity_wire.clone())
            .await
            .map_err(|e| {
                (
                    StatusCode::UNAUTHORIZED,
                    format!("Failed to get user info: {e}"),
                )
            })?;

    let user_principal = user_info.user_principal;

    // Delete image from S3
    crate::utils::s3::delete_profile_image_from_s3(&user_principal.to_text())
        .await
        .map_err(|e| {
            tracing::error!("Failed to delete profile image: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to delete profile image: {e}"),
            )
        })?;

    tracing::info!(
        "Successfully deleted profile image for user {}",
        user_principal
    );

    Ok(StatusCode::OK)
}