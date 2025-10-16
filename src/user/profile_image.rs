use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use base64::Engine;
use serde::{Deserialize, Serialize};
use tracing::instrument;
use utoipa::ToSchema;

use crate::{
    app_state::AppState, consts::USER_INFO_SERVICE_CANISTER_ID, types::DelegatedIdentityWire,
    user::utils::get_agent_from_delegated_identity_wire,
    utils::delegated_identity::get_user_info_from_delegated_identity_wire,
    utils::s3::upload_profile_image_to_s3,
};
use yral_canisters_client::user_info_service::{ProfileUpdateDetails, UserInfoService};

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

    // Set Sentry user context for tracking
    crate::middleware::set_user_context(user_principal);

    // Remove data URL prefix if present
    let base64_data = if let Some(comma_pos) = request.image_data.find(',') {
        &request.image_data[comma_pos + 1..]
    } else {
        &request.image_data
    };

    // Validate image data size
    if base64_data.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Image data is empty".to_string()));
    }

    // Maximum allowed size for base64 string (~5MB when decoded)
    const MAX_BASE64_SIZE: usize = 7 * 1024 * 1024; // ~5MB when decoded
    if base64_data.len() > MAX_BASE64_SIZE {
        let size_mb = base64_data.len() / (1024 * 1024);
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Image too large: {}MB. Maximum allowed size is 5MB",
                size_mb
            ),
        ));
    }

    // Validate that it's actually base64 data
    if base64::engine::general_purpose::STANDARD
        .decode(base64_data)
        .is_err()
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid image data format. Please upload a valid image".to_string(),
        ));
    }

    // Upload image to S3
    let profile_image_url = upload_profile_image_to_s3(base64_data, &user_principal.to_text())
        .await
        .map_err(|e| {
            tracing::error!("Failed to upload profile image: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to upload profile image: {e}"),
            )
        })?;

    // Update the user's profile in the User Info Service canister
    let user_agent = get_agent_from_delegated_identity_wire(&request.delegated_identity_wire)
        .await
        .map_err(|e| {
            tracing::error!("Failed to create user agent: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create user agent: {e}"),
            )
        })?;

    let user_info_service = UserInfoService(*USER_INFO_SERVICE_CANISTER_ID, &user_agent);

    let update_details = ProfileUpdateDetails {
        profile_picture_url: Some(profile_image_url.clone()),
        bio: None,
        website_url: None,
    };

    match user_info_service
        .update_profile_details(update_details)
        .await
    {
        Ok(yral_canisters_client::user_info_service::Result_::Ok) => {
            tracing::info!(
                "Successfully updated profile image for user {} in canister: {}",
                user_principal,
                profile_image_url
            );
        }
        Ok(yral_canisters_client::user_info_service::Result_::Err(e)) => {
            tracing::error!("Failed to update profile in canister: {}", e);
            if e.contains("not authorized") || e.contains("Not authorized") {
                return Err((
                    StatusCode::FORBIDDEN,
                    "Not authorized to update profile".to_string(),
                ));
            }
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to update profile in canister: {e}"),
            ));
        }
        Err(e) => {
            tracing::error!("Failed to update profile in canister: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to update profile in canister: {e}"),
            ));
        }
    }

    Ok(Json(UploadProfileImageResponse { profile_image_url }))
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

    // Set Sentry user context for tracking
    crate::middleware::set_user_context(user_principal);

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
