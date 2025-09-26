use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use candid::Principal;
use serde::{Deserialize, Serialize};
use tracing::instrument;
use utoipa::ToSchema;

use crate::{
    app_state::AppState, consts::USER_INFO_SERVICE_CANISTER_ID, events::types::{EventPayload, FollowUserPayload}, types::DelegatedIdentityWire,
    user::utils::get_agent_from_delegated_identity_wire,
    utils::delegated_identity::get_user_info_from_delegated_identity_wire,
};
use yral_canisters_client::user_info_service::UserInfoService;

#[derive(Serialize, Deserialize, ToSchema)]
pub struct FollowUserRequest {
    pub delegated_identity_wire: DelegatedIdentityWire,
    #[schema(value_type = String)]
    pub target_principal: Principal,
    pub follower_username: Option<String>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct FollowUserResponse {
    pub success: bool,
}

#[utoipa::path(
    post,
    path = "/follow",
    request_body = FollowUserRequest,
    tag = "user",
    responses(
        (status = 200, description = "Follow successful", body = FollowUserResponse),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    )
)]
#[instrument(skip(state, request))]
pub async fn handle_follow_user(
    State(state): State<Arc<AppState>>,
    Json(request): Json<FollowUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // 1. Verify the user identity and get user info
    let user_info =
        get_user_info_from_delegated_identity_wire(&state, request.delegated_identity_wire.clone())
            .await
            .map_err(|e| {
                (
                    StatusCode::UNAUTHORIZED,
                    format!("Failed to get user info: {e}"),
                )
            })?;

    let follower_principal = user_info.user_principal;

    // Don't allow users to follow themselves
    if follower_principal == request.target_principal {
        return Err((
            StatusCode::BAD_REQUEST,
            "Cannot follow yourself".to_string(),
        ));
    }

    // 2. Get agent for canister call
    let user_agent = get_agent_from_delegated_identity_wire(&request.delegated_identity_wire)
        .await
        .map_err(|e| {
            tracing::error!("Failed to create user agent: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create user agent: {e}"),
            )
        })?;

    // 3. Call user_info_service.follow_user()
    let user_info_service = UserInfoService(*USER_INFO_SERVICE_CANISTER_ID, &user_agent);

    match user_info_service.follow_user(request.target_principal).await {
        Ok(yral_canisters_client::user_info_service::Result_::Ok) => {
            tracing::info!(
                "User {} successfully followed {}",
                follower_principal,
                request.target_principal
            );

            // 4. Send notification event
            let follow_payload = FollowUserPayload {
                follower_principal_id: follower_principal,
                follower_username: request.follower_username,
                followee_principal_id: request.target_principal,
            };

            // Send notification asynchronously
            let event_payload = EventPayload::FollowUser(follow_payload);
            event_payload.send_notification(&state).await;

            // 5. Return success
            Ok(Json(FollowUserResponse { success: true }))
        }
        Ok(yral_canisters_client::user_info_service::Result_::Err(e)) => {
            tracing::error!("Failed to follow user: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to follow user: {e}"),
            ))
        }
        Err(e) => {
            tracing::error!("Network error following user: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Network error following user: {e}"),
            ))
        }
    }
}