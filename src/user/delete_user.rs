use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use utoipa::ToSchema;

use crate::{
    app_state::AppState, types::DelegatedIdentityWire,
    utils::delegated_identity::get_user_info_from_delegated_identity_wire,
};

use super::utils::get_agent_from_delegated_identity_wire;

#[derive(Serialize, Deserialize, ToSchema)]
pub struct DeleteUserRequest {
    pub delegated_identity_wire: DelegatedIdentityWire,
}

#[utoipa::path(
    delete,
    path = "/",
    request_body = DeleteUserRequest,
    tag = "user",
    responses(
        (status = 200, description = "User deleted successfully"),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    )
)]
#[instrument(skip(state, request))]
pub async fn handle_delete_user(
    State(state): State<Arc<AppState>>,
    Json(request): Json<DeleteUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let user_info =
        get_user_info_from_delegated_identity_wire(&state, request.delegated_identity_wire.clone())
            .await
            .map_err(|e| {
                (
                    StatusCode::UNAUTHORIZED,
                    format!("Failed to get user info: {}", e),
                )
            })?;

    let user_principal = user_info.user_principal;
    let user_canister = user_info.user_canister;

    let agent = get_agent_from_delegated_identity_wire(&request.delegated_identity_wire)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Use the common delete_canister_data function for steps 1-7

    #[cfg(not(any(feature = "local-bin", feature = "use-local-agent")))]
    {
        use crate::canister::delete_canister_data;

        delete_canister_data(&agent, &state, user_canister, user_principal, true)
            .await
            .map_err(|e| {
                log::error!("Failed to delete canister data: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to delete canister data: {}", e),
                )
            })?;

        // Step 8: Add to deleted canisters in SpaceTimeDB if user_principal is provided

        if let Err(e) = state
            .canisters_ctx
            .add_deleted_canister(user_canister, user_principal)
            .await
        {
            log::error!("Failed to add deleted canister to SpaceTimeDB: {}", e);
            // Don't fail the operation if SpaceTimeDB call fails
        }
    }

    Ok((StatusCode::OK, "User deleted successfully".to_string()))
}
