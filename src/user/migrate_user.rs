use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use utoipa::ToSchema;

use crate::{
    app_state::AppState, qstash::service_canister_migration::MigrateIndividualUserRequest,
    types::DelegatedIdentityWire,
    utils::delegated_identity::get_user_info_from_delegated_identity_wire,
};

use super::utils::get_agent_from_delegated_identity_wire;

#[derive(Serialize, Deserialize, ToSchema, Clone)]
pub struct MigrateIndividualUserRequestSchema {
    pub user_canister: String,
    pub user_principal: String,
}

impl From<MigrateIndividualUserRequest> for MigrateIndividualUserRequestSchema {
    fn from(request: MigrateIndividualUserRequest) -> Self {
        Self {
            user_canister: request.user_canister.to_text(),
            user_principal: request.user_principal.to_text(),
        }
    }
}

#[utoipa::path(
    post,
    path = "/start_user_migration",
    request_body = MigrateIndividualUserRequestSchema,
    tag = "user",
    responses(
        (status = 200, description = "Accept Request to Migrate Individual User"),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    )
)]
#[instrument(skip(state, request))]
pub async fn handle_user_migration(
    State(state): State<Arc<AppState>>,
    Json(request): Json<MigrateIndividualUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    state
        .qstash_client
        .migrate_individual_user_to_service_canister(&request)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::OK,
        "User migration request accepted".to_string(),
    ))
}
