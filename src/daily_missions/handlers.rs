use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::{path, ToSchema};
use yral_canisters_client::daily_missions;

use crate::app_state::AppState;
use crate::types::DelegatedIdentityWire;
use crate::utils::delegated_identity::get_user_info_from_delegated_identity_wire;

#[derive(Serialize, Deserialize, ToSchema)]
pub struct FulfillClaimRequest {
    pub delegated_identity_wire: DelegatedIdentityWire,
    pub reward_id: String,
}

#[utoipa::path(
    post,
    path = "/daily-missions/claim",
    request_body = FulfillClaimRequest,
    summary = "Fulfill a daily mission claim",
    description = "Fulfill a daily mission claim for a user using delegated identity",
    responses = (
        (200, description = "Claim fulfilled"),
        (400, description = "Invalid request"),
        (401, description = "Authentication failed"),
        (404, description = "User not found"),
        (500, description = "Internal server error"),
    )
)]
pub async fn fulfill_claim(
    State(state): State<Arc<AppState>>,
    Json(request): Json<FulfillClaimRequest>,
) -> impl IntoResponse {
    // Extract user info from delegated identity
    let user_info = match get_user_info_from_delegated_identity_wire(
        &state,
        request.delegated_identity_wire,
    )
    .await
    {
        Ok(info) => info,
        Err(e) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "error": format!("Authentication failed: {}", e)
                })),
            )
                .into_response();
        }
    };

    let user_principal = user_info.user_principal;
    let user_canister = user_info.user_canister;
    

    // TODO: Implement actual daily mission claim logic
    // This would typically involve:
    // 1. Validating the user has completed the mission
    // 2. Calling the daily missions canister to fulfill the claim
    // 3. Updating any necessary state

    // For now, return success with user info
    // (
    //     StatusCode::OK,
    //     Json(json!({
    //         "message": "Claim fulfilled successfully",
    //         "user_principal": user_principal.to_string(),
    //         "user_canister": user_canister.to_string()
    //     })),
    // )
    //     .into_response()
    
}
