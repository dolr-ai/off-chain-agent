use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::{path, ToSchema};
use yral_canisters_client::daily_missions::{ClaimRewardRequest, DailyMissions};
use yral_canisters_common::utils::token::{
    SatsOperations, TokenOperations, TokenOperationsProvider,
};

use crate::app_state::AppState;
use crate::consts::DAILY_MISSIONS_CANISTER_ID;
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
    description = "Fulfill a daily mission claim for a user using delegated identity and transfer tokens to user's wallet",
    tag = "daily-missions",
    responses(
        (status = 200, description = "Claim fulfilled and tokens transferred"),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Authentication failed"),
        (status = 404, description = "User not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn fulfill_claim(
    State(state): State<Arc<AppState>>,
    Json(request): Json<FulfillClaimRequest>,
) -> impl IntoResponse {
    // Extract user info from delegated identity
    let user_info =
        match get_user_info_from_delegated_identity_wire(&state, request.delegated_identity_wire)
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

    // Create the daily missions canister client
    let daily_missions_client = DailyMissions(*DAILY_MISSIONS_CANISTER_ID, &state.agent);

    // Create claim reward request
    let claim_request = ClaimRewardRequest {
        reward_id: request.reward_id.clone(),
    };

    // Call claim_reward method on the daily missions canister
    let claim_response = match daily_missions_client.claim_reward(claim_request).await {
        Ok(response) => {
            if !response.success {
                log::warn!(
                    "Failed to claim reward {} for user {}: {}",
                    request.reward_id,
                    user_principal,
                    response.message
                );

                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "success": false,
                        "error": response.message
                    })),
                )
                    .into_response();
            }
            response
        }
        Err(e) => {
            log::error!(
                "Canister call failed when claiming reward {} for user {}: {}",
                request.reward_id,
                user_principal,
                e
            );

            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "success": false,
                    "error": "Failed to communicate with daily missions canister"
                })),
            )
                .into_response();
        }
    };

    // If there's a reward amount, transfer tokens to user's wallet
    let token_transfer_result = if let Some(reward_amount) = claim_response.reward_amount {
        if reward_amount > 0 {
            // Create token operations provider for YRAL tokens
            // Using YRAL tokens for daily mission rewards - you can change this if needed
            let jwt_token = std::env::var("YRAL_HON_WORKER_JWT").ok();
            let token_ops = TokenOperationsProvider::Sats(SatsOperations::new(jwt_token));

            match token_ops.add_balance(user_principal, reward_amount).await {
                Ok(_) => {
                    log::info!(
                        "Successfully transferred {} YRAL tokens to user {} for reward {}",
                        reward_amount,
                        user_principal,
                        request.reward_id
                    );
                    Some(json!({
                        "success": true,
                        "amount": reward_amount,
                        "token_type": "YRAL"
                    }))
                }
                Err(e) => {
                    log::error!(
                        "Failed to transfer {} YRAL tokens to user {} for reward {}: {:?}",
                        reward_amount,
                        user_principal,
                        request.reward_id,
                        e
                    );
                    // Note: We still return success for the claim since it was successful,
                    // but indicate the token transfer failed
                    Some(json!({
                        "success": false,
                        "error": format!("Token transfer failed: {:?}", e),
                        "amount": reward_amount,
                        "token_type": "YRAL"
                    }))
                }
            }
        } else {
            None // No tokens to transfer
        }
    } else {
        None // No reward amount in response
    };

    log::info!(
        "Successfully claimed reward {} for user {}: {}",
        request.reward_id,
        user_principal,
        claim_response.message
    );

    (
        StatusCode::OK,
        Json(json!({
            "success": true,
            "message": claim_response.message,
            "reward_amount": claim_response.reward_amount,
            "claimed_reward": claim_response.claimed_reward,
            "token_transfer": token_transfer_result,
            "user_principal": user_principal.to_string(),
            "user_canister": user_canister.to_string()
        })),
    )
        .into_response()
}
