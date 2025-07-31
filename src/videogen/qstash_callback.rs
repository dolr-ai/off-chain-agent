use axum::{extract::State, http::StatusCode, Json};
use base64;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::instrument;
use yral_canisters_client::rate_limits::{RateLimits, VideoGenRequestKey, VideoGenRequestStatus};

use crate::{
    app_state::AppState,
    consts::RATE_LIMITS_CANISTER_ID,
    videogen::qstash_types::{QstashVideoGenCallback, VideoGenCallbackResult},
    videogen::token_operations::add_token_balance,
};

/// QStash callback wrapper structure
#[derive(Debug, Deserialize)]
pub struct QStashCallbackWrapper {
    pub status: u16,
    pub body: String, // Base64 encoded response body
    pub header: HashMap<String, Vec<String>>,
    pub retried: Option<u32>,
    #[serde(rename = "maxRetries")]
    pub max_retries: Option<u32>,
    #[serde(rename = "sourceMessageId")]
    pub source_message_id: String,
    pub url: String,
    pub method: String,
}

/// Helper function to decrement rate limit counter for failed requests
async fn decrement_counter_for_failure(
    rate_limits_client: &RateLimits<'_>,
    request_key: VideoGenRequestKey,
    property: String,
) {
    log::info!(
        "Decrementing rate limit counter for failed request: principal {} counter {} property {}",
        request_key.principal,
        request_key.counter,
        property
    );

    match rate_limits_client
        .decrement_video_generation_counter(request_key, property)
        .await
    {
        Ok(result) => match result {
            yral_canisters_client::rate_limits::Result1::Ok => {
                log::info!("Successfully decremented rate limit counter");
            }
            yral_canisters_client::rate_limits::Result1::Err(e) => {
                log::error!("Failed to decrement rate limit counter: {}", e);
                // Don't fail the callback if decrement fails
            }
        },
        Err(e) => {
            log::error!("Failed to call decrement_video_generation_counter: {}", e);
            // Don't fail the callback if decrement fails
        }
    }
}

/// Handle video generation completion callback from Qstash
#[instrument(skip(state))]
pub async fn handle_video_gen_callback(
    State(state): State<Arc<AppState>>,
    Json(wrapper): Json<QStashCallbackWrapper>,
) -> Result<StatusCode, (StatusCode, String)> {
    log::info!(
        "Received QStash callback for message: {} with status: {}",
        wrapper.source_message_id,
        wrapper.status
    );

    // Decode base64 body - we need this even for failed requests to get request details
    use base64::Engine;
    let decoded_body = base64::engine::general_purpose::STANDARD
        .decode(&wrapper.body)
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to decode body: {}", e),
            )
        })?;

    // First, try to parse the outer Qstash response which contains our callback data
    let qstash_response: serde_json::Value =
        serde_json::from_slice(&decoded_body).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to parse QStash response: {}", e),
            )
        })?;

    // Extract our callback data from the QStash response
    let callback: QstashVideoGenCallback = serde_json::from_value(qstash_response.clone())
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to parse callback data: {}", e),
            )
        })?;

    // Extract deducted_amount and token_type if present in the QStash request data
    let deducted_amount = qstash_response
        .get("deducted_amount")
        .and_then(|v| v.as_u64());

    let token_type = qstash_response
        .get("token_type")
        .and_then(|v| serde_json::from_value::<videogen_common::TokenType>(v.clone()).ok())
        .unwrap_or_default();

    log::info!(
        "Processing video generation callback for principal {} counter {}",
        callback.request_key.principal,
        callback.request_key.counter
    );

    // Create rate limits client
    let rate_limits_client = RateLimits(*RATE_LIMITS_CANISTER_ID, &state.agent);

    // Convert our key type to the canister's key type
    let canister_key = VideoGenRequestKey {
        principal: callback.request_key.principal,
        counter: callback.request_key.counter,
    };

    // Determine the status to update based on QStash response and callback result
    let (status, should_decrement) = if wrapper.status != 200 {
        // QStash request failed
        log::error!(
            "Video generation request failed with QStash status: {}",
            wrapper.status
        );
        let error_message = format!("QStash request failed with status: {}", wrapper.status);
        (VideoGenRequestStatus::Failed(error_message), true)
    } else {
        // QStash request succeeded, check the actual result
        match &callback.result {
            VideoGenCallbackResult::Success(response) => (
                VideoGenRequestStatus::Complete(response.video_url.clone()),
                false,
            ),
            VideoGenCallbackResult::Failure(error) => {
                (VideoGenRequestStatus::Failed(error.clone()), true)
            }
        }
    };

    // Update the status in the rate limits canister

    match rate_limits_client
        .update_video_generation_status(canister_key.clone(), status)
        .await
    {
        Ok(result) => match result {
            yral_canisters_client::rate_limits::Result1::Ok => {
                log::info!(
                    "Successfully updated video generation status for principal {} counter {}",
                    callback.request_key.principal,
                    callback.request_key.counter
                );

                // If the request failed (either QStash or video generation), handle cleanup
                if should_decrement {
                    // Decrement the rate limit counter
                    decrement_counter_for_failure(
                        &rate_limits_client,
                        canister_key,
                        callback.property.clone(),
                    )
                    .await;

                    // Rollback balance if we have the deducted amount
                    if let Some(deducted_amount) = deducted_amount {
                        // Get JWT token from environment variable
                        let jwt_token = match std::env::var("YRAL_HON_WORKER_JWT") {
                            Ok(token) => token,
                            Err(_) => {
                                log::error!("YRAL_HON_WORKER_JWT not set, cannot rollback balance");
                                return Ok(StatusCode::OK);
                            }
                        };

                        // Prepare auth based on token type
                        let jwt_opt = match &token_type {
                            videogen_common::TokenType::Sats => Some(jwt_token),
                            videogen_common::TokenType::Dolr => None,
                        };

                        let agent = if matches!(&token_type, videogen_common::TokenType::Dolr) {
                            Some(state.agent.clone())
                        } else {
                            None
                        };

                        log::info!(
                            "Rolling back {} {:?} for failed video generation: principal {}",
                            deducted_amount,
                            token_type,
                            callback.request_key.principal
                        );

                        if let Err(e) = add_token_balance(
                            callback.request_key.principal,
                            deducted_amount,
                            &token_type,
                            jwt_opt,
                            agent,
                        )
                        .await
                        {
                            log::error!(
                                "Failed to rollback {} {:?} for user {}: {}",
                                deducted_amount,
                                token_type,
                                callback.request_key.principal,
                                e
                            );
                        }
                    }
                }

                Ok(StatusCode::OK)
            }
            yral_canisters_client::rate_limits::Result1::Err(e) => {
                log::error!("Failed to update video generation status: {}", e);
                Err((StatusCode::INTERNAL_SERVER_ERROR, e))
            }
        },
        Err(e) => {
            log::error!("Failed to call update_video_generation_status: {}", e);
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Canister call failed: {}", e),
            ))
        }
    }
}
