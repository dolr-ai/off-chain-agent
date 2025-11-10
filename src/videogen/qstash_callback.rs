use axum::{extract::State, http::StatusCode, Json};
use base64;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::instrument;
use videogen_common::types_v2::VideoUploadHandling;
use yral_canisters_client::rate_limits::{RateLimits, VideoGenRequestKey, VideoGenRequestStatus};

use crate::{
    app_state::AppState,
    consts::RATE_LIMITS_CANISTER_ID,
    videogen::{
        qstash_types::{QstashVideoGenCallback, VideoGenCallbackResult},
        upload_ai_generated_video_to_canister_in_drafts::UploadAiVideoToCanisterRequest,
        utils::get_hon_worker_jwt_token,
    },
};

// Import utility functions for JWT and rollback
use super::utils::rollback_balance_on_failure;

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

/// Parse and validate QStash callback data
fn parse_qstash_callback(
    wrapper: &QStashCallbackWrapper,
) -> Result<QstashVideoGenCallback, (StatusCode, String)> {
    // Decode base64 body
    use base64::Engine;
    let decoded_body = base64::engine::general_purpose::STANDARD
        .decode(&wrapper.body)
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to decode body: {e}"),
            )
        })?;

    // Parse callback directly (includes deducted_amount and token_type now)
    serde_json::from_slice(&decoded_body).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("Failed to parse callback data: {e}"),
        )
    })
}

/// Determine status based on QStash response and callback result
#[allow(dead_code)]
fn determine_callback_status(
    qstash_status: u16,
    result: &VideoGenCallbackResult,
) -> (VideoGenRequestStatus, bool) {
    if qstash_status != 200 {
        // QStash request failed
        let error_message = format!("QStash request failed with status: {qstash_status}");
        (VideoGenRequestStatus::Failed(error_message), true)
    } else {
        // QStash request succeeded, check the actual result
        match result {
            VideoGenCallbackResult::Success(response) => (
                VideoGenRequestStatus::Complete(response.video_url.clone()),
                false,
            ),
            VideoGenCallbackResult::Failure(error) => {
                (VideoGenRequestStatus::Failed(error.clone()), true)
            }
        }
    }
}

/// Update status in rate limits canister
async fn update_rate_limit_status(
    rate_limits_client: &RateLimits<'_>,
    request_key: VideoGenRequestKey,
    status: VideoGenRequestStatus,
) -> Result<(), (StatusCode, String)> {
    match rate_limits_client
        .update_video_generation_status(request_key.clone(), status)
        .await
    {
        Ok(result) => match result {
            yral_canisters_client::rate_limits::Result1::Ok => {
                log::info!(
                    "Successfully updated video generation status for principal {} counter {}",
                    request_key.principal,
                    request_key.counter
                );
                Ok(())
            }
            yral_canisters_client::rate_limits::Result1::Err(e) => {
                log::error!("Failed to update video generation status: {e}");
                Err((StatusCode::INTERNAL_SERVER_ERROR, e))
            }
        },
        Err(e) => {
            log::error!("Failed to call update_video_generation_status: {e}");
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Canister call failed: {e}"),
            ))
        }
    }
}

/// Decrement rate limit counter for failed requests
async fn decrement_counter_for_failure(
    rate_limits_client: &RateLimits<'_>,
    request_key: VideoGenRequestKey,
    property: String,
) {
    log::info!(
        "Decrementing rate limit counter for failed request: principal {} counter {}",
        request_key.principal,
        request_key.counter
    );

    if let Err(e) = rate_limits_client
        .decrement_video_generation_counter_v_1(request_key, property)
        .await
    {
        log::error!("Failed to decrement counter: {e}");
        // Don't fail the callback if decrement fails
    }
}

/// Internal callback handler that can be used by both QStash and webhook handlers
pub async fn handle_video_gen_callback_internal(
    state: Arc<AppState>,
    callback: QstashVideoGenCallback,
) -> Result<StatusCode, (StatusCode, String)> {
    log::info!(
        "Processing video generation callback for principal {} counter {}",
        callback.request_key.principal,
        callback.request_key.counter
    );

    // 2. Determine status based on callback result
    let (status, should_decrement) = match &callback.result {
        VideoGenCallbackResult::Success(response) => (
            VideoGenRequestStatus::Complete(response.video_url.clone()),
            false,
        ),
        VideoGenCallbackResult::Failure(error) => {
            (VideoGenRequestStatus::Failed(error.clone()), true)
        }
    };

    // 3. Update status in rate limits canister
    let rate_limits_client = RateLimits(*RATE_LIMITS_CANISTER_ID, &state.agent);
    let request_key = VideoGenRequestKey {
        principal: callback.request_key.principal,
        counter: callback.request_key.counter,
    };

    update_rate_limit_status(&rate_limits_client, request_key.clone(), status.clone()).await?;

    // 4. Handle failure cleanup if needed
    if should_decrement {
        // Decrement counter
        decrement_counter_for_failure(&rate_limits_client, request_key, callback.property.clone())
            .await;

        // Rollback balance using our utils function
        if callback.deducted_amount.is_some() {
            match get_hon_worker_jwt_token() {
                Ok(jwt_token) => {
                    log::info!(
                        "Rolling back {} {:?} for failed video generation: principal {}",
                        callback.deducted_amount.unwrap_or(0),
                        callback.token_type,
                        callback.request_key.principal
                    );

                    if let Err(e) = rollback_balance_on_failure(
                        callback.request_key.principal,
                        callback.deducted_amount,
                        &callback.token_type,
                        jwt_token,
                        &state.agent,
                    )
                    .await
                    {
                        log::error!("Balance rollback failed: {e}");
                        // Don't fail the callback on rollback errors
                    }
                }
                Err(_) => {
                    log::error!("Cannot rollback - JWT token not available");
                }
            }
        }
    }

    if let VideoGenRequestStatus::Complete(ai_video_url) = &status {
        match callback.handle_video_upload {
            Some(VideoUploadHandling::ServerDraft) => {
                state
                    .qstash_client
                    .upload_ai_generated_video_to_canister_in_drafts(
                        UploadAiVideoToCanisterRequest {
                            ai_video_url: ai_video_url.clone(),
                            user_id: callback.request_key.principal,
                        },
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                Ok(StatusCode::OK)
            }

            _ => Ok::<_, (StatusCode, String)>(StatusCode::OK),
        }
    } else {
        Ok(StatusCode::OK)
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

    // 1. Parse callback (now includes deducted_amount and token_type)
    let callback = parse_qstash_callback(&wrapper)?;

    // For QStash callbacks, we need to determine if it should decrement based on QStash status
    let should_force_failure = wrapper.status != 200;

    let mut modified_callback = callback;
    if should_force_failure {
        // Override the result to be a failure if QStash request failed
        modified_callback.result = VideoGenCallbackResult::Failure(format!(
            "QStash request failed with status: {}",
            wrapper.status
        ));
    }

    // Use the internal handler
    handle_video_gen_callback_internal(state, modified_callback).await
}
