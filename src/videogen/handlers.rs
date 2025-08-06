use axum::{
    extract::State,
    http::StatusCode,
    Json,
};
use std::sync::Arc;
use videogen_common::VideoGenerator;

use super::qstash_types::QstashVideoGenRequest;
use super::rate_limit::verify_rate_limit_and_create_request_v1;
use super::signature::verify_videogen_request;
use super::token_operations::{
    add_token_balance, deduct_balance_with_cleanup, get_model_cost,
};
use crate::app_state::AppState;
use crate::consts::OFF_CHAIN_AGENT_URL;
use crate::utils::gcs::maybe_upload_image_to_gcs;
use cloud_storage::Client;

// /// Generate a video using the specified provider
// #[utoipa::path(
//     post,
//     path = "/generate",
//     request_body = videogen_common::VideoGenRequest,
//     responses(
//         (status = 200, description = "Video generation started successfully", body = videogen_common::VideoGenQueuedResponse),
//         (status = 400, description = "Invalid input", body = videogen_common::VideoGenError),
//         (status = 401, description = "Authentication failed - Bearer token required", body = videogen_common::VideoGenError),
//         (status = 429, description = "Rate limit exceeded", body = videogen_common::VideoGenError),
//         (status = 502, description = "Provider error", body = videogen_common::VideoGenError),
//         (status = 503, description = "Service unavailable", body = videogen_common::VideoGenError),
//     ),
//     security(
//         ("Bearer" = [])
//     ),
//     tag = "VideoGen"
// )]
// pub async fn generate_video(
//     State(app_state): State<Arc<AppState>>,
//     headers: HeaderMap,
//     Json(request): Json<videogen_common::VideoGenRequest>,
// ) -> Result<
//     Json<videogen_common::VideoGenQueuedResponse>,
//     (StatusCode, Json<videogen_common::VideoGenError>),
// > {
//     // Verify JWT token
//     let jwt_public_key = std::env::var("JWT_PUBLIC_KEY_PEM").map_err(|_| {
//         (
//             StatusCode::INTERNAL_SERVER_ERROR,
//             Json(videogen_common::VideoGenError::AuthError),
//         )
//     })?;

//     let jwt_aud = std::env::var("JWT_AUD").map_err(|_| {
//         (
//             StatusCode::INTERNAL_SERVER_ERROR,
//             Json(videogen_common::VideoGenError::AuthError),
//         )
//     })?;

//     verify_jwt_from_header(&jwt_public_key, jwt_aud, &headers).map_err(|(_, status)| {
//         (
//             StatusCode::from_u16(status).unwrap_or(StatusCode::UNAUTHORIZED),
//             Json(videogen_common::VideoGenError::AuthError),
//         )
//     })?;

//     log::info!(
//         "agent princ {:?}",
//         app_state.agent.get_principal().unwrap().to_text()
//     );

//     // Verify rate limit and create request atomically
//     let model_name = request.input.model_name();
//     let prompt = request.input.get_prompt();
//     let property = if model_name == "INTTEST" {
//         "VIDEOGEN_INTTEST"
//     } else {
//         "VIDEOGEN"
//     };

//     let request_key = verify_rate_limit_and_create_request(
//         request.principal,
//         model_name,
//         prompt,
//         property,
//         &app_state,
//     )
//     .await
//     .map_err(|(status, error)| (status, Json(error)))?;

//     // Get provider before moving input
//     let provider = request.input.provider();

//     // Queue to Qstash
//     let qstash_request = QstashVideoGenRequest {
//         user_principal: request.principal,
//         input: request.input,
//         request_key: request_key.clone(),
//         property: property.to_string(),
//         deducted_amount: None, // No balance deduction for JWT-based auth
//         token_type: request.token_type.clone(),
//     };

//     let callback_url = OFF_CHAIN_AGENT_URL
//         .join("qstash/video_gen_callback")
//         .map_err(|e| {
//             (
//                 StatusCode::INTERNAL_SERVER_ERROR,
//                 Json(videogen_common::VideoGenError::NetworkError(format!(
//                     "Failed to construct callback URL: {}",
//                     e
//                 ))),
//             )
//         })?
//         .to_string();

//     app_state
//         .qstash_client
//         .queue_video_generation(&qstash_request, &callback_url)
//         .await
//         .map_err(|e| {
//             (
//                 StatusCode::SERVICE_UNAVAILABLE,
//                 Json(videogen_common::VideoGenError::NetworkError(format!(
//                     "Failed to queue video generation: {}",
//                     e
//                 ))),
//             )
//         })?;

//     // Return polling response
//     Ok(Json(videogen_common::VideoGenQueuedResponse {
//         operation_id: format!("{}_{}", request_key.principal, request_key.counter),
//         provider: provider.to_string(),
//         request_key: videogen_common::VideoGenRequestKey {
//             principal: request_key.principal,
//             counter: request_key.counter,
//         },
//     }))
// }

/// Generate a video using a signed request with signature verification and balance deduction
#[utoipa::path(
    post,
    path = "/generate_signed",
    request_body = videogen_common::VideoGenRequestWithSignature,
    responses(
        (status = 200, description = "Video generation started successfully", body = videogen_common::VideoGenQueuedResponse),
        (status = 400, description = "Invalid input", body = videogen_common::VideoGenError),
        (status = 401, description = "Authentication failed - Invalid signature", body = videogen_common::VideoGenError),
        (status = 402, description = "Insufficient balance", body = videogen_common::VideoGenError),
        (status = 429, description = "Rate limit exceeded", body = videogen_common::VideoGenError),
        (status = 502, description = "Provider error", body = videogen_common::VideoGenError),
        (status = 503, description = "Service unavailable", body = videogen_common::VideoGenError),
    ),
    tag = "VideoGen"
)]

pub async fn generate_video_signed(
    State(app_state): State<Arc<AppState>>,
    Json(signed_request): Json<videogen_common::VideoGenRequestWithSignature>,
) -> Result<
    Json<videogen_common::VideoGenQueuedResponse>,
    (StatusCode, Json<videogen_common::VideoGenError>),
> {
    let user_principal = signed_request.request.principal;

    // Verify signature
    verify_videogen_request(user_principal, &signed_request)
        .map_err(|e| (StatusCode::UNAUTHORIZED, Json(e)))?;

    log::info!("Signature verified for user {user_principal}");

    // Get token type and model info
    let model_name = signed_request.request.input.model_name();
    let prompt = signed_request.request.input.get_prompt();
    let token_type = &signed_request.request.token_type;
    let property = if model_name == "INTTEST" {
        "VIDEOGEN_INTTEST"
    } else {
        "VIDEOGEN"
    };

    // Get provider before moving input
    let provider = signed_request.request.input.provider();

    // Get the cost based on model and token type
    let cost = get_model_cost(model_name, token_type);

    // Determine the payment amount for rate limit check
    let payment_amount = if matches!(token_type, videogen_common::TokenType::Free) {
        None
    } else {
        Some(cost)
    };

    // First verify rate limit with v1 function
    let request_key = verify_rate_limit_and_create_request_v1(
        user_principal,
        model_name,
        prompt,
        property,
        token_type,
        payment_amount,
        &app_state,
    )
    .await
    .map_err(|(status, error)| (status, Json(error)))?;

    // Get JWT token from environment variable
    let jwt_token = std::env::var("YRAL_HON_WORKER_JWT").map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(videogen_common::VideoGenError::AuthError),
        )
    })?;

    // Handle balance deduction with automatic cleanup on failure
    let deducted_amount = deduct_balance_with_cleanup(
        user_principal,
        cost,
        token_type,
        jwt_token.clone(),
        &app_state.agent,
        &request_key,
        property,
    )
    .await?;

    // Process image if present - upload large images to GCS
    let mut input = signed_request.request.input;
    process_input_image(
        &mut input,
        app_state.gcs_client.clone(),
        &user_principal.to_string(),
    )
    .await?;

    // Queue to Qstash with potentially updated input (image uploaded to GCS)
    let qstash_request = QstashVideoGenRequest {
        user_principal: signed_request.request.principal,
        input, // Use the potentially modified input
        request_key: request_key.clone(),
        property: property.to_string(),
        deducted_amount, // Now it's Option<u64> - None for free, Some for paid
        token_type: signed_request.request.token_type,
    };

    let callback_url = OFF_CHAIN_AGENT_URL
        .join("qstash/video_gen_callback")
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(videogen_common::VideoGenError::NetworkError(format!(
                    "Failed to construct callback URL: {e}"
                ))),
            )
        })?
        .to_string();

    match app_state
        .qstash_client
        .queue_video_generation(&qstash_request, &callback_url)
        .await
    {
        Ok(()) => {
            // Successfully queued
        }
        Err(e) => {
            // Failed to queue - rollback balance
            log::error!(
                "Failed to queue video generation: {e}. Rolling back balance."
            );

            if let Some(deducted_amount) = qstash_request.deducted_amount {
                let jwt_opt = match &qstash_request.token_type {
                    videogen_common::TokenType::Sats => Some(jwt_token.clone()),
                    videogen_common::TokenType::Dolr => None,
                    videogen_common::TokenType::Free => None, // Should not reach here
                };

                let agent =
                    if matches!(&qstash_request.token_type, videogen_common::TokenType::Dolr) {
                        Some(app_state.agent.clone())
                    } else {
                        None
                    };

                if let Err(rollback_err) = add_token_balance(
                    user_principal,
                    deducted_amount,
                    &qstash_request.token_type,
                    jwt_opt,
                    agent,
                )
                .await
                {
                    log::error!(
                        "Failed to rollback {} {:?} for user {}: {}",
                        deducted_amount,
                        qstash_request.token_type,
                        user_principal,
                        rollback_err
                    );
                }
            }

            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(videogen_common::VideoGenError::NetworkError(format!(
                    "Failed to queue video generation: {e}"
                ))),
            ));
        }
    }

    // Return polling response
    Ok(Json(videogen_common::VideoGenQueuedResponse {
        operation_id: format!("{}_{}", request_key.principal, request_key.counter),
        provider: provider.to_string(),
        request_key: videogen_common::VideoGenRequestKey {
            principal: request_key.principal,
            counter: request_key.counter,
        },
    }))
}

/// Helper function to process images in VideoGenInput
/// Uploads large images to GCS and replaces them with URLs
async fn process_input_image(
    input: &mut videogen_common::VideoGenInput,
    gcs_client: Arc<Client>,
    user_principal: &str,
) -> Result<(), (StatusCode, Json<videogen_common::VideoGenError>)> {
    // Get mutable reference to image if it exists
    if let Some(image_data) = input.get_image_mut() {
        // Process the image and update it in place
        *image_data = maybe_upload_image_to_gcs(gcs_client, image_data.clone(), user_principal)
            .await
            .map_err(|e| {
                log::error!("Failed to upload image to GCS: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(videogen_common::VideoGenError::NetworkError(format!(
                        "Failed to upload image: {e}"
                    ))),
                )
            })?;
    }

    Ok(())
}
