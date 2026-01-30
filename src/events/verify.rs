use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use candid::Principal;
use yral_metrics::metrics::sealed_metric::SealedMetric;

use crate::{
    app_state::AppState,
    events::{EventBulkRequestV2, VerifiedEventBulkRequestV2},
    utils::delegated_identity::get_user_info_from_delegated_identity_wire,
};

use super::{EventBulkRequest, VerifiedEventBulkRequest};

pub async fn verify_event_bulk_request(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    // Extract the JSON body
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Failed to parse request body #1: {e}"),
            ))
        }
    };

    // Parse the JSON
    let event_bulk_request: EventBulkRequest = match serde_json::from_slice(&bytes) {
        Ok(req) => req,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Failed to parse request body to EventBulkRequest: {e}"),
            ))
        }
    };

    let user_info = get_user_info_from_delegated_identity_wire(
        &state,
        event_bulk_request.delegated_identity_wire.clone(),
    )
    .await
    .map_err(|e| {
        (
            StatusCode::UNAUTHORIZED,
            format!("Failed to get user info: {e}"),
        )
    })?;
    let user_principal = user_info.user_principal;
    let user_canister = user_info.user_canister;

    // Set Sentry user context for tracking
    crate::middleware::set_user_context(user_principal);

    // verify all events are valid
    for event in event_bulk_request.events.clone() {
        if event.user_canister().unwrap_or(Principal::anonymous()) != user_canister {
            return Err((StatusCode::BAD_REQUEST, "Invalid user canister".to_string()));
        }
        if event.user_id().unwrap_or_default() != user_principal.to_string() {
            return Err((StatusCode::BAD_REQUEST, "Invalid user id".to_string()));
        }
    }

    let verified_request = VerifiedEventBulkRequest {
        events: event_bulk_request.events,
    };

    let request_body = serde_json::to_string(&verified_request).unwrap();
    let request = Request::from_parts(parts, axum::body::Body::from(request_body));

    // Pass the request to the next handler
    Ok(next.run(request).await)
}

pub async fn verify_event_bulk_request_v3(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    // Extract the JSON body
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Failed to parse request body: {e}"),
            ))
        }
    };

    // Parse the JSON
    let event_bulk_request: EventBulkRequestV2 = match serde_json::from_slice(&bytes) {
        Ok(req) => req,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Failed to parse request body to EventBulkRequestV2: {e}"),
            ))
        }
    };

    let user_info = get_user_info_from_delegated_identity_wire(
        &state,
        event_bulk_request.delegated_identity_wire.clone(),
    )
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

    let user_principal_str = user_principal.to_string();
    for event in &event_bulk_request.events {
        if let Some(event_user_id) = event.get("user_id").and_then(|v| v.as_str()) {
            if event_user_id != user_principal_str {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "Invalid user_id: does not match authenticated principal".to_string(),
                ));
            }
        }
    }

    let verified_request = VerifiedEventBulkRequestV2 {
        events: event_bulk_request.events,
        user_id: user_principal.to_string(),
    };

    let request_body = serde_json::to_string(&verified_request).unwrap();
    let request = Request::from_parts(parts, axum::body::Body::from(request_body));

    // Pass the request to the next handler
    Ok(next.run(request).await)
}
