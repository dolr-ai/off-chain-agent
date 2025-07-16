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
    app_state::AppState, utils::delegated_identity::get_user_info_from_delegated_identity_wire,
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
                format!("Failed to parse request body #1: {}", e),
            ))
        }
    };

    // Parse the JSON
    let event_bulk_request: EventBulkRequest = match serde_json::from_slice(&bytes) {
        Ok(req) => req,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("Failed to parse request body to EventBulkRequest: {}", e),
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
            format!("Failed to get user info: {}", e),
        )
    })?;
    let user_principal = user_info.user_principal;
    let user_canister = user_info.user_canister;

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
