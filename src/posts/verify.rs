use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use candid::Principal;
use serde::{Deserialize, Serialize};

use crate::{
    app_state::AppState, utils::delegated_identity::get_user_info_from_delegated_identity_wire,
};

use super::PostRequest;

#[derive(Clone, Serialize, Deserialize)]
pub struct VerifiedPostRequest<T> {
    pub user_principal: Principal,
    pub user_canister: Principal,
    pub request: PostRequest<T>,
}

pub async fn verify_post_request<T>(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode>
where
    T: for<'de> Deserialize<'de> + Serialize + Clone + Send + Sync + 'static,
{
    // Extract the JSON body
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };

    // Parse the JSON
    let post_request: PostRequest<T> = match serde_json::from_slice(&bytes) {
        Ok(req) => req,
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };

    let user_info = get_user_info_from_delegated_identity_wire(
        &state,
        post_request.delegated_identity_wire.clone(),
    )
    .await
    .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let user_principal = user_info.user_principal;
    let user_canister = user_info.user_canister;

    // Create a verified request with all the necessary context
    let verified_request = VerifiedPostRequest {
        user_principal,
        user_canister,
        request: post_request,
    };

    let request_body = serde_json::to_string(&verified_request).unwrap();
    let request = Request::from_parts(parts, axum::body::Body::from(request_body));

    // Pass the request to the next handler
    Ok(next.run(request).await)
}
