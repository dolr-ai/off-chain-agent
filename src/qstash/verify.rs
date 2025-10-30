use axum::{
    body::Body,
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use http::{HeaderMap, StatusCode};
use http_body_util::BodyExt;
use k256::sha2::{Digest, Sha256};
use serde::{Deserialize, Serialize};

use super::QStashState;

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    exp: usize,
    iat: usize,
    nbf: isize,
    jti: String,
    body: String,
}

#[allow(dead_code)]
pub async fn verify_qstash_message(
    State(state): State<QStashState>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let sig = headers
        .get("Upstash-Signature")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let sig_str = sig.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;

    let jwt = jsonwebtoken::decode::<Claims>(sig_str, &state.decoding_key, &state.validation)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    let (parts, body) = request.into_parts();

    let sig_body_hash = URL_SAFE
        .decode(jwt.claims.body)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    let body_raw = body
        .collect()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .to_bytes();
    let derived_hash = Sha256::digest(&body_raw);

    if derived_hash.as_slice() != sig_body_hash.as_slice() {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let new_req = Request::from_parts(parts, Body::from(body_raw));

    Ok(next.run(new_req).await)
}
