use std::collections::HashSet;
use std::env;

use axum::http::HeaderMap;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

#[allow(clippy::result_large_err)]
pub fn check_auth_grpc(req: Request<()>) -> Result<Request<()>, Status> {
    let mut grpc_token = env::var("GRPC_AUTH_TOKEN").expect("GRPC_AUTH_TOKEN is required");
    let mut yral_cloudflare_workers_grpc_auth_token =
        env::var("YRAL_CLOUDFLARE_WORKER_GRPC_AUTH_TOKEN")
            .expect("YRAL_CLOUDFLARE_WORKER_GRPC_AUTH_TOKEN is required");
    grpc_token.retain(|c| !c.is_whitespace());
    yral_cloudflare_workers_grpc_auth_token.retain(|c| !c.is_whitespace());

    let token: MetadataValue<_> = format!("Bearer {grpc_token}").parse().unwrap();
    let yral_cloudflare_worker_token: MetadataValue<_> =
        format!("Bearer {yral_cloudflare_workers_grpc_auth_token}")
            .parse()
            .unwrap();

    match req.metadata().get("authorization") {
        Some(t) if token == t || t == yral_cloudflare_worker_token => Ok(req),
        _ => Err(Status::unauthenticated("No valid auth token")),
    }
}

pub fn check_auth_events(req_token: Option<String>) -> Result<(), anyhow::Error> {
    let mut token = env::var("GRPC_AUTH_TOKEN").expect("GRPC_AUTH_TOKEN is required");
    let mut yral_cloudflare_worker_token = env::var("YRAL_CLOUDFLARE_WORKER_GRPC_AUTH_TOKEN")
        .expect("YRAL_CLOUDFLARE_WORKER_GRPC_AUTH_TOKEN is required");
    token.retain(|c| !c.is_whitespace());
    yral_cloudflare_worker_token.retain(|c| !c.is_whitespace());

    match req_token {
        Some(t) if token == t || t == yral_cloudflare_worker_token => Ok(()),
        _ => Err(anyhow::anyhow!("No valid auth token")),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub aud: String,
    pub exp: usize,
}

#[allow(dead_code)]
pub fn verify_jwt(
    public_key_pem: &str,
    aud: String,
    jwt: &str,
) -> Result<(), jsonwebtoken::errors::Error> {
    let mut validation = Validation::default();
    validation.aud = Some(HashSet::from([aud]));
    validation.algorithms = vec![Algorithm::EdDSA];

    jsonwebtoken::decode::<Claims>(
        jwt,
        &DecodingKey::from_ed_pem(public_key_pem.as_bytes())?,
        &validation,
    )?;

    Ok(())
}

#[allow(dead_code)]
pub fn verify_jwt_from_header(
    public_key_pem: &str,
    aud: String,
    headers: &HeaderMap,
) -> Result<(), (String, u16)> {
    let jwt = headers
        .get("Authorization")
        .ok_or_else(|| ("missing Authorization header".to_string(), 401))?;

    let jwt = jwt
        .to_str()
        .map_err(|_| ("invalid Authorization header".to_string(), 401))?;

    if !jwt.starts_with("Bearer ") {
        return Err(("invalid Authorization header".to_string(), 401));
    }

    let jwt = &jwt[7..];
    verify_jwt(public_key_pem, aud, jwt).map_err(|_| ("invalid JWT".to_string(), 401))
}
