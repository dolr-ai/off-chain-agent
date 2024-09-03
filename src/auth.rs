use std::collections::HashSet;
use std::env;

use axum::extract::FromRequestParts;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{async_trait, Json};
use http::request::Parts;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

#[derive(Debug)]
pub enum AuthError {
    WrongCredentials,
    MissingCredentials,
    TokenCreation,
    InvalidToken,
}

pub struct AuthBearer(pub String);

#[async_trait]
impl<S> FromRequestParts<S> for AuthBearer
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Extract the token from the authorization header
        let bearer = parts.headers.get("Authorization");
        if bearer.is_none() {
            return Err(AuthError::MissingCredentials);
        }

        let bearer = bearer.unwrap().to_str().unwrap();
        let bearer = bearer.split("Bearer ");
        let bearer = bearer.last().unwrap();

        Ok(AuthBearer(bearer.to_string()))
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AuthError::WrongCredentials => (StatusCode::UNAUTHORIZED, "Wrong credentials"),
            AuthError::MissingCredentials => (StatusCode::BAD_REQUEST, "Missing credentials"),
            AuthError::TokenCreation => (StatusCode::INTERNAL_SERVER_ERROR, "Token creation error"),
            AuthError::InvalidToken => (StatusCode::BAD_REQUEST, "Invalid token"),
        };
        let body = Json(json!({
            "error": error_message,
        }));
        (status, body).into_response()
    }
}

pub fn check_auth_grpc(req: Request<()>) -> Result<Request<()>, Status> {
    let mut grpc_token = env::var("GRPC_AUTH_TOKEN").expect("GRPC_AUTH_TOKEN is required");
    grpc_token.retain(|c| !c.is_whitespace());
    let token: MetadataValue<_> = format!("Bearer {}", grpc_token).parse().unwrap();

    match req.metadata().get("authorization") {
        Some(t) if token == t => Ok(req),
        _ => Err(Status::unauthenticated("No valid auth token")),
    }
}

pub fn check_auth_grpc_test(req: Request<()>) -> Result<Request<()>, Status> {
    Ok(req)
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct MLFeedClaims {
    pub sub: String,
    pub company: String,
}

pub fn check_auth_grpc_offchain_mlfeed(req: Request<()>) -> Result<Request<()>, Status> {
    let token = req
        .metadata()
        .get("authorization")
        .ok_or(Status::unauthenticated("No valid auth token"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("Invalid auth token"))?
        .trim_start_matches("Bearer ");

    let mlfeed_public_key =
        env::var("MLFEED_JWT_PUBLIC_KEY").expect("MLFEED_JWT_PUBLIC_KEY is required");

    let decoding_key = DecodingKey::from_ed_pem(mlfeed_public_key.as_bytes())
        .expect("failed to create decoding key");

    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.required_spec_claims = HashSet::new();
    validation.validate_exp = false;

    let token_message =
        decode::<MLFeedClaims>(token, &decoding_key, &validation).expect("failed to decode token");

    let claims = token_message.claims;
    if claims.sub != "yral-ml-feed-server" || claims.company != "gobazzinga" {
        return Err(Status::unauthenticated("Invalid auth token"));
    }

    Ok(req)
}
