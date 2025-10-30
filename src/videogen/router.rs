use std::sync::Arc;
use utoipa_axum::{router::OpenApiRouter, routes};
use axum::{routing::post, Router};

use crate::{
    app_state::AppState,
    videogen::{handlers, handlers_v2, replicate_webhook},
};

/// V1 API routes for video generation
pub fn videogen_router<S>(state: Arc<AppState>) -> OpenApiRouter<S> {
    OpenApiRouter::new()
        .routes(routes!(handlers::generate_video_with_identity))
        .with_state(state)
}

/// V2 API routes for video generation
pub fn videogen_router_v2<S>(state: Arc<AppState>) -> OpenApiRouter<S> {
    OpenApiRouter::new()
        .routes(routes!(handlers_v2::get_providers))
        .routes(routes!(handlers_v2::get_providers_all))
        .routes(routes!(handlers_v2::generate_video_with_identity_v2))
        .with_state(state)
}

/// Replicate webhook router - separate from API docs since it's an internal endpoint
pub fn replicate_webhook_router(state: Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route("/webhook", post(replicate_webhook::handle_replicate_webhook))
        .with_state(state)
}
