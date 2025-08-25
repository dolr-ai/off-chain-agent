use std::sync::Arc;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{
    app_state::AppState,
    videogen::{handlers, handlers_v2},
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
