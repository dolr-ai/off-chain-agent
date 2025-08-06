use std::sync::Arc;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{app_state::AppState, videogen::handlers};

pub fn videogen_router<S>(state: Arc<AppState>) -> OpenApiRouter<S> {
    OpenApiRouter::new()
        // .routes(routes!(handlers::generate_video)) // Commented out as per user request
        .routes(routes!(handlers::generate_video_signed))
        .with_state(state)
}
