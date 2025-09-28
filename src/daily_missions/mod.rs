pub mod handlers;
pub mod types;
pub mod utils;

pub use types::*;

use std::sync::Arc;

use utoipa_axum::{router::OpenApiRouter, routes};

use crate::app_state::AppState;

pub fn daily_missions_router(state: Arc<AppState>) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(handlers::fulfill_claim))
        .with_state(state)
}
