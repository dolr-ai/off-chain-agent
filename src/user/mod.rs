pub mod delete_user;
pub mod migrate_user;
pub mod utils;

use std::sync::Arc;

use utoipa_axum::{router::OpenApiRouter, routes};

use crate::app_state::AppState;

pub fn user_router(state: Arc<AppState>) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(delete_user::handle_delete_user))
        .routes(routes!(migrate_user::handle_user_migration))
        .with_state(state)
}
