pub mod delete_user;
pub mod follow;
pub mod migrate_user;
pub mod profile_image;
pub mod utils;

use std::sync::Arc;

use utoipa_axum::{router::OpenApiRouter, routes};

use crate::app_state::AppState;

pub fn user_router(state: Arc<AppState>) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(delete_user::handle_delete_user))
        .routes(routes!(
            profile_image::handle_upload_profile_image,
            profile_image::handle_delete_profile_image
        ))
        .routes(routes!(follow::handle_follow_user))
        .routes(routes!(follow::handle_follow_user_notification))
        .routes(routes!(migrate_user::handle_user_migration))
        .with_state(state)
}
