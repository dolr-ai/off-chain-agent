pub mod delete_user;
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
        .with_state(state)
}
