use crate::app_state::AppState;
use crate::duplicate_video::phash_api;
use std::sync::Arc;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

pub fn video_router(app_state: Arc<AppState>) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(phash_api::compute_phash_api))
        .with_state(app_state)
}
