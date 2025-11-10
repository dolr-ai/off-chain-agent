use crate::app_state::AppState;
use crate::milvus::api;
use std::sync::Arc;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

pub fn milvus_router(app_state: Arc<AppState>) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(api::check_duplicate_handler))
        .with_state(app_state)
}
