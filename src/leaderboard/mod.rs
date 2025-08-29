pub mod handlers;
pub mod redis_ops;
pub mod tournament;
pub mod types;

pub use types::*;

use crate::app_state::AppState;
use std::sync::Arc;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

pub fn leaderboard_router(state: Arc<AppState>) -> OpenApiRouter {
    OpenApiRouter::new()
        // Score updates
        .routes(routes!(handlers::update_score_handler))
        // Leaderboard queries
        .routes(routes!(handlers::get_leaderboard_handler))
        .routes(routes!(handlers::get_user_rank_handler))
        .routes(routes!(handlers::search_users_handler))
        .routes(routes!(handlers::get_tournament_history_handler))
        .routes(routes!(handlers::get_tournament_results_handler))
        .routes(routes!(handlers::tournament_lifecycle_check_handler))
        .with_state(state)
}
