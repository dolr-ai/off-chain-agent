use std::sync::Arc;

use axum::{extract::State, response::IntoResponse};
use http::StatusCode;
use yral_canisters_client::{ic::NOTIFICATION_STORE_ID, notification_store::NotificationStore};

use crate::app_state::AppState;

pub async fn prune_notification_store(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let admin_client = state.agent.clone();
    let notif_store = NotificationStore(NOTIFICATION_STORE_ID, &admin_client);

    notif_store.prune_notifications().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to prune notification store: {}", e),
        )
    })?;

    Ok((StatusCode::OK, "Notification store pruned".to_string()))
}
