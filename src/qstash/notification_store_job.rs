use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, Json};
use http::StatusCode;
use yral_canisters_client::{ic::NOTIFICATION_STORE_ID, notification_store::NotificationStore};

use crate::app_state::AppState;

// pub async fn prune_notification_store_job(State(state): State<Arc<AppState>>, Json(request): Json<()>) -> Result<(), anyhow::Error> {
//     let admin_client = state.agent.clone();
//     let notif_store = NotificationStore(NOTIFICATION_STORE_ID, &admin_client);

//     notif_store.prune_notification_store().await?;

//     Ok(())
// }

pub async fn prune_notification_store(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let admin_client = state.agent.clone();
    let notif_store = NotificationStore(NOTIFICATION_STORE_ID, &admin_client);

    notif_store.prune_notification_store().await?;

    Ok((
        StatusCode::OK,
        "Notification store pruned".to_string(),
    ))
}