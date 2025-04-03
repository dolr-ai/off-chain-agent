use std::sync::Arc;

use axum::{extract::State, Json};

use crate::{app_state::AppState, AppError};

pub async fn storj_ingest(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<storj_interface::duplicate::Args>,
) -> Result<(), AppError> {
    // let client = reqwest::Client::new();
    // client
    //     .post(
    //         STORJ_INTERFACE_URL
    //             .join("/duplicate")
    //             .expect("url to be valid"),
    //     )
    //     .json(&payload)
    //     .bearer_auth(STORJ_INTERFACE_TOKEN.as_str())
    //     .send()
    //     .await?;
    //
    log::info!("test succesful");

    Ok(())
}

pub async fn enqueue_storj_backfill_item(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<storj_interface::duplicate::Args>,
) -> Result<(), AppError> {
    state.qstash_client.duplicate_to_storj(payload).await?;

    Ok(())
}
