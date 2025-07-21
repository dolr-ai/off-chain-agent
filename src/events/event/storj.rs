use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::anyhow;
use axum::{extract::State, Json};
use serde::Deserialize;
use yral_canisters_client::dedup_index::{DedupIndex, SystemTime as DedupSystemTime};

use crate::{
    app_state::AppState,
    consts::{DEDUP_INDEX_CANISTER_ID, STORJ_INTERFACE_TOKEN, STORJ_INTERFACE_URL},
    AppError,
};

pub async fn storj_ingest(
    Json(payload): Json<storj_interface::duplicate::Args>,
) -> Result<(), AppError> {
    let client = reqwest::Client::new();
    client
        .post(
            STORJ_INTERFACE_URL
                .join("/duplicate")
                .expect("url to be valid"),
        )
        .json(&payload)
        .bearer_auth(STORJ_INTERFACE_TOKEN.as_str())
        .send()
        .await?;

    Ok(())
}

/// for the purpose of backfilling, can be removed once there are no more items
/// to be filled
pub async fn enqueue_storj_backfill_item(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<storj_interface::duplicate::Args>,
) -> Result<(), AppError> {
    state.qstash_client.duplicate_to_storj(payload).await?;

    Ok(())
}

#[derive(Deserialize)]
pub struct DedupRequestArgs {
    pub video_id: String,
    pub video_hash: String,
    pub created_at: SystemTime,
}

pub async fn enqueue_dedup_index_backfill_item(
    State(state): State<Arc<AppState>>,
    Json(DedupRequestArgs {
        video_id,
        video_hash,
        created_at,
    }): Json<DedupRequestArgs>,
) -> Result<(), AppError> {
    let dedup_index = DedupIndex(*DEDUP_INDEX_CANISTER_ID, &state.agent);
    let created_at = created_at
        .duration_since(UNIX_EPOCH)
        .map_err(|err| anyhow!("couldn't get duration?: {err:?}"))?;

    dedup_index
        .add_video_to_index(
            video_id,
            (
                video_hash,
                DedupSystemTime {
                    nanos_since_epoch: created_at.subsec_nanos(),
                    secs_since_epoch: created_at.as_secs(),
                },
            ),
        )
        .await
        .map_err(|err| anyhow!("couldn't add video to index: {err:#?}"))?;

    Ok(())
}
