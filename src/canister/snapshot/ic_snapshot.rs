use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, Json};
use candid::{CandidType, Decode, Encode, Principal};
use http::StatusCode;
use ic_agent::Agent;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::app_state::AppState;

const MANAGEMENT_CANISTER_ID: Principal = Principal::management_canister();
const MAX_SNAPSHOTS_PER_CANISTER: usize = 10;

#[derive(CandidType, Deserialize, Debug, Clone)]
pub struct Snapshot {
    pub id: Vec<u8>,
    pub taken_at_timestamp: u64,
    pub total_size: u64,
}

#[derive(CandidType, Serialize)]
struct TakeCanisterSnapshotArgs {
    canister_id: Principal,
    replace_snapshot: Option<Vec<u8>>,
}

#[derive(CandidType, Serialize)]
struct ListCanisterSnapshotsArgs {
    canister_id: Principal,
}

#[derive(CandidType, Serialize)]
struct DeleteCanisterSnapshotArgs {
    canister_id: Principal,
    snapshot_id: Vec<u8>,
}

async fn list_canister_snapshots(
    agent: &Agent,
    canister_id: Principal,
) -> Result<Vec<Snapshot>, anyhow::Error> {
    let args = ListCanisterSnapshotsArgs { canister_id };

    let response = agent
        .query(&MANAGEMENT_CANISTER_ID, "list_canister_snapshots")
        .with_arg(Encode!(&args)?)
        .call()
        .await?;

    let snapshots = Decode!(&response, Vec<Snapshot>)?;
    Ok(snapshots)
}

async fn take_canister_snapshot(
    agent: &Agent,
    canister_id: Principal,
    replace_snapshot: Option<Vec<u8>>,
) -> Result<Snapshot, anyhow::Error> {
    let args = TakeCanisterSnapshotArgs {
        canister_id,
        replace_snapshot,
    };

    let response = agent
        .update(&MANAGEMENT_CANISTER_ID, "take_canister_snapshot")
        .with_arg(Encode!(&args)?)
        .call_and_wait()
        .await?;

    let snapshot = Decode!(&response, Snapshot)?;
    Ok(snapshot)
}

/// Evict oldest snapshot if at the limit, then take a new snapshot
async fn take_snapshot_with_eviction(
    agent: &Agent,
    canister_id: Principal,
) -> Result<Snapshot, anyhow::Error> {
    // List existing snapshots
    let existing = list_canister_snapshots(agent, canister_id).await?;

    // If at limit, find oldest and use replace_snapshot
    let replace_snapshot = if existing.len() >= MAX_SNAPSHOTS_PER_CANISTER {
        let oldest = existing
            .iter()
            .min_by_key(|s| s.taken_at_timestamp)
            .map(|s| s.id.clone());

        if let Some(ref id) = oldest {
            log::info!(
                "Canister {} has {} snapshots, replacing oldest: {}",
                canister_id,
                existing.len(),
                hex::encode(id)
            );
        }

        oldest
    } else {
        None
    };

    take_canister_snapshot(agent, canister_id, replace_snapshot).await
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TakeSnapshotPayload {
    pub canister_id: Principal,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TakeSnapshotResponse {
    pub canister_id: String,
    pub snapshot_id: String,
    pub taken_at_timestamp: u64,
    pub total_size: u64,
}

/// Take an IC-level snapshot of a canister. Automatically evicts oldest snapshot if at 10 limit.
#[instrument(skip(state))]
pub async fn take_ic_snapshot(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<TakeSnapshotPayload>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let agent = &state.agent;

    let snapshot = take_snapshot_with_eviction(agent, payload.canister_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    log::info!(
        "Created IC snapshot for canister {}: id={}, size={}",
        payload.canister_id,
        hex::encode(&snapshot.id),
        snapshot.total_size
    );

    Ok((
        StatusCode::OK,
        Json(TakeSnapshotResponse {
            canister_id: payload.canister_id.to_string(),
            snapshot_id: hex::encode(&snapshot.id),
            taken_at_timestamp: snapshot.taken_at_timestamp,
            total_size: snapshot.total_size,
        }),
    ))
}
