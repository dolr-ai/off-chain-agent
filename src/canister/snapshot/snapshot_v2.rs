use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, Json};
use chrono::Utc;
use http::StatusCode;
use ic_agent::Agent;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use yral_canisters_client::ic::PLATFORM_ORCHESTRATOR_ID;

use crate::{
    app_state::AppState,
    canister::snapshot::{
        alert::snapshot_alert_job_impl,
        download::get_canister_snapshot,
        upload::upload_snapshot_to_storj_v2,
        utils::insert_canister_backup_date_into_redis,
    },
    types::RedisPool,
};

use super::{utils::get_subnet_orch_ids_list_for_backup, CanisterData, CanisterType};

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupCanistersJobPayload {
    pub num_canisters: u32,
    pub parallelism: u32,
}

#[instrument(skip(state))]
pub async fn backup_canisters_job_v2(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<BackupCanistersJobPayload>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let date_str = Utc::now().format("%Y-%m-%d").to_string();
    log::info!(
        "Starting backup canisters job v2 at {} for date {}",
        timestamp,
        date_str
    );

    let agent = state.agent.clone();
    let canister_backup_redis_pool = state.canister_backup_redis_pool.clone();

    // TODO: migrate to user_post_service/user_info_service
    // Individual user canister backups are no longer needed — user canisters have been
    // decommissioned. Only platform/subnet orchestrator backups remain.
    let _ = payload; // payload kept for API compatibility

    tokio::spawn(async move {
        if let Err(e) =
            backup_pf_and_subnet_orchs(&agent, &canister_backup_redis_pool, date_str.clone()).await
        {
            log::error!("Failed to backup PF and subnet orchs: {e}");
        }

        log::info!("Successfully backed up PF and subnet orchs. Starting snapshot alert job");

        if let Err(e) =
            snapshot_alert_job_impl(&agent, &canister_backup_redis_pool, date_str.clone()).await
        {
            log::error!("Failed to run snapshot alert job: {e}");
        }
    });

    Ok((StatusCode::OK, "Backup started".to_string()))
}

#[instrument(skip(agent))]
pub async fn backup_pf_and_subnet_orchs(
    agent: &Agent,
    canister_backup_redis_pool: &RedisPool,
    date_str: String,
) -> Result<(), anyhow::Error> {
    let pf_orch_canister_data = CanisterData {
        canister_id: PLATFORM_ORCHESTRATOR_ID,
        canister_type: CanisterType::PlatformOrch,
    };

    if let Err(e) = backup_canister_impl(
        agent,
        canister_backup_redis_pool,
        pf_orch_canister_data,
        date_str.clone(),
    )
    .await
    {
        log::error!("Failed to backup platform orchestrator: {e}");
    }

    let subnet_orch_ids =
        get_subnet_orch_ids_list_for_backup(agent, canister_backup_redis_pool, date_str.clone())
            .await?;

    for subnet_orch_id in subnet_orch_ids {
        let subnet_orch_canister_data = CanisterData {
            canister_id: subnet_orch_id,
            canister_type: CanisterType::SubnetOrch,
        };

        if let Err(e) = backup_canister_impl(
            agent,
            canister_backup_redis_pool,
            subnet_orch_canister_data,
            date_str.clone(),
        )
        .await
        {
            log::error!("Failed to backup subnet orchestrator: {e}");
        }
    }

    Ok(())
}

#[instrument(skip(agent))]
pub async fn backup_canister_impl(
    agent: &Agent,
    canister_backup_redis_pool: &RedisPool,
    canister_data: CanisterData,
    date_str: String,
) -> Result<(), anyhow::Error> {
    let canister_id = canister_data.canister_id.to_string();

    let snapshot_bytes = get_canister_snapshot(canister_data.clone(), agent)
        .await
        .map_err(|e| {
            log::error!(
                "Failed to get user canister snapshot for canister: {canister_id} error: {e}"
            );
            anyhow::anyhow!("get_canister_snapshot error: {}", e)
        })?;

    upload_snapshot_to_storj_v2(canister_data.canister_id, date_str.clone(), snapshot_bytes)
        .await
        .map_err(|e| {
            log::error!(
                "Failed to upload user canister snapshot to storj for canister: {canister_id} error: {e}"
            );
            anyhow::anyhow!("upload_snapshot_to_storj error: {}", e)
        })?;

    if let Err(e) = insert_canister_backup_date_into_redis(
        canister_backup_redis_pool,
        date_str.clone(),
        canister_data,
    )
    .await
    {
        log::error!("Failed to insert into redis: {e}");
    }

    Ok(())
}
