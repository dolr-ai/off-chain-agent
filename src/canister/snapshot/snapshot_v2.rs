#[cfg(not(feature = "local-bin"))]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, Json};
use candid::Principal;
#[cfg(not(feature = "local-bin"))]
use chrono::Utc;
#[cfg(not(feature = "local-bin"))]
use futures::StreamExt;
use http::StatusCode;
#[cfg(not(feature = "local-bin"))]
use ic_agent::Agent;
use serde::{Deserialize, Serialize};
#[cfg(not(feature = "local-bin"))]
use tracing::instrument;

#[cfg(not(feature = "local-bin"))]
use yral_canisters_client::ic::PLATFORM_ORCHESTRATOR_ID;

use crate::app_state::AppState;
#[cfg(not(feature = "local-bin"))]
use crate::canister::snapshot::utils::insert_canister_backup_date_into_redis;
#[cfg(not(feature = "local-bin"))]
use crate::{
    canister::snapshot::{
        alert::snapshot_alert_job_impl,
        download::get_canister_snapshot,
        upload::upload_snapshot_to_storj_v2,
    },
    types::RedisPool,
};

#[cfg(not(feature = "local-bin"))]
use super::CanisterData;
#[cfg(not(feature = "local-bin"))]
use super::{utils::get_subnet_orch_ids_list_for_backup, CanisterType};
#[cfg(not(feature = "local-bin"))]
use crate::canister::snapshot::utils::get_user_canister_list_for_backup;

#[derive(Debug, Serialize, Deserialize)]
pub struct BackupCanistersJobPayload {
    pub num_canisters: u32,
    pub parallelism: u32,
}

#[instrument(skip(state))]
#[cfg(not(feature = "local-bin"))]
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

    let mut user_canister_list =
        get_user_canister_list_for_backup(&agent, &canister_backup_redis_pool, date_str.clone())
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if payload.num_canisters > 0 {
        user_canister_list = user_canister_list
            .into_iter()
            .take(payload.num_canisters as usize)
            .collect();
    }

    tokio::spawn(async move {
        let _failed_canisters_ids = backup_user_canisters_bulk(
            &agent,
            user_canister_list,
            &canister_backup_redis_pool,
            date_str.clone(),
            payload.parallelism,
        )
        .await;

        if let Err(e) =
            backup_pf_and_subnet_orchs(&agent, &canister_backup_redis_pool, date_str.clone()).await
        {
            log::error!("Failed to backup PF and subnet orchs: {e}");
        }

        log::info!("Successfully backed up PF and subnet orchs. Starting snapshot alert job");

        #[cfg(not(feature = "local-bin"))]
        {
            if let Err(e) =
                snapshot_alert_job_impl(&agent, &canister_backup_redis_pool, date_str.clone()).await
            {
                log::error!("Failed to run snapshot alert job: {e}");
            }
        }
    });

    Ok((StatusCode::OK, "Backup started".to_string()))
}

#[cfg(feature = "local-bin")]
pub async fn backup_canisters_job_v2(
    State(_state): State<Arc<AppState>>,
    Json(_payload): Json<BackupCanistersJobPayload>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    Ok((StatusCode::OK, "Backup not available in local-bin mode".to_string()))
}

#[instrument(skip(agent, user_canister_list, canister_backup_redis_pool))]
#[cfg(not(feature = "local-bin"))]
pub async fn backup_user_canisters_bulk(
    agent: &Agent,
    user_canister_list: Vec<Principal>,
    canister_backup_redis_pool: &RedisPool,
    date_str: String,
    parallelism: u32,
) -> Result<Vec<Principal>, anyhow::Error> {
    let total_canisters = user_canister_list.len();
    let completed_counter = Arc::new(AtomicUsize::new(0));
    let failed_counter = Arc::new(AtomicUsize::new(0));

    log::info!("Starting backup for {total_canisters} canisters");

    let futures = user_canister_list.into_iter().map(|canister_id| {
        let agent = agent.clone();
        let date_str = date_str.clone();
        let completed_counter = completed_counter.clone();
        let failed_counter = failed_counter.clone();
        let canister_data = CanisterData {
            canister_id,
            canister_type: CanisterType::User,
        };
        let canister_backup_redis_pool = canister_backup_redis_pool.clone();

        async move {
            let result = backup_canister_impl(
                &agent,
                &canister_backup_redis_pool,
                canister_data.clone(),
                date_str,
            )
            .await;

            let current_completed = if result.is_ok() {
                completed_counter.fetch_add(1, Ordering::Relaxed) + 1
            } else {
                failed_counter.fetch_add(1, Ordering::Relaxed);
                completed_counter.fetch_add(1, Ordering::Relaxed) + 1
            };

            if current_completed % 500 == 0 {
                let failed_count = failed_counter.load(Ordering::Relaxed);
                let success_count = current_completed - failed_count;
                log::info!(
                    "Backup progress: {current_completed}/{total_canisters} completed - {success_count} successful, {failed_count} failed"
                );
            }

            Some((
                canister_data.canister_id,
                result.map_err(|e| anyhow::anyhow!("Failed to backup user canister: {}", e)),
            ))
        }
    });
    let results: Vec<Option<(Principal, Result<(), anyhow::Error>)>> =
        futures::stream::iter(futures)
            .buffer_unordered(parallelism as usize)
            .collect::<Vec<_>>()
            .await;

    let failed_canisters_ids = results
        .iter()
        .filter(|result| result.is_some())
        .map(|result| result.as_ref().unwrap().0)
        .collect::<Vec<_>>();
    log::error!(
        "Failed to backup user canisters: {:?}/{:?}",
        failed_canisters_ids.len(),
        results.len()
    );

    Ok(failed_canisters_ids)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BackupUserCanisterPayload {
    pub canister_id: Principal,
    pub date_str: String,
}

#[instrument(skip(state))]
#[cfg(not(feature = "local-bin"))]
pub async fn backup_user_canister(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<BackupUserCanisterPayload>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let agent = state.agent.clone();
    let canister_backup_redis_pool = state.canister_backup_redis_pool.clone();

    let canister_data = CanisterData {
        canister_id: payload.canister_id,
        canister_type: CanisterType::User,
    };

    backup_canister_impl(
        &agent,
        &canister_backup_redis_pool,
        canister_data,
        payload.date_str,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((StatusCode::OK, "Backup successful".to_string()))
}

#[cfg(feature = "local-bin")]
pub async fn backup_user_canister(
    State(_state): State<Arc<AppState>>,
    Json(_payload): Json<BackupUserCanisterPayload>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    Ok((StatusCode::OK, "Backup not available in local-bin mode".to_string()))
}

#[instrument(skip(agent))]
#[cfg(not(feature = "local-bin"))]
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
#[cfg(not(feature = "local-bin"))]
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
