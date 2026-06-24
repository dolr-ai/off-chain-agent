use candid::Principal;
use ic_agent::Agent;
use tracing::instrument;
use yral_canisters_client::{
    platform_orchestrator::PlatformOrchestrator,
    user_index::UserIndex,
};

use super::{CanisterData, CanisterType};

#[instrument(skip(agent))]
pub async fn get_canister_snapshot(
    canister_data: CanisterData,
    agent: &Agent,
) -> Result<Vec<u8>, anyhow::Error> {
    match canister_data.canister_type {
        CanisterType::SubnetOrch => {
            get_subnet_orchestrator_snapshot(canister_data.canister_id, agent).await
        }
        CanisterType::PlatformOrch => {
            get_platform_orchestrator_snapshot(canister_data.canister_id, agent).await
        }
    }
}

#[instrument(skip(agent))]
pub async fn get_subnet_orchestrator_snapshot(
    canister_id: Principal,
    agent: &Agent,
) -> Result<Vec<u8>, anyhow::Error> {
    let subnet_orch = UserIndex(canister_id, agent);

    let snapshot_size = subnet_orch.save_snapshot_json().await.map_err(|e| {
        log::error!("Failed to save subnet orchestrator snapshot: {e}");
        anyhow::anyhow!("Failed to save subnet orchestrator snapshot: {}", e)
    })?;

    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    // Download snapshot

    let mut snapshot_bytes = vec![];
    let chunk_size = 1000 * 1000;
    let num_iters = (snapshot_size as f32 / chunk_size as f32).ceil() as u32;
    for i in 0..num_iters {
        let start = i * chunk_size;
        let mut end = (i + 1) * chunk_size;
        if end > snapshot_size {
            end = snapshot_size;
        }

        let res = subnet_orch
            .download_snapshot(start as u64, (end - start) as u64)
            .await
            .map_err(|e| {
                log::error!("Failed to download subnet orchestrator snapshot: {e}");
                anyhow::anyhow!("Failed to download subnet orchestrator snapshot: {}", e)
            })?;

        snapshot_bytes.extend(res);
    }

    // clear snapshot
    subnet_orch.clear_snapshot().await.map_err(|e| {
        log::error!("Failed to clear subnet orchestrator snapshot: {e}");
        anyhow::anyhow!("Failed to clear subnet orchestrator snapshot: {}", e)
    })?;

    Ok(snapshot_bytes)
}

#[instrument(skip(agent))]
pub async fn get_platform_orchestrator_snapshot(
    canister_id: Principal,
    agent: &Agent,
) -> Result<Vec<u8>, anyhow::Error> {
    let platform_orchestrator = PlatformOrchestrator(canister_id, agent);

    let snapshot_size = platform_orchestrator
        .save_snapshot_json()
        .await
        .map_err(|e| {
            log::error!("Failed to save platform orchestrator snapshot: {e}");
            anyhow::anyhow!("Failed to save platform orchestrator snapshot: {}", e)
        })?;

    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    // Download snapshot

    let mut snapshot_bytes = vec![];
    let chunk_size = 1000 * 1000;
    let num_iters = (snapshot_size as f32 / chunk_size as f32).ceil() as u32;
    for i in 0..num_iters {
        let start = i * chunk_size;
        let mut end = (i + 1) * chunk_size;
        if end > snapshot_size {
            end = snapshot_size;
        }

        let res = platform_orchestrator
            .download_snapshot(start as u64, (end - start) as u64)
            .await
            .map_err(|e| {
                log::error!("Failed to download platform orchestrator snapshot: {e}");
                anyhow::anyhow!("Failed to download platform orchestrator snapshot: {}", e)
            })?;

        snapshot_bytes.extend(res);
    }

    // clear snapshot
    platform_orchestrator.clear_snapshot().await.map_err(|e| {
        log::error!("Failed to clear platform orchestrator snapshot: {e}");
        anyhow::anyhow!("Failed to clear platform orchestrator snapshot: {}", e)
    })?;

    Ok(snapshot_bytes)
}
