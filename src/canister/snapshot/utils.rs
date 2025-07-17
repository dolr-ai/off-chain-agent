use std::collections::HashSet;

use candid::Principal;
use ic_agent::Agent;
use redis::AsyncCommands;
use yral_canisters_client::ic::PLATFORM_ORCHESTRATOR_ID;

use crate::{
    canister::{
        snapshot::CanisterType,
        utils::{get_subnet_orch_ids, get_user_canisters_list_v2},
    },
    types::RedisPool,
};

use super::CanisterData;

pub async fn insert_canister_backup_date_into_redis(
    canister_backup_redis_pool: &RedisPool,
    date_str: String,
    canister_data: CanisterData,
) -> Result<(), anyhow::Error> {
    let redis_key = format!(
        "canister_backup_date:{:?}:{}",
        canister_data.canister_type, date_str
    );
    let mut conn = canister_backup_redis_pool.get().await?;

    conn.rpush::<String, String, ()>(redis_key, canister_data.canister_id.to_string())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to insert into redis: {}", e))?;

    Ok(())
}

pub async fn get_canister_backup_date_list(
    canister_backup_redis_pool: &RedisPool,
    canister_type: CanisterType,
    date_str: String,
) -> Result<Vec<String>, anyhow::Error> {
    let mut conn = canister_backup_redis_pool.get().await?;
    let len = conn
        .llen::<String, usize>(format!(
            "canister_backup_date:{canister_type:?}:{date_str}"
        ))
        .await?;

    // fetch in chunks of 10000
    let mut canister_backup_date_list = vec![];
    for i in (0..len).step_by(10000) {
        let chunk = conn
            .lrange::<String, Vec<String>>(
                format!("canister_backup_date:{canister_type:?}:{date_str}"),
                i as isize,
                (i + 9999) as isize,
            )
            .await?;
        canister_backup_date_list.extend(chunk);
    }

    Ok(canister_backup_date_list)
}

pub async fn get_user_canister_list_for_backup(
    agent: &Agent,
    canister_backup_redis_pool: &RedisPool,
    date_str: String,
) -> Result<Vec<Principal>, anyhow::Error> {
    let user_canister_list = get_user_canisters_list_v2(agent).await?;

    log::info!("User canister list length: {:?}", user_canister_list.len());

    let canister_backup_date_list =
        get_canister_backup_date_list(canister_backup_redis_pool, CanisterType::User, date_str)
            .await?;
    let canister_backup_date_set = canister_backup_date_list
        .iter()
        .map(|canister_id| Principal::from_text(canister_id).unwrap())
        .collect::<HashSet<_>>();

    let user_canister_list = user_canister_list
        .into_iter()
        .filter(|canister_id| !canister_backup_date_set.contains(canister_id))
        .collect::<Vec<_>>();

    log::info!(
        "User canister list length after filtering: {:?}",
        user_canister_list.len()
    );

    Ok(user_canister_list)
}

pub async fn get_subnet_orch_ids_list_for_backup(
    agent: &Agent,
    canister_backup_redis_pool: &RedisPool,
    date_str: String,
) -> Result<Vec<Principal>, anyhow::Error> {
    let subnet_orch_ids_list = get_subnet_orch_ids(agent).await?;

    log::info!(
        "Subnet orch ids list length: {:?}",
        subnet_orch_ids_list.len()
    );

    let canister_backup_date_list = get_canister_backup_date_list(
        canister_backup_redis_pool,
        CanisterType::SubnetOrch,
        date_str,
    )
    .await?;
    let canister_backup_date_set = canister_backup_date_list
        .iter()
        .map(|canister_id| Principal::from_text(canister_id).unwrap())
        .collect::<HashSet<_>>();

    let subnet_orch_ids_list = subnet_orch_ids_list
        .into_iter()
        .filter(|canister_id| !canister_backup_date_set.contains(canister_id))
        .collect::<Vec<_>>();

    log::info!(
        "Subnet orch ids list length after filtering: {:?}",
        subnet_orch_ids_list.len()
    );

    Ok(subnet_orch_ids_list)
}

pub async fn get_platform_orch_ids_list_for_backup(
    _agent: &Agent,
    canister_backup_redis_pool: &RedisPool,
    date_str: String,
) -> Result<Vec<Principal>, anyhow::Error> {
    let platform_orch_id = PLATFORM_ORCHESTRATOR_ID;

    let canister_backup_date_list = get_canister_backup_date_list(
        canister_backup_redis_pool,
        CanisterType::PlatformOrch,
        date_str,
    )
    .await?;

    if canister_backup_date_list.contains(&platform_orch_id.to_string()) {
        return Ok(vec![]);
    }

    Ok(vec![platform_orch_id])
}
