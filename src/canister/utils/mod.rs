use ic_agent::{export::Principal, Agent};
use tracing::instrument;
use yral_canisters_client::{
    ic::PLATFORM_ORCHESTRATOR_ID, platform_orchestrator::PlatformOrchestrator,
    user_index::UserIndex,
};

#[instrument(skip(agent))]
pub async fn get_subnet_orch_ids(agent: &Agent) -> Result<Vec<Principal>, anyhow::Error> {
    let pf_orch = PlatformOrchestrator(PLATFORM_ORCHESTRATOR_ID, agent);

    let subnet_orch_ids = pf_orch.get_all_subnet_orchestrators().await?;

    Ok(subnet_orch_ids)
}

#[instrument(skip(agent))]
pub async fn get_user_canisters_list_v2(agent: &Agent) -> Result<Vec<Principal>, anyhow::Error> {
    let subnet_orch_ids = get_subnet_orch_ids(agent).await?;

    let mut canister_ids_list = vec![];

    for subnet_orch_id in subnet_orch_ids {
        let subnet_orch = UserIndex(subnet_orch_id, agent);
        let user_canister_ids = subnet_orch.get_user_canister_list().await?;
        canister_ids_list.extend(user_canister_ids);
    }

    Ok(canister_ids_list)
}

#[instrument(skip(agent))]
pub async fn get_user_principal_canister_list_v2(
    agent: &Agent,
) -> Result<Vec<(Principal, Principal)>, anyhow::Error> {
    let subnet_orch_ids = get_subnet_orch_ids(agent).await?;

    let mut user_principal_canister_list = vec![];

    for subnet_orch_id in subnet_orch_ids {
        let subnet_orch = UserIndex(subnet_orch_id, agent);
        let user_principal_canister_ids = subnet_orch.get_user_id_and_canister_list().await?;
        user_principal_canister_list.extend(user_principal_canister_ids);
    }

    Ok(user_principal_canister_list)
}
