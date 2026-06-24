use ic_agent::{export::Principal, Agent};
use tracing::instrument;
use yral_canisters_client::{
    ic::PLATFORM_ORCHESTRATOR_ID, platform_orchestrator::PlatformOrchestrator,
};

#[instrument(skip(agent))]
pub async fn get_subnet_orch_ids(agent: &Agent) -> Result<Vec<Principal>, anyhow::Error> {
    let pf_orch = PlatformOrchestrator(PLATFORM_ORCHESTRATOR_ID, agent);

    let subnet_orch_ids = pf_orch.get_all_subnet_orchestrators().await?;

    Ok(subnet_orch_ids)
}

/// Fetch the list of (user_principal, canister_id) pairs.
///
/// Subnet orchestrators (user_index canisters) have been decommissioned.
/// This function now returns an empty list. Callers that need user canister
/// mappings should use user_info_service instead.
#[instrument(skip(_agent))]
pub async fn get_user_principal_canister_list_v2(
    _agent: &Agent,
) -> Result<Vec<(Principal, Principal)>, anyhow::Error> {
    Ok(vec![])
}
