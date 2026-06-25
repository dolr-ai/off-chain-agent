use ic_agent::{export::Principal, Agent};
use tracing::instrument;

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
