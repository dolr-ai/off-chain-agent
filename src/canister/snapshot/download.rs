use ic_agent::Agent;
use tracing::instrument;

use super::{CanisterData, CanisterType};

#[instrument(skip(agent))]
pub async fn get_canister_snapshot(
    canister_data: CanisterData,
    agent: &Agent,
) -> Result<Vec<u8>, anyhow::Error> {
    match canister_data.canister_type {
        // Subnet orchestrators (user_index canisters) have been decommissioned.
        // Snapshots are no longer taken for this canister type.
        CanisterType::SubnetOrch => {
            log::warn!(
                "Snapshot requested for decommissioned subnet orchestrator {}; skipping",
                canister_data.canister_id
            );
            Ok(vec![])
        }
        // Platform orchestrator has been decommissioned; snapshots are no longer taken.
        CanisterType::PlatformOrch => {
            log::warn!(
                "Snapshot requested for decommissioned platform orchestrator {}; skipping",
                canister_data.canister_id
            );
            Ok(vec![])
        }
    }
}
