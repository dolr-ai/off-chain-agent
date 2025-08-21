use ic_agent::{identity::DelegatedIdentity, Agent};

use crate::types::DelegatedIdentityWire;

pub async fn get_agent_from_delegated_identity_wire(
    identity_wire: &DelegatedIdentityWire,
) -> Result<Agent, anyhow::Error> {
    let identity: DelegatedIdentity =
        identity_wire
            .clone()
            .try_into()
            .map_err(|e: k256::elliptic_curve::Error| {
                anyhow::anyhow!("Failed to create delegated identity: {}", e)
            })?;
    let agent = Agent::builder()
        .with_identity(identity)
        .with_url("https://ic0.app")
        .build()?;

    Ok(agent)
}
