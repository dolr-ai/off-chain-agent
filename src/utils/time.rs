use std::time::UNIX_EPOCH;

/// Local re-definition of the candid SystemTime struct previously imported
/// from `individual_user_template`. Kept here so callers that still need to
/// serialise timestamps to candid don't depend on the decommissioned canister
/// client.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, candid::CandidType, serde::Serialize, serde::Deserialize,
)]
pub struct SystemTime {
    pub nanos_since_epoch: u32,
    pub secs_since_epoch: u64,
}

pub fn system_time_to_custom(time: std::time::SystemTime) -> SystemTime {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    SystemTime {
        nanos_since_epoch: duration.subsec_nanos(),
        secs_since_epoch: duration.as_secs(),
    }
}
