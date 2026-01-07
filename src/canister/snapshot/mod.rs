use candid::Principal;
use serde::{Deserialize, Serialize};

pub mod alert;
pub mod download;
pub mod ic_snapshot;
pub mod snapshot_v2;
pub mod upload;
pub mod utils;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum CanisterType {
    User,
    SubnetOrch,
    PlatformOrch,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CanisterData {
    pub canister_id: Principal,
    pub canister_type: CanisterType,
}
