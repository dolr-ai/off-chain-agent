use candid::CandidType;
use serde::{Deserialize, Serialize};

pub type RedisPool = bb8::Pool<bb8_redis::RedisConnectionManager>;

// Re-export DelegatedIdentityWire from yral-types
pub use yral_types::delegated_identity::DelegatedIdentityWire;

#[derive(Serialize, Deserialize, Clone, Copy, CandidType, Debug, PartialEq)]
#[allow(dead_code)]
pub enum SessionType {
    AnonymousSession,
    RegisteredSession,
}
