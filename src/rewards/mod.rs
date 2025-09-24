pub mod api;
pub mod btc_conversion;
pub mod config;
pub mod engine;
pub mod fraud_detection;
pub mod history;
pub mod user_verification;
pub mod view_tracking;
pub mod wallet;

pub use config::RewardConfig;
pub use engine::RewardEngine;
pub use view_tracking::ViewTracker;

use crate::types::RedisPool;
use anyhow::Result;
use std::sync::Arc;

#[derive(Clone)]
pub struct RewardsModule {
    pub view_tracker: ViewTracker,
    pub reward_engine: RewardEngine,
    pub redis_pool: RedisPool,
}

impl RewardsModule {
    pub fn new(redis_pool: RedisPool) -> Self {
        let view_tracker = ViewTracker::new(redis_pool.clone());
        let reward_engine = RewardEngine::new(redis_pool.clone());

        Self {
            view_tracker,
            reward_engine,
            redis_pool,
        }
    }

    pub async fn initialize(&mut self) -> Result<()> {
        // Load Lua scripts into Redis
        self.view_tracker.load_lua_scripts().await?;
        // Initialize reward engine (loads Lua scripts)
        self.reward_engine.initialize().await?;
        Ok(())
    }
}