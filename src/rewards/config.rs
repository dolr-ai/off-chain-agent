use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RewardConfig {
    pub reward_amount_inr: f64,
    pub view_milestone: u64,
    pub min_watch_duration: f64,
    pub fraud_threshold: usize,
    pub shadow_ban_duration: u64,
    pub config_version: u64,
}

impl Default for RewardConfig {
    fn default() -> Self {
        Self {
            reward_amount_inr: 10.0,
            view_milestone: 100,
            min_watch_duration: 3.0,
            fraud_threshold: 5,
            shadow_ban_duration: 3600,
            config_version: 1,
        }
    }
}

pub struct ConfigManager {
    config: Arc<RwLock<RewardConfig>>,
}

impl ConfigManager {
    pub fn new(initial_config: Option<RewardConfig>) -> Self {
        Self {
            config: Arc::new(RwLock::new(initial_config.unwrap_or_default())),
        }
    }

    pub async fn get_config(&self) -> RewardConfig {
        self.config.read().await.clone()
    }

    pub async fn update_config(&self, new_config: RewardConfig) -> Result<()> {
        let mut config = self.config.write().await;
        // Increment version on config change
        let mut updated_config = new_config;
        updated_config.config_version = config.config_version + 1;
        *config = updated_config;
        Ok(())
    }

    pub async fn get_config_version(&self) -> u64 {
        self.config.read().await.config_version
    }
}