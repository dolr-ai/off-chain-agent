use crate::types::RedisPool;
use anyhow::{Context, Result};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
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

/// Get the current reward configuration from Redis
pub async fn get_config(redis_pool: &RedisPool) -> Result<RewardConfig> {
    let mut conn = redis_pool.get().await?;
    let config_str: Option<String> = conn
        .get("rewards:config")
        .await
        .context("Failed to get config from Redis")?;

    match config_str {
        Some(s) => serde_json::from_str(&s).context("Failed to deserialize config"),
        None => {
            // If no config exists, initialize with default
            let default_config = RewardConfig::default();
            initialize_config(redis_pool, &default_config).await?;
            Ok(default_config)
        }
    }
}

/// Update the reward configuration in Redis
pub async fn update_config(redis_pool: &RedisPool, new_config: RewardConfig) -> Result<()> {
    let mut conn = redis_pool.get().await?;

    // Atomically increment the global config version
    let version: u64 = conn
        .incr("rewards:config:version", 1)
        .await
        .context("Failed to increment config version")?;

    // Update config with new version
    let mut config = new_config;
    config.config_version = version;

    let config_json = serde_json::to_string(&config)?;
    conn.set("rewards:config", config_json)
        .await
        .context("Failed to store config in Redis")?;

    log::info!("Updated reward config to version {}: {:?}", version, config);
    Ok(())
}

/// Get the current config version from Redis
pub async fn get_config_version(redis_pool: &RedisPool) -> Result<u64> {
    let mut conn = redis_pool.get().await?;
    let version: Option<u64> = conn
        .get("rewards:config:version")
        .await
        .context("Failed to get config version")?;
    Ok(version.unwrap_or(1))
}

/// Initialize config in Redis if it doesn't exist
async fn initialize_config(redis_pool: &RedisPool, config: &RewardConfig) -> Result<()> {
    let mut conn = redis_pool.get().await?;

    // Set initial version if not exists
    let _: bool = conn
        .set_nx("rewards:config:version", config.config_version)
        .await
        .context("Failed to initialize config version")?;

    // Set config
    let config_json = serde_json::to_string(config)?;
    let _: bool = conn
        .set_nx("rewards:config", config_json)
        .await
        .context("Failed to initialize config")?;

    log::info!("Initialized reward config with defaults: {:?}", config);
    Ok(())
}