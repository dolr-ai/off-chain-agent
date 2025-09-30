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
    conn.set::<_, _, ()>("rewards:config", config_json)
        .await
        .context("Failed to store config in Redis")?;

    log::info!("Updated reward config to version {}: {:?}", version, config);
    Ok(())
}

/// Get the current config version from Redis
#[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::*;

    struct TestConfig {
        redis_pool: RedisPool,
        cleanup_keys: Vec<String>,
    }

    impl TestConfig {
        async fn new() -> Self {
            let redis_url = std::env::var("TEST_REDIS_URL")
                .unwrap_or_else(|_| "redis://localhost:6379".to_string());

            let manager = bb8_redis::RedisConnectionManager::new(redis_url)
                .expect("Failed to create Redis connection manager");
            let pool = bb8::Pool::builder()
                .build(manager)
                .await
                .expect("Failed to build Redis pool");

            Self {
                redis_pool: pool,
                cleanup_keys: vec![
                    "rewards:config".to_string(),
                    "rewards:config:version".to_string(),
                ],
            }
        }

        async fn cleanup(&self) -> Result<()> {
            let mut conn = self.redis_pool.get().await?;
            for key in &self.cleanup_keys {
                let _: () = conn.del(key).await?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_default_config() {
        let config = RewardConfig::default();
        assert_eq!(config.reward_amount_inr, 10.0);
        assert_eq!(config.view_milestone, 100);
        assert_eq!(config.min_watch_duration, 3.0);
        assert_eq!(config.fraud_threshold, 5);
        assert_eq!(config.shadow_ban_duration, 3600);
        assert_eq!(config.config_version, 1);
    }

    #[tokio::test]
    async fn test_get_config_initializes_default() {
        let test_config = TestConfig::new().await;

        // Clean up any existing config
        test_config.cleanup().await.unwrap();

        // First get should initialize with defaults
        let config = get_config(&test_config.redis_pool).await.unwrap();
        assert_eq!(config.reward_amount_inr, 10.0);
        assert_eq!(config.view_milestone, 100);
        assert_eq!(config.config_version, 1);

        // Verify version was set
        let version = get_config_version(&test_config.redis_pool).await.unwrap();
        assert_eq!(version, 1);

        test_config.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_update_config() {
        let test_config = TestConfig::new().await;

        // Clean up any existing config
        test_config.cleanup().await.unwrap();

        let new_config = RewardConfig {
            reward_amount_inr: 20.0,
            view_milestone: 200,
            min_watch_duration: 5.0,
            fraud_threshold: 10,
            shadow_ban_duration: 7200,
            config_version: 0, // Will be overridden
        };

        update_config(&test_config.redis_pool, new_config)
            .await
            .unwrap();

        // Verify the config was updated
        let retrieved_config = get_config(&test_config.redis_pool).await.unwrap();
        assert_eq!(retrieved_config.reward_amount_inr, 20.0);
        assert_eq!(retrieved_config.view_milestone, 200);
        assert_eq!(retrieved_config.min_watch_duration, 5.0);
        assert_eq!(retrieved_config.fraud_threshold, 10);
        assert_eq!(retrieved_config.shadow_ban_duration, 7200);

        // Version should have been incremented
        let version = get_config_version(&test_config.redis_pool).await.unwrap();
        assert!(version >= 1);

        test_config.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_config_version_increment() {
        let test_config = TestConfig::new().await;

        // Clean up any existing config
        test_config.cleanup().await.unwrap();

        // Initialize
        let _ = get_config(&test_config.redis_pool).await.unwrap();
        let initial_version = get_config_version(&test_config.redis_pool).await.unwrap();

        // Update config multiple times
        for i in 0..3 {
            let config = RewardConfig {
                reward_amount_inr: 10.0 + i as f64,
                ..Default::default()
            };
            update_config(&test_config.redis_pool, config)
                .await
                .unwrap();
        }

        // Version should have incremented by 3
        let final_version = get_config_version(&test_config.redis_pool).await.unwrap();
        assert_eq!(final_version, initial_version + 3);

        test_config.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_config_serialization() {
        let test_config = TestConfig::new().await;

        // Clean up any existing config
        test_config.cleanup().await.unwrap();

        let config = RewardConfig {
            reward_amount_inr: 15.5,
            view_milestone: 150,
            min_watch_duration: 4.5,
            fraud_threshold: 8,
            shadow_ban_duration: 5400,
            config_version: 1,
        };

        update_config(&test_config.redis_pool, config.clone())
            .await
            .unwrap();

        let retrieved = get_config(&test_config.redis_pool).await.unwrap();
        assert_eq!(retrieved.reward_amount_inr, config.reward_amount_inr);
        assert_eq!(retrieved.view_milestone, config.view_milestone);
        assert_eq!(retrieved.min_watch_duration, config.min_watch_duration);
        assert_eq!(retrieved.fraud_threshold, config.fraud_threshold);
        assert_eq!(retrieved.shadow_ban_duration, config.shadow_ban_duration);

        test_config.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_config_updates() {
        let test_config = TestConfig::new().await;

        // Clean up any existing config
        test_config.cleanup().await.unwrap();

        // Initialize
        let _ = get_config(&test_config.redis_pool).await.unwrap();
        let initial_version = get_config_version(&test_config.redis_pool).await.unwrap();

        // Spawn multiple concurrent update tasks
        let mut handles = vec![];
        for i in 0..5 {
            let pool = test_config.redis_pool.clone();
            let handle = tokio::spawn(async move {
                let config = RewardConfig {
                    reward_amount_inr: 10.0 + i as f64,
                    ..Default::default()
                };
                update_config(&pool, config).await
            });
            handles.push(handle);
        }

        // Wait for all updates
        for handle in handles {
            let _ = handle.await.unwrap();
        }

        // Version should have incremented by 5
        let final_version = get_config_version(&test_config.redis_pool).await.unwrap();
        assert_eq!(final_version, initial_version + 5);

        test_config.cleanup().await.unwrap();
    }
}
