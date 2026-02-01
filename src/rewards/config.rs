use crate::yral_auth::dragonfly::DragonflyPool;
use anyhow::{Context, Result};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum RewardTokenType {
    #[default]
    Btc,
    Dolr,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RewardMode {
    InrAmount { amount_per_view_inr: f64 },
    DirectTokenE8s { amount_per_milestone_e8s: u64 },
}

impl Default for RewardMode {
    fn default() -> Self {
        RewardMode::InrAmount {
            amount_per_view_inr: 0.037,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RewardConfigV1 {
    pub reward_amount_inr: f64,
    pub view_milestone: u64,
    pub min_watch_duration: f64,
    pub fraud_threshold: usize,
    pub shadow_ban_duration: u64,
    pub config_version: u64,
    #[serde(default)]
    pub reward_token: RewardTokenType,
}

impl From<RewardConfigV1> for RewardConfig {
    fn from(v1: RewardConfigV1) -> Self {
        Self {
            reward_mode: RewardMode::InrAmount {
                amount_per_view_inr: v1.reward_amount_inr,
            },
            view_milestone: v1.view_milestone,
            min_watch_duration: v1.min_watch_duration,
            fraud_threshold: v1.fraud_threshold,
            shadow_ban_duration: v1.shadow_ban_duration,
            config_version: v1.config_version,
            reward_token: v1.reward_token,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RewardConfig {
    pub reward_mode: RewardMode,
    pub view_milestone: u64,
    pub min_watch_duration: f64,
    pub fraud_threshold: usize,
    pub shadow_ban_duration: u64,
    pub config_version: u64,
    pub reward_token: RewardTokenType,
}

impl Default for RewardConfig {
    fn default() -> Self {
        Self {
            reward_mode: RewardMode::default(),
            view_milestone: 100,
            min_watch_duration: 3.0,
            fraud_threshold: 5,
            shadow_ban_duration: 3600,
            config_version: 1,
            reward_token: RewardTokenType::default(),
        }
    }
}

/// Get the current reward configuration from Dragonfly
pub async fn get_config(dragonfly_pool: &Arc<DragonflyPool>) -> Result<RewardConfig> {
    let mut conn = dragonfly_pool.get().await?;
    let config_key = "impressions:rewards:config".to_string();
    let config_str: Option<String> = conn
        .get(&config_key)
        .await
        .context("Failed to get config from Dragonfly")?;

    match config_str {
        Some(s) => {
            let config = match serde_json::from_str::<RewardConfig>(&s) {
                Ok(v2_config) => v2_config,
                Err(_) => {
                    log::info!("Attempting to migrate V1 config to V2");
                    let v1_config: RewardConfigV1 = serde_json::from_str(&s)
                        .context("Failed to deserialize as both V2 and V1 config")?;

                    let v2_config: RewardConfig = v1_config.into();

                    log::info!("Persisting migrated V2 config back to Redis");
                    let v2_json = serde_json::to_string(&v2_config)?;
                    conn.set::<_, _, ()>(&config_key, v2_json)
                        .await
                        .context("Failed to persist migrated V2 config")?;

                    v2_config
                }
            };

            Ok(config)
        }
        None => {
            // If no config exists, initialize with default
            let default_config = RewardConfig::default();
            initialize_config(dragonfly_pool, &default_config).await?;
            Ok(default_config)
        }
    }
}

/// Update the reward configuration in Dragonfly
pub async fn update_config(
    dragonfly_pool: &Arc<DragonflyPool>,
    new_config: RewardConfig,
) -> Result<()> {
    let mut conn = dragonfly_pool.get().await?;

    let config_version_key = "impressions:rewards:config:version".to_string();
    let config_key = "impressions:rewards:config".to_string();

    // Atomically increment the global config version
    let version: u64 = conn
        .incr(&config_version_key, 1)
        .await
        .context("Failed to increment config version")?;

    // Update config with new version
    let mut config = new_config;
    config.config_version = version;

    let config_json = serde_json::to_string(&config)?;
    conn.set::<_, _, ()>(&config_key, config_json)
        .await
        .context("Failed to store config in Dragonfly")?;

    log::info!("Updated reward config to version {}: {:?}", version, config);
    Ok(())
}

/// Get the current config version from Dragonfly
#[cfg(test)]
pub async fn get_config_version(dragonfly_pool: &Arc<DragonflyPool>) -> Result<u64> {
    let mut conn = dragonfly_pool.get().await?;
    let config_version_key = "impressions:rewards:config:version".to_string();
    let version: Option<u64> = conn
        .get(&config_version_key)
        .await
        .context("Failed to get config version")?;
    Ok(version.unwrap_or(1))
}

/// Initialize config in Dragonfly if it doesn't exist
async fn initialize_config(
    dragonfly_pool: &Arc<DragonflyPool>,
    config: &RewardConfig,
) -> Result<()> {
    let mut conn = dragonfly_pool.get().await?;

    let config_version_key = "impressions:rewards:config:version".to_string();
    let config_key = "impressions:rewards:config".to_string();

    // Set initial version if not exists
    let _: bool = conn
        .set_nx(&config_version_key, config.config_version)
        .await
        .context("Failed to initialize config version")?;

    // Set config
    let config_json = serde_json::to_string(config)?;
    let _: bool = conn
        .set_nx(&config_key, config_json)
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
                    "impressions:rewards:config".to_string(),
                    "impressions:rewards:config:version".to_string(),
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
        assert!(matches!(
            config.reward_mode,
            RewardMode::InrAmount {
                amount_per_view_inr
            } if (amount_per_view_inr - 0.037).abs() < 0.001
        ));
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
        assert!(matches!(config.reward_mode, RewardMode::InrAmount { .. }));
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
            reward_mode: RewardMode::InrAmount {
                amount_per_view_inr: 20.0,
            },
            view_milestone: 200,
            min_watch_duration: 5.0,
            fraud_threshold: 10,
            shadow_ban_duration: 7200,
            config_version: 0, // Will be overridden
            reward_token: RewardTokenType::default(),
        };

        update_config(&test_config.redis_pool, new_config)
            .await
            .unwrap();

        // Verify the config was updated
        let retrieved_config = get_config(&test_config.redis_pool).await.unwrap();
        assert!(matches!(
            retrieved_config.reward_mode,
            RewardMode::InrAmount {
                amount_per_view_inr
            } if (amount_per_view_inr - 20.0).abs() < 0.001
        ));
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
                reward_mode: RewardMode::InrAmount {
                    amount_per_view_inr: 10.0 + i as f64,
                },
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
            reward_mode: RewardMode::InrAmount {
                amount_per_view_inr: 15.5,
            },
            view_milestone: 150,
            min_watch_duration: 4.5,
            fraud_threshold: 8,
            shadow_ban_duration: 5400,
            config_version: 1,
            reward_token: RewardTokenType::default(),
        };

        update_config(&test_config.redis_pool, config.clone())
            .await
            .unwrap();

        let retrieved = get_config(&test_config.redis_pool).await.unwrap();
        assert_eq!(retrieved.reward_mode, config.reward_mode);
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
                    reward_mode: RewardMode::InrAmount {
                        amount_per_view_inr: 10.0 + i as f64,
                    },
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

    /// Test V1 to V2 config migration
    #[tokio::test]
    async fn test_v1_to_v2_migration() {
        let test_config = TestConfig::new().await;
        test_config.cleanup().await.unwrap();

        // V1 config JSON (old format)
        let v1_config_json = r#"{
            "reward_amount_inr": 25.0,
            "view_milestone": 100,
            "min_watch_duration": 3.0,
            "fraud_threshold": 5,
            "shadow_ban_duration": 3600,
            "config_version": 1,
            "reward_token": "btc"
        }"#;

        let mut conn = test_config.redis_pool.get().await.unwrap();
        let _: () = conn
            .set("impressions:rewards:config", v1_config_json)
            .await
            .unwrap();

        // First read should trigger migration and persist V2
        let config = get_config(&test_config.redis_pool).await.unwrap();

        assert!(matches!(
            config.reward_mode,
            RewardMode::InrAmount {
                amount_per_view_inr
            } if (amount_per_view_inr - 25.0).abs() < 0.001
        ));

        // Read the raw JSON from Redis to verify V2 was persisted
        let persisted_json: String = conn.get("impressions:rewards:config").await.unwrap();
        let persisted_config: RewardConfig = serde_json::from_str(&persisted_json).unwrap();

        // Verify the persisted config is V2 format with reward_mode
        assert!(matches!(
            persisted_config.reward_mode,
            RewardMode::InrAmount {
                amount_per_view_inr
            } if (amount_per_view_inr - 25.0).abs() < 0.001
        ));

        test_config.cleanup().await.unwrap();
    }

    /// Test InrAmount mode
    #[tokio::test]
    async fn test_inr_amount_mode() {
        let test_config = TestConfig::new().await;
        test_config.cleanup().await.unwrap();

        let config = RewardConfig {
            reward_mode: RewardMode::InrAmount {
                amount_per_view_inr: 50.0,
            },
            view_milestone: 200,
            min_watch_duration: 5.0,
            fraud_threshold: 10,
            shadow_ban_duration: 7200,
            config_version: 1,
            reward_token: RewardTokenType::Btc,
        };

        update_config(&test_config.redis_pool, config.clone())
            .await
            .unwrap();

        let retrieved = get_config(&test_config.redis_pool).await.unwrap();
        assert!(matches!(
            retrieved.reward_mode,
            RewardMode::InrAmount {
                amount_per_view_inr
            } if (amount_per_view_inr - 50.0).abs() < 0.001
        ));
        assert_eq!(retrieved.view_milestone, 200);

        test_config.cleanup().await.unwrap();
    }

    /// Test DirectTokenE8s mode
    #[tokio::test]
    async fn test_direct_token_e8s_mode() {
        let test_config = TestConfig::new().await;
        test_config.cleanup().await.unwrap();

        let config = RewardConfig {
            reward_mode: RewardMode::DirectTokenE8s {
                amount_per_milestone_e8s: 5_000_000, // 0.05 tokens per milestone
            },
            view_milestone: 100,
            min_watch_duration: 3.0,
            fraud_threshold: 5,
            shadow_ban_duration: 3600,
            config_version: 1,
            reward_token: RewardTokenType::Dolr,
        };

        update_config(&test_config.redis_pool, config.clone())
            .await
            .unwrap();

        let retrieved = get_config(&test_config.redis_pool).await.unwrap();
        assert!(matches!(
            retrieved.reward_mode,
            RewardMode::DirectTokenE8s {
                amount_per_milestone_e8s
            } if amount_per_milestone_e8s == 5_000_000
        ));
        assert_eq!(retrieved.reward_token, RewardTokenType::Dolr);

        test_config.cleanup().await.unwrap();
    }

    /// Test serialization and deserialization of both reward modes
    #[tokio::test]
    async fn test_reward_mode_serialization() {
        // Test InrAmount mode
        let inr_mode = RewardMode::InrAmount {
            amount_per_view_inr: 12.5,
        };
        let json = serde_json::to_string(&inr_mode).unwrap();
        let deserialized: RewardMode = serde_json::from_str(&json).unwrap();
        assert_eq!(inr_mode, deserialized);

        // Test DirectTokenE8s mode
        let e8s_mode = RewardMode::DirectTokenE8s {
            amount_per_milestone_e8s: 10_000_000,
        };
        let json = serde_json::to_string(&e8s_mode).unwrap();
        let deserialized: RewardMode = serde_json::from_str(&json).unwrap();
        assert_eq!(e8s_mode, deserialized);
    }

    /// Test V1 to V2 conversion
    #[tokio::test]
    async fn test_v1_conversion() {
        let v1 = RewardConfigV1 {
            reward_amount_inr: 15.0,
            view_milestone: 100,
            min_watch_duration: 3.0,
            fraud_threshold: 5,
            shadow_ban_duration: 3600,
            config_version: 1,
            reward_token: RewardTokenType::Btc,
        };

        let v2: RewardConfig = v1.into();

        assert!(matches!(
            v2.reward_mode,
            RewardMode::InrAmount {
                amount_per_view_inr
            } if (amount_per_view_inr - 15.0).abs() < 0.001
        ));
        assert_eq!(v2.view_milestone, 100);
        assert_eq!(v2.reward_token, RewardTokenType::Btc);
    }
}
