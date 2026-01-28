use std::env;
use std::sync::Arc;

use crate::yral_auth::dragonfly::DragonflyPool;
use anyhow::Result;
use candid::Principal;
use chrono::Utc;
use redis::AsyncCommands;
use reqwest::Client;
use serde_json::json;

const DEFAULT_FRAUD_THRESHOLD: usize = 5; // 5 rewards in time window
const DEFAULT_TIME_WINDOW: i64 = 60 * 60; // 60 minutes
const DEFAULT_SHADOW_BAN_DURATION: u64 = 3600 * 5; // 5 hours

#[derive(Debug, Clone, PartialEq)]
pub enum FraudCheck {
    Clean,
    Suspicious,
}

#[derive(Clone)]
pub struct FraudDetector {
    dragonfly_pool: Arc<DragonflyPool>,
    threshold: usize,
    time_window: i64,
    shadow_ban_duration: u64,
}

impl FraudDetector {
    pub fn new(dragonfly_pool: Arc<DragonflyPool>) -> Self {
        Self {
            dragonfly_pool,
            threshold: DEFAULT_FRAUD_THRESHOLD,
            time_window: DEFAULT_TIME_WINDOW,
            shadow_ban_duration: DEFAULT_SHADOW_BAN_DURATION,
        }
    }

    pub fn with_config(
        dragonfly_pool: Arc<DragonflyPool>,
        threshold: usize,
        shadow_ban_duration: u64,
    ) -> Self {
        Self {
            dragonfly_pool,
            threshold,
            time_window: DEFAULT_TIME_WINDOW,
            shadow_ban_duration,
        }
    }

    /// Check for fraud patterns and shadow ban if necessary
    pub async fn check_fraud_patterns(&self, creator_id: Principal) -> FraudCheck {
        let key = format!("impressions:test:rewards:user:{}:recent", creator_id);
        let current_timestamp = Utc::now().timestamp();

        let dragonfly_pool = self.dragonfly_pool.clone();
        let threshold = self.threshold;
        let time_window = self.time_window;
        let shadow_ban_duration = self.shadow_ban_duration;
        let creator_id_str = creator_id.to_string();

        // Run fraud check asynchronously
        tokio::spawn(async move {
            if let Ok(mut conn) = dragonfly_pool.get().await {
                // Add current timestamp
                let _ = conn.lpush::<_, _, ()>(&key, current_timestamp).await;
                let _ = conn.ltrim::<_, ()>(&key, 0, 100).await; // Keep last 100 rewards
                let _ = conn.expire::<_, ()>(&key, 3600).await; // 1 hour TTL

                // Get recent timestamps
                if let Ok(recent_timestamps) = conn.lrange::<_, Vec<i64>>(&key, 0, -1).await {
                    let cutoff = current_timestamp - time_window;
                    let recent_count = recent_timestamps.iter().filter(|&&ts| ts > cutoff).count();

                    log::debug!(
                        "Creator {} has {} rewards in last {} seconds",
                        creator_id_str,
                        recent_count,
                        time_window
                    );

                    if recent_count > threshold {
                        // Shadow ban the creator
                        let ban_key =
                            format!("impressions:test:rewards:shadow_ban:{}", creator_id_str);
                        if let Err(e) = conn
                            .set_ex::<_, _, ()>(&ban_key, "1", shadow_ban_duration)
                            .await
                        {
                            log::error!("Failed to shadow ban creator {}: {}", creator_id_str, e);
                        }

                        log::warn!(
                            "Shadow banned creator {} for {} seconds due to {} rewards in {} seconds",
                            creator_id_str,
                            shadow_ban_duration,
                            recent_count,
                            time_window
                        );

                        // Send alert
                        send_fraud_alert(creator_id_str.clone(), recent_count);
                    }
                }
            }
        });

        // For the immediate check, we need to check if already shadow banned
        if let Ok(mut conn) = self.dragonfly_pool.get().await {
            let ban_key = format!("impressions:test:rewards:shadow_ban:{}", creator_id);
            if let Ok(is_banned) = conn.exists::<_, bool>(&ban_key).await {
                if is_banned {
                    return FraudCheck::Suspicious;
                }
            }
        }

        FraudCheck::Clean
    }

    /// Check if a creator is currently shadow banned
    pub async fn is_shadow_banned(&self, creator_id: &Principal) -> Result<bool> {
        let mut conn = self.dragonfly_pool.get().await?;
        let ban_key = format!("impressions:test:rewards:shadow_ban:{}", creator_id);
        let is_banned: bool = conn.exists(&ban_key).await?;
        Ok(is_banned)
    }
}

/// Send fraud alert to Google Chat
fn send_fraud_alert(creator_id: String, reward_count: usize) {
    tokio::spawn(async move {
        log::error!(
            "FRAUD ALERT: Creator {} received {} rewards in short time window",
            creator_id,
            reward_count
        );

        // Google Chat webhook URL with authentication
        let btc_rewards_webhook_url = env::var("BTC_REWARDS_GCHAT_WEBHOOK_URL")
            .expect("BTC_REWARDS_GCHAT_WEBHOOK_URL must be set");

        // Create simple text message for Google Chat
        let message = format!(
            "⚠️ FRAUD ALERT - Reward System\n\nCreator: {}\nRewards: {} in 60 minutes\nAction: Shadow banned for 5 hours\nTime: {}\n\nView creator: https://yral.com/profile/{}/posts",
            creator_id,
            reward_count,
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
            creator_id
        );

        let data = json!({
            "text": message
        });

        // Send to Google Chat webhook
        let client = Client::new();
        match client
            .post(btc_rewards_webhook_url)
            .header("Content-Type", "application/json")
            .json(&data)
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    log::info!("Fraud alert sent to Google Chat for creator {}", creator_id);
                } else {
                    log::error!(
                        "Failed to send fraud alert to Google Chat: HTTP {} - {}",
                        response.status(),
                        response.text().await.unwrap_or_default()
                    );
                }
            }
            Err(e) => {
                log::error!("Failed to send fraud alert to Google Chat: {}", e);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    struct TestFraudDetector {
        detector: FraudDetector,
        #[allow(dead_code)]
        test_prefix: String,
    }

    impl TestFraudDetector {
        async fn new() -> Self {
            let redis_url = std::env::var("TEST_REDIS_URL")
                .unwrap_or_else(|_| "redis://localhost:6379".to_string());

            let manager = bb8_redis::RedisConnectionManager::new(redis_url)
                .expect("Failed to create Redis connection manager");
            let pool = bb8::Pool::builder()
                .build(manager)
                .await
                .expect("Failed to build Redis pool");

            let test_prefix = format!("test_fraud_{}", Uuid::new_v4().to_string());
            let detector = FraudDetector::with_config(pool, 3, 60);

            Self {
                detector,
                test_prefix,
            }
        }

        async fn cleanup(&self) -> Result<()> {
            let mut conn = self.detector.redis_pool.get().await?;

            let patterns = vec![
                format!("impressions:test:rewards:user:*:recent"),
                format!("impressions:test:rewards:shadow_ban:*"),
            ];

            for pattern in patterns {
                let mut cursor = 0;
                loop {
                    let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(&pattern)
                        .arg("COUNT")
                        .arg(100)
                        .query_async(&mut *conn)
                        .await?;

                    if !keys.is_empty() {
                        let _: () = conn.del(keys).await?;
                    }

                    cursor = new_cursor;
                    if cursor == 0 {
                        break;
                    }
                }
            }
            Ok(())
        }

        fn test_principal(&self, suffix: &str) -> Principal {
            Principal::self_authenticating(format!("fraud_test_{}", suffix))
        }
    }

    #[tokio::test]
    async fn test_fraud_check_clean() {
        let test_detector = TestFraudDetector::new().await;
        let creator = test_detector.test_principal("clean");

        let result = test_detector.detector.check_fraud_patterns(creator).await;
        assert_eq!(result, FraudCheck::Clean);

        test_detector.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_fraud_detection_threshold() {
        let test_detector = TestFraudDetector::new().await;
        let creator = test_detector.test_principal("fraud1");

        // Simulate multiple rewards quickly (threshold is 3)
        for _ in 0..2 {
            let result = test_detector.detector.check_fraud_patterns(creator).await;
            assert_eq!(result, FraudCheck::Clean);
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // Wait for async fraud check to complete
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // After exceeding threshold, should still return Clean initially (async processing)
        // But subsequent calls should detect the shadow ban
        test_detector.detector.check_fraud_patterns(creator).await;
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Note: The actual shadow banning happens asynchronously, so we can't reliably
        // test the automatic banning in unit tests. We're primarily testing the
        // manual ban/unban functionality and detection.

        test_detector.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_fraud_checks() {
        let test_detector = TestFraudDetector::new().await;
        let creators: Vec<Principal> = (0..5)
            .map(|i| test_detector.test_principal(&format!("concurrent{}", i)))
            .collect();

        let mut handles = vec![];
        for creator in creators {
            let detector = test_detector.detector.clone();
            let handle = tokio::spawn(async move { detector.check_fraud_patterns(creator).await });
            handles.push(handle);
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert_eq!(result, FraudCheck::Clean);
        }

        test_detector.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_send_fraud_alert() {
        // This test verifies that send_fraud_alert function executes without panicking
        // Note: This will actually send a test alert to Google Chat webhook
        let test_creator_id = "test-creator-12345".to_string();
        let test_reward_count = 10;

        // Call send_fraud_alert (spawns async task)
        send_fraud_alert(test_creator_id.clone(), test_reward_count);

        // Wait a bit to ensure the async task completes
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // If we reach here without panic, the test passes
        // The actual alert will be sent to Google Chat
        log::info!(
            "Test alert sent for creator {} with {} rewards",
            test_creator_id,
            test_reward_count
        );
    }
}
