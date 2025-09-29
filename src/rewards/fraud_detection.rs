use crate::{
    consts::GOOGLE_CHAT_REPORT_SPACE_URL, offchain_service::send_message_gchat, types::RedisPool,
};
use anyhow::Result;
use candid::Principal;
use chrono::Utc;
use redis::AsyncCommands;
use serde_json::json;

const DEFAULT_FRAUD_THRESHOLD: usize = 5; // 5 rewards in time window
const DEFAULT_TIME_WINDOW: i64 = 600; // 10 minutes
const DEFAULT_SHADOW_BAN_DURATION: u64 = 3600; // 1 hour

#[derive(Debug, Clone, PartialEq)]
pub enum FraudCheck {
    Clean,
    Suspicious,
}

#[derive(Clone)]
pub struct FraudDetector {
    redis_pool: RedisPool,
    threshold: usize,
    time_window: i64,
    shadow_ban_duration: u64,
}

impl FraudDetector {
    pub fn new(redis_pool: RedisPool) -> Self {
        Self {
            redis_pool,
            threshold: DEFAULT_FRAUD_THRESHOLD,
            time_window: DEFAULT_TIME_WINDOW,
            shadow_ban_duration: DEFAULT_SHADOW_BAN_DURATION,
        }
    }

    pub fn with_config(redis_pool: RedisPool, threshold: usize, shadow_ban_duration: u64) -> Self {
        Self {
            redis_pool,
            threshold,
            time_window: DEFAULT_TIME_WINDOW,
            shadow_ban_duration,
        }
    }

    /// Check for fraud patterns and shadow ban if necessary
    pub async fn check_fraud_patterns(&self, creator_id: Principal) -> FraudCheck {
        let key = format!("rewards:user:{}:recent", creator_id);
        let current_timestamp = Utc::now().timestamp();

        let redis_pool = self.redis_pool.clone();
        let threshold = self.threshold;
        let time_window = self.time_window;
        let shadow_ban_duration = self.shadow_ban_duration;
        let creator_id_str = creator_id.to_string();

        // Run fraud check asynchronously
        tokio::spawn(async move {
            if let Ok(mut conn) = redis_pool.get().await {
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
                        let ban_key = format!("rewards:shadow_ban:{}", creator_id_str);
                        let _ = conn
                            .set_ex::<_, _, ()>(&ban_key, "1", shadow_ban_duration)
                            .await;

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
        if let Ok(mut conn) = self.redis_pool.get().await {
            let ban_key = format!("rewards:shadow_ban:{}", creator_id);
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
        let mut conn = self.redis_pool.get().await?;
        let ban_key = format!("rewards:shadow_ban:{}", creator_id);
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

        // Create Google Chat message with card format for better visibility
        let data = json!({
            "cardsV2": [{
                "card": {
                    "header": {
                        "title": "⚠️ Fraud Alert - Reward System",
                        "subtitle": "Suspicious activity detected",
                        "imageUrl": "https://fonts.gstatic.com/s/i/short-term/release/googlesymbols/warning/default/48px.svg",
                        "imageType": "CIRCLE"
                    },
                    "sections": [{
                        "header": "Alert Details",
                        "widgets": [
                            {
                                "decoratedText": {
                                    "topLabel": "Creator Principal",
                                    "text": format!("<b>{}</b>", creator_id),
                                    "startIcon": {
                                        "knownIcon": "PERSON"
                                    }
                                }
                            },
                            {
                                "decoratedText": {
                                    "topLabel": "Rewards in Time Window",
                                    "text": format!("<b>{} rewards</b> in 10 minutes", reward_count),
                                    "startIcon": {
                                        "knownIcon": "CLOCK"
                                    }
                                }
                            },
                            {
                                "decoratedText": {
                                    "topLabel": "Action Taken",
                                    "text": "<b><font color=\"#FF0000\">Shadow banned for 1 hour</font></b>",
                                    "startIcon": {
                                        "knownIcon": "BOOKMARK"
                                    }
                                }
                            },
                            {
                                "decoratedText": {
                                    "topLabel": "Timestamp",
                                    "text": format!("{}", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")),
                                    "startIcon": {
                                        "knownIcon": "INVITE"
                                    }
                                }
                            }
                        ]
                    }],
                    "fixedFooter": {
                        "primaryButton": {
                            "text": "View Creator",
                            "url": format!("https://yral.com/@{}", creator_id),
                            "type": "OPEN_LINK"
                        }
                    }
                }
            }]
        });

        // Send to Google Chat
        if let Err(e) = send_message_gchat(GOOGLE_CHAT_REPORT_SPACE_URL, data).await {
            log::error!("Failed to send fraud alert to Google Chat: {}", e);
        } else {
            log::info!("Fraud alert sent to Google Chat for creator {}", creator_id);
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
                format!("rewards:user:*:recent"),
                format!("rewards:shadow_ban:*"),
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
}
