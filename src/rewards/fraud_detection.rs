use crate::{consts::GOOGLE_CHAT_REPORT_SPACE_URL, offchain_service::send_message_gchat, types::RedisPool};
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

    pub fn with_config(
        redis_pool: RedisPool,
        threshold: usize,
        shadow_ban_duration: u64,
    ) -> Self {
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
                    let recent_count = recent_timestamps
                        .iter()
                        .filter(|&&ts| ts > cutoff)
                        .count();

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

    /// Manually shadow ban a creator
    pub async fn shadow_ban(&self, creator_id: &Principal, duration_seconds: u64) -> Result<()> {
        let mut conn = self.redis_pool.get().await?;
        let ban_key = format!("rewards:shadow_ban:{}", creator_id);
        conn.set_ex(&ban_key, "1", duration_seconds).await?;
        log::info!("Manually shadow banned creator {} for {} seconds", creator_id, duration_seconds);
        Ok(())
    }

    /// Remove shadow ban for a creator
    pub async fn remove_shadow_ban(&self, creator_id: &Principal) -> Result<()> {
        let mut conn = self.redis_pool.get().await?;
        let ban_key = format!("rewards:shadow_ban:{}", creator_id);
        conn.del(&ban_key).await?;
        log::info!("Removed shadow ban for creator {}", creator_id);
        Ok(())
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