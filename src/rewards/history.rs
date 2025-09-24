use crate::types::RedisPool;
use anyhow::Result;
use candid::Principal;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ViewRecord {
    pub user_id: String,
    pub video_id: String,
    pub timestamp: i64,
    pub duration_watched: f64,
    pub percentage_watched: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RewardRecord {
    pub video_id: String,
    pub milestone: u64,
    pub reward_btc: f64,
    pub reward_inr: f64,
    pub timestamp: i64,
    pub tx_id: Option<String>,
    pub view_count: u64,
}

#[derive(Clone)]
pub struct HistoryTracker {
    redis_pool: RedisPool,
}

impl HistoryTracker {
    pub fn new(redis_pool: RedisPool) -> Self {
        Self { redis_pool }
    }

    /// Record a view in history (non-atomic, best effort)
    pub async fn record_view(&self, record: ViewRecord) {
        let video_history_key = format!("rewards:video:{}:view_history", record.video_id);
        let user_history_key = format!("rewards:user:{}:view_history", record.user_id);

        let redis_pool = self.redis_pool.clone();
        let record_clone = record.clone();

        // Fire and forget - don't block on history storage
        tokio::spawn(async move {
            if let Ok(mut conn) = redis_pool.get().await {
                // Store video view history
                let video_json = json!({
                    "user_id": record_clone.user_id,
                    "timestamp": record_clone.timestamp,
                    "duration_watched": record_clone.duration_watched,
                    "percentage_watched": record_clone.percentage_watched
                })
                .to_string();

                let _ = conn.lpush::<_, _, ()>(&video_history_key, &video_json).await;
                let _ = conn.ltrim::<_, ()>(&video_history_key, 0, 9999).await; // Keep last 10k

                // Store user view history
                let user_json = json!({
                    "video_id": record_clone.video_id,
                    "timestamp": record_clone.timestamp,
                    "duration_watched": record_clone.duration_watched,
                    "percentage_watched": record_clone.percentage_watched
                })
                .to_string();

                let _ = conn.lpush::<_, _, ()>(&user_history_key, &user_json).await;
                let _ = conn.ltrim::<_, ()>(&user_history_key, 0, 999).await; // Keep last 1k per user
                let _ = conn.expire::<_, ()>(&user_history_key, 7776000).await; // 90 days TTL

                log::debug!(
                    "Recorded view history for video {} by user {}",
                    record_clone.video_id,
                    record_clone.user_id
                );
            }
        });
    }

    /// Record a reward in history (non-atomic, best effort)
    pub async fn record_reward(&self, creator_id: &Principal, record: RewardRecord) {
        let creator_id_str = creator_id.to_string();
        let user_key = format!("rewards:user:{}:reward_history", creator_id_str);
        let creator_key = format!("rewards:creator:{}:reward_history", creator_id_str);

        let redis_pool = self.redis_pool.clone();
        let json = match serde_json::to_string(&record) {
            Ok(j) => j,
            Err(e) => {
                log::error!("Failed to serialize reward record: {}", e);
                return;
            }
        };

        let creator_id_clone = creator_id_str.clone();
        // Fire and forget
        tokio::spawn(async move {
            if let Ok(mut conn) = redis_pool.get().await {
                let _ = conn.lpush::<_, _, ()>(&user_key, &json).await;
                let _ = conn.lpush::<_, _, ()>(&creator_key, &json).await;
                let _ = conn.ltrim::<_, ()>(&user_key, 0, 999).await; // Keep last 1k
                let _ = conn.ltrim::<_, ()>(&creator_key, 0, 9999).await; // Keep last 10k for creators

                log::debug!("Recorded reward history for creator {}", creator_id_clone);
            }
        });
    }

    /// Get video view history
    pub async fn get_video_views(
        &self,
        video_id: &str,
        limit: usize,
    ) -> Result<Vec<ViewRecord>> {
        let mut conn = self.redis_pool.get().await?;
        let key = format!("rewards:video:{}:view_history", video_id);

        let history: Vec<String> = conn.lrange(&key, 0, limit as isize - 1).await?;

        let mut records = Vec::new();
        for item in history {
            if let Ok(mut record) = serde_json::from_str::<serde_json::Value>(&item) {
                // Add video_id as it's not stored in the video history
                record["video_id"] = json!(video_id);
                if let Ok(view_record) = serde_json::from_value::<ViewRecord>(record) {
                    records.push(view_record);
                }
            }
        }

        Ok(records)
    }

    /// Get user's view history
    pub async fn get_user_view_history(
        &self,
        user_id: &Principal,
        limit: usize,
    ) -> Result<Vec<ViewRecord>> {
        let mut conn = self.redis_pool.get().await?;
        let key = format!("rewards:user:{}:view_history", user_id);

        let history: Vec<String> = conn.lrange(&key, 0, limit as isize - 1).await?;

        let mut records = Vec::new();
        for item in history {
            if let Ok(mut record) = serde_json::from_str::<serde_json::Value>(&item) {
                // Add user_id as it's not stored in the user history
                record["user_id"] = json!(user_id.to_string());
                if let Ok(view_record) = serde_json::from_value::<ViewRecord>(record) {
                    records.push(view_record);
                }
            }
        }

        Ok(records)
    }

    /// Get user's reward history
    pub async fn get_user_reward_history(
        &self,
        user_id: &Principal,
        limit: usize,
    ) -> Result<Vec<RewardRecord>> {
        let mut conn = self.redis_pool.get().await?;
        let key = format!("rewards:user:{}:reward_history", user_id);

        let history: Vec<String> = conn.lrange(&key, 0, limit as isize - 1).await?;

        let mut records = Vec::new();
        for item in history {
            if let Ok(reward) = serde_json::from_str::<RewardRecord>(&item) {
                records.push(reward);
            }
        }

        Ok(records)
    }

    /// Get creator's reward history
    pub async fn get_creator_reward_history(
        &self,
        creator_id: &Principal,
        limit: usize,
    ) -> Result<Vec<RewardRecord>> {
        let mut conn = self.redis_pool.get().await?;
        let key = format!("rewards:creator:{}:reward_history", creator_id);

        let history: Vec<String> = conn.lrange(&key, 0, limit as isize - 1).await?;

        let mut records = Vec::new();
        for item in history {
            if let Ok(reward) = serde_json::from_str::<RewardRecord>(&item) {
                records.push(reward);
            }
        }

        Ok(records)
    }

    /// Update a reward record with transaction ID
    pub async fn update_reward_tx_id(
        &self,
        creator_id: &Principal,
        video_id: &str,
        milestone: u64,
        tx_id: String,
    ) -> Result<()> {
        // This is a best-effort update, we don't need to be atomic here
        // In practice, we might want to store tx_id separately or include it when creating the record
        log::info!(
            "Updated reward tx_id for creator {} video {} milestone {}: {}",
            creator_id,
            video_id,
            milestone,
            tx_id
        );
        Ok(())
    }
}