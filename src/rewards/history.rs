use crate::yral_auth::dragonfly::DragonflyPool;
use anyhow::Result;
use candid::Principal;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
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
    dragonfly_pool: Arc<DragonflyPool>,
}

impl HistoryTracker {
    pub fn new(dragonfly_pool: Arc<DragonflyPool>) -> Self {
        Self { dragonfly_pool }
    }

    /// Record a view in history (non-atomic, best effort)
    pub async fn record_view(&self, record: ViewRecord) {
        let video_history_key =
            format!("impressions:rewards:video:{}:view_history", record.video_id);
        let user_history_key = format!("impressions:rewards:user:{}:view_history", record.user_id);

        let dragonfly_pool = self.dragonfly_pool.clone();
        let record_clone = record.clone();

        // Fire and forget - don't block on history storage
        tokio::spawn(async move {
            // Store video view history
            let video_json = json!({
                "user_id": record_clone.user_id,
                "timestamp": record_clone.timestamp,
                "duration_watched": record_clone.duration_watched,
                "percentage_watched": record_clone.percentage_watched
            })
            .to_string();

            let video_key = video_history_key.clone();
            let video_json_clone = video_json.clone();
            let _ = dragonfly_pool
                .execute_with_retry(|mut conn| {
                    let key = video_key.clone();
                    let json = video_json_clone.clone();
                    async move {
                        conn.lpush::<_, _, ()>(&key, &json).await?;
                        conn.ltrim::<_, ()>(&key, 0, 9999).await // Keep last 10k
                    }
                })
                .await;

            // Store user view history
            let user_json = json!({
                "video_id": record_clone.video_id,
                "timestamp": record_clone.timestamp,
                "duration_watched": record_clone.duration_watched,
                "percentage_watched": record_clone.percentage_watched
            })
            .to_string();

            let user_key_clone = user_history_key.clone();
            let user_json_clone = user_json.clone();
            let _ = dragonfly_pool
                .execute_with_retry(|mut conn| {
                    let key = user_key_clone.clone();
                    let json = user_json_clone.clone();
                    async move {
                        conn.lpush::<_, _, ()>(&key, &json).await?;
                        conn.ltrim::<_, ()>(&key, 0, 999).await?; // Keep last 1k per user
                        conn.expire::<_, ()>(&key, 7776000).await // 90 days TTL
                    }
                })
                .await;

            log::debug!(
                "Recorded view history for video {} by user {}",
                record_clone.video_id,
                record_clone.user_id
            );
        });
    }

    /// Record a reward in history (non-atomic, best effort)
    pub async fn record_reward(&self, creator_id: &Principal, record: RewardRecord) {
        let creator_id_str = creator_id.to_string();
        let user_key = format!("impressions:rewards:user:{}:reward_history", creator_id_str);
        let creator_key = format!(
            "impressions:rewards:creator:{}:reward_history",
            creator_id_str
        );

        let dragonfly_pool = self.dragonfly_pool.clone();
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
            let user_key_clone = user_key.clone();
            let creator_key_clone = creator_key.clone();
            let json_clone = json.clone();

            let _ = dragonfly_pool
                .execute_with_retry(|mut conn| {
                    let u_key = user_key_clone.clone();
                    let c_key = creator_key_clone.clone();
                    let j = json_clone.clone();
                    async move {
                        conn.lpush::<_, _, ()>(&u_key, &j).await?;
                        conn.lpush::<_, _, ()>(&c_key, &j).await?;
                        conn.ltrim::<_, ()>(&u_key, 0, 999).await?; // Keep last 1k
                        conn.ltrim::<_, ()>(&c_key, 0, 9999).await // Keep last 10k for creators
                    }
                })
                .await;

            log::debug!("Recorded reward history for creator {}", creator_id_clone);
        });
    }

    /// Get video view history
    pub async fn get_video_views(&self, video_id: &str, limit: usize) -> Result<Vec<ViewRecord>> {
        let key = format!("impressions:rewards:video:{}:view_history", video_id);

        let history: Vec<String> = self
            .dragonfly_pool
            .execute_with_retry(|mut conn| {
                let k = key.clone();
                let lim = limit as isize;
                async move { conn.lrange(&k, 0, lim - 1).await }
            })
            .await?;

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
        let key = format!("impressions:rewards:user:{}:view_history", user_id);
        let user_id_str = user_id.to_string();

        let history: Vec<String> = self
            .dragonfly_pool
            .execute_with_retry(|mut conn| {
                let k = key.clone();
                let lim = limit as isize;
                async move { conn.lrange(&k, 0, lim - 1).await }
            })
            .await?;

        let mut records = Vec::new();
        for item in history {
            if let Ok(mut record) = serde_json::from_str::<serde_json::Value>(&item) {
                // Add user_id as it's not stored in the user history
                record["user_id"] = json!(user_id_str.clone());
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
        let key = format!("impressions:rewards:user:{}:reward_history", user_id);

        let history: Vec<String> = self
            .dragonfly_pool
            .execute_with_retry(|mut conn| {
                let k = key.clone();
                let lim = limit as isize;
                async move { conn.lrange(&k, 0, lim - 1).await }
            })
            .await?;

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
        let key = format!("impressions:rewards:creator:{}:reward_history", creator_id);

        let history: Vec<String> = self
            .dragonfly_pool
            .execute_with_retry(|mut conn| {
                let k = key.clone();
                let lim = limit as isize;
                async move { conn.lrange(&k, 0, lim - 1).await }
            })
            .await?;

        let mut records = Vec::new();
        for item in history {
            if let Ok(reward) = serde_json::from_str::<RewardRecord>(&item) {
                records.push(reward);
            }
        }

        Ok(records)
    }
}
