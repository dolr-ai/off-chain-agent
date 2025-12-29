use anyhow::{Context, Result};
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::env;

/// Internal keys for temporary/scratchpad data
pub mod internal_keys {
    pub const NSFW_V2_PENDING_BATCH: &str = "offchain_internal_video_processing:temp:nsfw_v2_batch";
}

/// Pending NSFW v2 batch item for batched BigQuery processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingNsfwV2Item {
    pub video_id: String,
    pub nsfw_prob: f32,
    pub is_nsfw: bool,
}

#[derive(Clone)]
pub struct ScratchpadClient {
    client: redis::Client,
}

impl ScratchpadClient {
    pub async fn get_connection(&self) -> Result<MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to get scratchpad redis connection")
    }

    // ============ NSFW V2 Batch Methods ============

    /// Add an item to the NSFW v2 pending batch. Returns the new batch size.
    pub async fn add_to_nsfw_v2_batch(&self, item: &PendingNsfwV2Item) -> Result<usize> {
        let mut conn = self.get_connection().await?;
        let json_str = serde_json::to_string(item)?;
        conn.hset::<_, _, _, ()>(
            internal_keys::NSFW_V2_PENDING_BATCH,
            &item.video_id,
            json_str,
        )
        .await?;
        let count: usize = conn.hlen(internal_keys::NSFW_V2_PENDING_BATCH).await?;
        Ok(count)
    }

    /// Get the current batch size
    pub async fn get_nsfw_v2_batch_size(&self) -> Result<usize> {
        let mut conn = self.get_connection().await?;
        let count: usize = conn.hlen(internal_keys::NSFW_V2_PENDING_BATCH).await?;
        Ok(count)
    }

    /// Get all items in the pending batch
    pub async fn get_nsfw_v2_batch_items(&self) -> Result<Vec<PendingNsfwV2Item>> {
        let mut conn = self.get_connection().await?;
        let items: std::collections::HashMap<String, String> =
            conn.hgetall(internal_keys::NSFW_V2_PENDING_BATCH).await?;

        let mut result = Vec::with_capacity(items.len());
        for (_video_id, json_str) in items {
            if let Ok(item) = serde_json::from_str::<PendingNsfwV2Item>(&json_str) {
                result.push(item);
            }
        }
        Ok(result)
    }

    /// Clear the pending batch
    pub async fn clear_nsfw_v2_batch(&self) -> Result<()> {
        let mut conn = self.get_connection().await?;
        conn.del::<_, ()>(internal_keys::NSFW_V2_PENDING_BATCH)
            .await?;
        Ok(())
    }

    /// Atomically get all items and clear the batch
    pub async fn take_nsfw_v2_batch(&self) -> Result<Vec<PendingNsfwV2Item>> {
        let items = self.get_nsfw_v2_batch_items().await?;
        if !items.is_empty() {
            self.clear_nsfw_v2_batch().await?;
        }
        Ok(items)
    }

    // ============ Generic Redis Methods ============

    pub async fn set_json<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let json_str = serde_json::to_string(value)?;
        conn.set::<_, _, ()>(key, json_str).await?;
        Ok(())
    }

    pub async fn set_json_ex<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        ttl_secs: u64,
    ) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let json_str = serde_json::to_string(value)?;
        conn.set_ex::<_, _, ()>(key, json_str, ttl_secs).await?;
        Ok(())
    }

    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let mut conn = self.get_connection().await?;
        let value: Option<String> = conn.get(key).await?;
        match value {
            Some(json_str) => {
                let parsed = serde_json::from_str(&json_str)?;
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }

    pub async fn del(&self, key: &str) -> Result<()> {
        let mut conn = self.get_connection().await?;
        conn.del::<_, ()>(key).await?;
        Ok(())
    }

    pub async fn exists(&self, key: &str) -> Result<bool> {
        let mut conn = self.get_connection().await?;
        let exists: bool = conn.exists(key).await?;
        Ok(exists)
    }

    pub async fn hset<T: Serialize>(&self, key: &str, field: &str, value: &T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let json_str = serde_json::to_string(value)?;
        conn.hset::<_, _, _, ()>(key, field, json_str).await?;
        Ok(())
    }

    pub async fn hget<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
        field: &str,
    ) -> Result<Option<T>> {
        let mut conn = self.get_connection().await?;
        let value: Option<String> = conn.hget(key, field).await?;
        match value {
            Some(json_str) => {
                let parsed = serde_json::from_str(&json_str)?;
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }

    pub async fn lpush<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let json_str = serde_json::to_string(value)?;
        conn.lpush::<_, _, ()>(key, json_str).await?;
        Ok(())
    }
}

pub async fn init_scratchpad_client() -> Result<ScratchpadClient> {
    let host = env::var("SCRATCHPAD_DRAGONFLY_HOST")
        .context("SCRATCHPAD_DRAGONFLY_HOST environment variable not set")?;
    let port: u16 = env::var("SCRATCHPAD_DRAGONFLY_PORT")
        .context("SCRATCHPAD_DRAGONFLY_PORT environment variable not set")?
        .parse()
        .context("SCRATCHPAD_DRAGONFLY_PORT must be a valid port number")?;
    let password = env::var("SCRATCHPAD_DRAGONFLY_PASSWORD")
        .context("SCRATCHPAD_DRAGONFLY_PASSWORD environment variable not set")?;

    // Build Redis URL: redis://:password@host:port
    // No TLS (redis:// not rediss://), no cluster
    // URL encode password in case it contains special characters
    let encoded_password = urlencoding::encode(&password);
    let redis_url = format!("redis://:{}@{}:{}", encoded_password, host, port);

    log::info!("Connecting to scratchpad Dragonfly at {}:{}", host, port);

    let client = redis::Client::open(redis_url.as_str())
        .context("Failed to create scratchpad redis client")?;

    // Test the connection
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("Failed to connect to scratchpad Dragonfly")?;

    let pong: String = redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .context("Failed to ping scratchpad Dragonfly")?;

    if pong != "PONG" {
        anyhow::bail!("Unexpected ping response from scratchpad: {}", pong);
    }

    log::info!(
        "Successfully connected to scratchpad Dragonfly at {}:{}",
        host,
        port
    );

    Ok(ScratchpadClient { client })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scratchpad_connection() {
        if env::var("SCRATCHPAD_DRAGONFLY_HOST").is_err() {
            println!("Skipping test: SCRATCHPAD_DRAGONFLY_HOST not set");
            return;
        }

        let client = init_scratchpad_client()
            .await
            .expect("Failed to init scratchpad client");

        let test_key = "test:scratchpad_connection_check";
        let test_data =
            serde_json::json!({"test": "value", "timestamp": chrono::Utc::now().to_rfc3339()});

        client
            .set_json(test_key, &test_data)
            .await
            .expect("Failed to set test key");

        let result: Option<serde_json::Value> = client
            .get_json(test_key)
            .await
            .expect("Failed to get test key");
        assert!(result.is_some(), "Test key should exist");

        client
            .del(test_key)
            .await
            .expect("Failed to delete test key");

        println!("Scratchpad Dragonfly connection test passed!");
    }
}
