use anyhow::{Context, Result};
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use rustls_pemfile;
use serde::Serialize;
use std::env;
use std::io::BufReader;

/// Kvrocks cluster configuration
const KVROCKS_MASTER_1: &str = "136.243.150.223";
const KVROCKS_MASTER_2: &str = "138.201.128.44";
const KVROCKS_TLS_PORT: u16 = 6666;

/// Key prefixes for different data types (matching BigQuery tables)
pub mod keys {
    pub const TEST_EVENTS_ANALYTICS: &str = "test_events_analytics";
    pub const VIDEO_NSFW: &str = "video_nsfw";
    pub const VIDEO_NSFW_AGG: &str = "video_nsfw_agg";
    pub const VIDEO_EMBEDDINGS_AGG: &str = "video_embeddings_agg";
    pub const VIDEOHASH_PHASH: &str = "videohash_phash";
    pub const VIDEOHASH_ORIGINAL: &str = "videohash_original";
    pub const VIDEO_UNIQUE: &str = "video_unique";
    pub const VIDEO_UNIQUE_V2: &str = "video_unique_v2";
    pub const VIDEO_DELETED: &str = "video_deleted";
    pub const VIDEO_DEDUP_STATUS: &str = "video_dedup_status";
    pub const UGC_CONTENT_APPROVAL: &str = "ugc_content_approval";
}

/// Kvrocks client wrapper for the cluster
#[derive(Clone)]
pub struct KvrocksClient {
    client: Client,
}

impl KvrocksClient {
    /// Get a multiplexed connection for async operations
    pub async fn get_connection(&self) -> Result<MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to get kvrocks connection")
    }

    /// Store a JSON value with a key
    pub async fn set_json<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let json_str = serde_json::to_string(value)?;
        conn.set::<_, _, ()>(key, json_str).await?;
        Ok(())
    }

    /// Store a JSON value with a key and expiration (in seconds)
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

    /// Store data in a hash
    pub async fn hset<T: Serialize>(&self, key: &str, field: &str, value: &T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let json_str = serde_json::to_string(value)?;
        conn.hset::<_, _, _, ()>(key, field, json_str).await?;
        Ok(())
    }

    /// Store multiple fields in a hash
    pub async fn hset_multiple<T: Serialize>(
        &self,
        key: &str,
        items: &[(String, T)],
    ) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let fields: Vec<(String, String)> = items
            .iter()
            .filter_map(|(field, value)| {
                serde_json::to_string(value)
                    .ok()
                    .map(|v| (field.clone(), v))
            })
            .collect();

        if !fields.is_empty() {
            conn.hset_multiple::<_, _, _, ()>(key, &fields).await?;
        }
        Ok(())
    }

    /// Add to a sorted set (for time-series data)
    pub async fn zadd<T: Serialize>(&self, key: &str, score: f64, value: &T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let json_str = serde_json::to_string(value)?;
        conn.zadd::<_, _, _, ()>(key, json_str, score).await?;
        Ok(())
    }

    /// Push to a list (for event streams)
    pub async fn lpush<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let json_str = serde_json::to_string(value)?;
        conn.lpush::<_, _, ()>(key, json_str).await?;
        Ok(())
    }

    /// Delete a key
    pub async fn del(&self, key: &str) -> Result<()> {
        let mut conn = self.get_connection().await?;
        conn.del::<_, ()>(key).await?;
        Ok(())
    }

    /// Check if a key exists
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let mut conn = self.get_connection().await?;
        let exists: bool = conn.exists(key).await?;
        Ok(exists)
    }

    /// Get a JSON value
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
}

/// Initialize the kvrocks client with TLS
pub async fn init_kvrocks_client() -> Result<KvrocksClient> {
    let password =
        env::var("KVROCKS_PASSWORD").context("KVROCKS_PASSWORD environment variable not set")?;

    // Build Redis URL with TLS
    let redis_url = format!(
        "rediss://default:{}@{}:{}",
        password, KVROCKS_MASTER_1, KVROCKS_TLS_PORT
    );

    // Create client with custom TLS config
    let client = Client::build_with_tls(
        redis_url,
        redis::TlsCertificates {
            client_tls: Some(redis::ClientTlsConfig {
                client_cert: rustls_pemfile::certs(&mut BufReader::new(
                    &get_client_cert_pem()?[..],
                ))
                .collect::<Result<Vec<_>, _>>()
                .context("Failed to parse client cert")?
                .into_iter()
                .next()
                .context("No client certificate found")?,
                client_key: rustls_pemfile::private_key(&mut BufReader::new(
                    &get_client_key_pem()?[..],
                ))
                .context("Failed to parse client key")?
                .context("No private key found")?,
            }),
            root_cert: Some(
                rustls_pemfile::certs(&mut BufReader::new(&get_ca_cert_pem()?[..]))
                    .collect::<Result<Vec<_>, _>>()
                    .context("Failed to parse CA cert")?
                    .into_iter()
                    .next()
                    .context("No CA certificate found")?,
            ),
        },
    )
    .context("Failed to build kvrocks client with TLS")?;

    // Test connection
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("Failed to connect to kvrocks")?;

    // Ping to verify connection
    let pong: String = redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .context("Failed to ping kvrocks")?;

    if pong != "PONG" {
        anyhow::bail!("Unexpected ping response from kvrocks: {}", pong);
    }

    log::info!(
        "Successfully connected to kvrocks at {}:{}",
        KVROCKS_MASTER_1,
        KVROCKS_TLS_PORT
    );

    Ok(KvrocksClient { client })
}

/// Helper to get CA cert PEM bytes
fn get_ca_cert_pem() -> Result<Vec<u8>> {
    Ok(env::var("KVROCKS_CA_CERT")
        .context("KVROCKS_CA_CERT env var not set")?
        .into_bytes())
}

/// Helper to get client cert PEM bytes
fn get_client_cert_pem() -> Result<Vec<u8>> {
    Ok(env::var("KVROCKS_CLIENT_CERT")
        .context("KVROCKS_CLIENT_CERT env var not set")?
        .into_bytes())
}

/// Helper to get client key PEM bytes
fn get_client_key_pem() -> Result<Vec<u8>> {
    Ok(env::var("KVROCKS_CLIENT_KEY")
        .context("KVROCKS_CLIENT_KEY env var not set")?
        .into_bytes())
}

/// Helper functions for storing data to kvrocks (matching BigQuery tables)
impl KvrocksClient {
    /// Store event analytics data
    pub async fn store_event(&self, event_id: &str, event_data: &serde_json::Value) -> Result<()> {
        let key = format!("{}:{}", keys::TEST_EVENTS_ANALYTICS, event_id);
        self.set_json(&key, event_data).await
    }

    /// Store NSFW data for a video
    pub async fn store_video_nsfw(
        &self,
        video_id: &str,
        nsfw_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_NSFW, video_id);
        self.set_json(&key, nsfw_data).await
    }

    /// Store aggregated NSFW data
    pub async fn store_video_nsfw_agg(
        &self,
        video_id: &str,
        data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_NSFW_AGG, video_id);
        self.set_json(&key, data).await
    }

    /// Store video embeddings aggregated data
    pub async fn store_video_embeddings_agg(
        &self,
        video_id: &str,
        data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_EMBEDDINGS_AGG, video_id);
        self.set_json(&key, data).await
    }

    /// Store phash data for a video
    pub async fn store_videohash_phash(
        &self,
        video_id: &str,
        phash_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEOHASH_PHASH, video_id);
        self.set_json(&key, phash_data).await
    }

    /// Store original videohash
    pub async fn store_videohash_original(
        &self,
        video_id: &str,
        hash_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEOHASH_ORIGINAL, video_id);
        self.set_json(&key, hash_data).await
    }

    /// Store unique video record
    pub async fn store_video_unique(&self, video_id: &str, data: &serde_json::Value) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_UNIQUE, video_id);
        self.set_json(&key, data).await
    }

    /// Store unique video record v2
    pub async fn store_video_unique_v2(
        &self,
        video_id: &str,
        data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_UNIQUE_V2, video_id);
        self.set_json(&key, data).await
    }

    /// Store deleted video record
    pub async fn store_video_deleted(
        &self,
        video_id: &str,
        delete_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_DELETED, video_id);
        self.set_json(&key, delete_data).await
    }

    /// Store dedup status for a video
    pub async fn store_video_dedup_status(
        &self,
        video_id: &str,
        status_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_DEDUP_STATUS, video_id);
        self.set_json(&key, status_data).await
    }

    /// Store dedup status with custom table suffix (for HAM thresholds)
    pub async fn store_video_dedup_status_with_table(
        &self,
        table_suffix: &str,
        video_id: &str,
        status_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}:{}", keys::VIDEO_DEDUP_STATUS, table_suffix, video_id);
        self.set_json(&key, status_data).await
    }

    /// Store UGC content approval record
    pub async fn store_ugc_content_approval(
        &self,
        video_id: &str,
        approval_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::UGC_CONTENT_APPROVAL, video_id);
        self.set_json(&key, approval_data).await
    }

    /// Update UGC content approval status
    pub async fn update_ugc_approval_status(
        &self,
        video_id: &str,
        is_approved: bool,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::UGC_CONTENT_APPROVAL, video_id);

        // Get existing data, update is_approved field
        if let Some(mut data) = self.get_json::<serde_json::Value>(&key).await? {
            if let Some(obj) = data.as_object_mut() {
                obj.insert(
                    "is_approved".to_string(),
                    serde_json::Value::Bool(is_approved),
                );
                self.set_json(&key, &data).await?;
            }
        }
        Ok(())
    }

    /// Delete UGC content approval record
    pub async fn delete_ugc_content_approval(&self, video_id: &str) -> Result<()> {
        let key = format!("{}:{}", keys::UGC_CONTENT_APPROVAL, video_id);
        self.del(&key).await
    }

    /// Delete video unique record
    pub async fn delete_video_unique(&self, video_id: &str) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_UNIQUE, video_id);
        self.del(&key).await
    }

    /// Delete video unique v2 record
    pub async fn delete_video_unique_v2(&self, video_id: &str) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_UNIQUE_V2, video_id);
        self.del(&key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_kvrocks_connection() {
        // Skip if env vars not set
        if env::var("KVROCKS_PASSWORD").is_err() {
            println!("Skipping test: KVROCKS_PASSWORD not set");
            return;
        }

        let client = init_kvrocks_client()
            .await
            .expect("Failed to init kvrocks client");

        // Test set and get
        let test_key = "test:connection_check";
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

        // Cleanup
        client
            .del(test_key)
            .await
            .expect("Failed to delete test key");

        println!("Kvrocks connection test passed!");
    }
}
