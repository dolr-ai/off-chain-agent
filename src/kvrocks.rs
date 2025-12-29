use anyhow::{Context, Result};
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::env;

const KVROCKS_TLS_PORT: u16 = 6666;

pub mod keys {
    pub const VIDEO_NSFW: &str = "offchain:video_nsfw";
    pub const VIDEO_DELETED: &str = "offchain:video_deleted";
    pub const VIDEO_UNIQUE_V2: &str = "offchain:video_unique_v2";
    pub const USER_UPLOADED_CONTENT_APPROVAL: &str = "offchain:user_uploaded_content_approval";
    pub const BOT_UPLOADED_AI_CONTENT: &str = "offchain:bot_uploaded_ai_content";
    pub const VIDEO_DEDUP_STATUS: &str = "offchain:video_dedup_status";
    pub const VIDEOHASH_PHASH: &str = "offchain:videohash_phash";
    pub const VIDEOHASH_ORIGINAL: &str = "offchain:videohash_original";
    pub const VIDEO_EMBEDDINGS: &str = "offchain:video_embeddings";
    pub const VIDEO_METADATA: &str = "offchain:metadata:video_details";
}

/// NSFW classification data for a video
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoNsfw {
    pub video_id: String,
    pub gcs_video_id: String,
    pub is_nsfw: bool,
    pub nsfw_ec: String,
    pub nsfw_gore: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probability: Option<f32>,
}

/// Perceptual hash data for a video
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideohashPhash {
    pub video_id: String,
    pub phash: String,
    pub num_frames: i32,
    pub hash_size: i32,
    pub duration: f64,
    pub width: i64,
    pub height: i64,
    pub fps: f64,
    pub created_at: String,
}

/// Original video hash data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideohashOriginal {
    pub video_id: String,
    pub videohash: String,
    pub created_at: String,
}

/// Unique video marker (v2)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoUniqueV2 {
    pub video_id: String,
    pub videohash: String,
    pub created_at: String,
}

/// Deleted video record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoDeleted {
    pub canister_id: String,
    pub post_id: String,
    pub video_id: String,
    pub gcs_video_id: String,
    pub deleted_at: String,
}

/// Video deduplication status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoDedupStatus {
    pub video_id: String,
    pub phash: String,
    pub is_duplicate: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duplicate_of: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hamming_distance: Option<u32>,
    pub ingested_at: String,
}

/// Video embeddings data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoEmbeddings {
    pub video_id: String,
    pub embedding_vectors: Vec<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// User uploaded content approval data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserUploadedContentApproval {
    pub video_id: String,
    pub post_id: String,
    pub canister_id: String,
    pub user_id: String,
    pub is_approved: bool,
    pub created_at: String,
}

/// Bot uploaded AI content marker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotUploadedAiContent {
    pub video_id: String,
    pub created_at: String,
}

/// Video metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoMetadata {
    pub video_id: String,
    pub post_id: String,
    pub publisher_user_id: String,
}

#[derive(Clone)]
pub struct KvrocksClient {
    client: ClusterClient,
}

impl KvrocksClient {
    pub async fn get_connection(&self) -> Result<ClusterConnection> {
        self.client
            .get_async_connection()
            .await
            .context("Failed to get kvrocks cluster connection")
    }

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

    pub async fn hset<T: Serialize>(&self, key: &str, field: &str, value: &T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let json_str = serde_json::to_string(value)?;
        conn.hset::<_, _, _, ()>(key, field, json_str).await?;
        Ok(())
    }

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

    pub async fn zadd<T: Serialize>(&self, key: &str, score: f64, value: &T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let json_str = serde_json::to_string(value)?;
        conn.zadd::<_, _, _, ()>(key, json_str, score).await?;
        Ok(())
    }

    pub async fn lpush<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let json_str = serde_json::to_string(value)?;
        conn.lpush::<_, _, ()>(key, json_str).await?;
        Ok(())
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

    /// Store a struct as a Redis HASH (each field becomes a hash field)
    pub async fn set_hash<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let mut conn = self.get_connection().await?;
        // Serialize to JSON Value to get field names
        let json_value = serde_json::to_value(value)?;
        let obj = json_value
            .as_object()
            .context("Value must serialize to a JSON object")?;

        let fields: Vec<(&str, String)> = obj
            .iter()
            .map(|(k, v)| {
                // Store primitives directly, objects as JSON strings
                let value_str = match v {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Null => String::new(),
                    other => other.to_string(),
                };
                (k.as_str(), value_str)
            })
            .collect();

        if !fields.is_empty() {
            conn.hset_multiple::<_, _, _, ()>(key, &fields).await?;
        }
        Ok(())
    }

    /// Get a struct from a Redis HASH
    pub async fn get_hash<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let mut conn = self.get_connection().await?;
        let hash: std::collections::HashMap<String, String> = conn.hgetall(key).await?;

        if hash.is_empty() {
            return Ok(None);
        }

        // Convert HashMap to JSON Value for deserialization
        let mut json_obj = serde_json::Map::new();
        for (k, v) in hash {
            // Try to parse as JSON value, fall back to string
            let json_val = serde_json::from_str(&v).unwrap_or(serde_json::Value::String(v));
            json_obj.insert(k, json_val);
        }

        let parsed = serde_json::from_value(serde_json::Value::Object(json_obj))?;
        Ok(Some(parsed))
    }
}

pub async fn init_kvrocks_client() -> Result<KvrocksClient> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let password = env::var("KVROCKS_PASSWORD")
        .context("KVROCKS_PASSWORD environment variable not set")?
        .trim()
        .to_string();
    let hosts_str = env::var("KVROCKS_HOSTS")
        .context("KVROCKS_HOSTS environment variable not set")?
        .trim()
        .to_string();

    let hosts: Vec<&str> = hosts_str
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if hosts.is_empty() {
        anyhow::bail!(
            "KVROCKS_HOSTS must contain at least one host, got: '{}'",
            hosts_str
        );
    }

    let encoded_password = urlencoding::encode(&password);
    let node_urls: Vec<String> = hosts
        .iter()
        .map(|host| {
            format!(
                "rediss://:{}@{}:{}",
                encoded_password, host, KVROCKS_TLS_PORT
            )
        })
        .collect();

    log::info!(
        "Connecting to kvrocks cluster with {} nodes",
        node_urls.len()
    );

    let tls_certs = redis::TlsCertificates {
        client_tls: Some(redis::ClientTlsConfig {
            client_cert: get_client_cert_pem()?,
            client_key: get_client_key_pem()?,
        }),
        root_cert: Some(get_ca_cert_pem()?),
    };

    let client = ClusterClient::builder(node_urls)
        .tls(redis::TlsMode::Secure)
        .certs(tls_certs)
        .build()
        .context("Failed to build kvrocks cluster client")?;

    let mut conn = client
        .get_async_connection()
        .await
        .context("Failed to connect to kvrocks cluster")?;

    let pong: String = redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .context("Failed to ping kvrocks cluster")?;

    if pong != "PONG" {
        anyhow::bail!("Unexpected ping response from kvrocks: {}", pong);
    }

    log::info!(
        "Successfully connected to kvrocks cluster with {} seed nodes",
        hosts.len()
    );

    Ok(KvrocksClient { client })
}

fn normalize_pem(pem: String) -> Vec<u8> {
    let normalized = pem
        .replace("\\n", "\n")
        .replace("\\r\\n", "\n")
        .replace("\\r", "")
        .replace("\r\n", "\n")
        .replace("\r", "")
        .trim()
        .to_string();
    if normalized.ends_with('\n') {
        normalized.into_bytes()
    } else {
        format!("{}\n", normalized).into_bytes()
    }
}

fn get_ca_cert_pem() -> Result<Vec<u8>> {
    Ok(normalize_pem(
        env::var("KVROCKS_CA_CERT").context("KVROCKS_CA_CERT env var not set")?,
    ))
}

fn get_client_cert_pem() -> Result<Vec<u8>> {
    Ok(normalize_pem(
        env::var("KVROCKS_CLIENT_CERT").context("KVROCKS_CLIENT_CERT env var not set")?,
    ))
}

fn get_client_key_pem() -> Result<Vec<u8>> {
    Ok(normalize_pem(
        env::var("KVROCKS_CLIENT_KEY").context("KVROCKS_CLIENT_KEY env var not set")?,
    ))
}

impl KvrocksClient {
    pub async fn store_video_nsfw(&self, data: &VideoNsfw) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_NSFW, data.video_id);
        self.set_hash(&key, data).await
    }

    pub async fn get_video_nsfw(&self, video_id: &str) -> Result<Option<VideoNsfw>> {
        let key = format!("{}:{}", keys::VIDEO_NSFW, video_id);
        self.get_hash(&key).await
    }

    pub async fn store_videohash_phash(&self, data: &VideohashPhash) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEOHASH_PHASH, data.video_id);
        self.set_hash(&key, data).await
    }

    pub async fn get_videohash_phash(&self, video_id: &str) -> Result<Option<VideohashPhash>> {
        let key = format!("{}:{}", keys::VIDEOHASH_PHASH, video_id);
        self.get_hash(&key).await
    }

    pub async fn store_videohash_original(&self, data: &VideohashOriginal) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEOHASH_ORIGINAL, data.video_id);
        self.set_hash(&key, data).await
    }

    pub async fn get_videohash_original(
        &self,
        video_id: &str,
    ) -> Result<Option<VideohashOriginal>> {
        let key = format!("{}:{}", keys::VIDEOHASH_ORIGINAL, video_id);
        self.get_hash(&key).await
    }

    pub async fn store_video_unique_v2(&self, data: &VideoUniqueV2) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_UNIQUE_V2, data.video_id);
        self.set_hash(&key, data).await
    }

    pub async fn get_video_unique_v2(&self, video_id: &str) -> Result<Option<VideoUniqueV2>> {
        let key = format!("{}:{}", keys::VIDEO_UNIQUE_V2, video_id);
        self.get_hash(&key).await
    }

    pub async fn store_video_deleted(&self, data: &VideoDeleted) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_DELETED, data.video_id);
        self.set_hash(&key, data).await
    }

    pub async fn get_video_deleted(&self, video_id: &str) -> Result<Option<VideoDeleted>> {
        let key = format!("{}:{}", keys::VIDEO_DELETED, video_id);
        self.get_hash(&key).await
    }

    pub async fn store_video_dedup_status(&self, data: &VideoDedupStatus) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_DEDUP_STATUS, data.video_id);
        self.set_hash(&key, data).await
    }

    pub async fn get_video_dedup_status(&self, video_id: &str) -> Result<Option<VideoDedupStatus>> {
        let key = format!("{}:{}", keys::VIDEO_DEDUP_STATUS, video_id);
        self.get_hash(&key).await
    }

    pub async fn push_video_embedding(
        &self,
        video_id: &str,
        embedding_vector: Vec<f32>,
        metadata: Option<serde_json::Value>,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_EMBEDDINGS, video_id);

        let data = if let Some(mut existing) = self.get_video_embeddings(video_id).await? {
            existing.embedding_vectors.push(embedding_vector);
            if let Some(new_metadata) = metadata {
                existing.metadata = Some(new_metadata);
            }
            existing
        } else {
            VideoEmbeddings {
                video_id: video_id.to_string(),
                embedding_vectors: vec![embedding_vector],
                metadata,
            }
        };

        self.set_hash(&key, &data).await
    }

    pub async fn get_video_embeddings(&self, video_id: &str) -> Result<Option<VideoEmbeddings>> {
        let key = format!("{}:{}", keys::VIDEO_EMBEDDINGS, video_id);
        self.get_hash(&key).await
    }

    pub async fn store_user_uploaded_content_approval(
        &self,
        data: &UserUploadedContentApproval,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::USER_UPLOADED_CONTENT_APPROVAL, data.video_id);
        self.set_hash(&key, data).await
    }

    pub async fn get_user_uploaded_content_approval(
        &self,
        video_id: &str,
    ) -> Result<Option<UserUploadedContentApproval>> {
        let key = format!("{}:{}", keys::USER_UPLOADED_CONTENT_APPROVAL, video_id);
        self.get_hash(&key).await
    }

    pub async fn update_user_uploaded_content_approval_status(
        &self,
        video_id: &str,
        is_approved: bool,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::USER_UPLOADED_CONTENT_APPROVAL, video_id);
        if let Some(mut data) = self.get_user_uploaded_content_approval(video_id).await? {
            data.is_approved = is_approved;
            self.set_hash(&key, &data).await?;
        }
        Ok(())
    }

    pub async fn delete_user_uploaded_content_approval(&self, video_id: &str) -> Result<()> {
        let key = format!("{}:{}", keys::USER_UPLOADED_CONTENT_APPROVAL, video_id);
        self.del(&key).await
    }

    pub async fn store_bot_uploaded_ai_content(&self, data: &BotUploadedAiContent) -> Result<()> {
        let key = format!("{}:{}", keys::BOT_UPLOADED_AI_CONTENT, data.video_id);
        self.set_hash(&key, data).await
    }

    pub async fn store_video_metadata(&self, data: &VideoMetadata) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_METADATA, data.video_id);
        self.set_hash(&key, data).await
    }

    pub async fn get_video_metadata(&self, video_id: &str) -> Result<Option<VideoMetadata>> {
        let key = format!("{}:{}", keys::VIDEO_METADATA, video_id);
        self.get_hash(&key).await
    }

    pub async fn delete_video_unique_v2(&self, video_id: &str) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_UNIQUE_V2, video_id);
        self.del(&key).await
    }

    pub async fn fetch_unprocessed_video_phashes(
        &self,
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        let mut conn = self.get_connection().await?;
        let pattern = format!("{}:*", keys::VIDEOHASH_PHASH);
        let mut cursor = 0u64;
        let mut unprocessed = Vec::new();

        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await
                .context("Failed to scan videohash_phash keys")?;

            for key in keys {
                if unprocessed.len() >= limit {
                    break;
                }

                let video_id = key
                    .strip_prefix(&format!("{}:", keys::VIDEOHASH_PHASH))
                    .unwrap_or("")
                    .to_string();

                if video_id.is_empty() {
                    continue;
                }

                let dedup_key = format!("{}:{}", keys::VIDEO_DEDUP_STATUS, video_id);
                let exists: bool = conn.exists(&dedup_key).await.unwrap_or(false);

                if !exists {
                    if let Some(phash_data) = self.get_videohash_phash(&video_id).await? {
                        unprocessed.push((video_id, phash_data.phash));
                    }
                }
            }

            cursor = new_cursor;
            if cursor == 0 || unprocessed.len() >= limit {
                break;
            }
        }

        log::info!(
            "Found {} unprocessed videos from kvrocks scan",
            unprocessed.len()
        );
        Ok(unprocessed)
    }

    pub async fn has_any_processed_videos(&self) -> Result<bool> {
        let mut conn = self.get_connection().await?;
        let pattern = format!("{}:*", keys::VIDEO_DEDUP_STATUS);

        // Just try to find one key
        let (_, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(0)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(1)
            .query_async(&mut conn)
            .await
            .context("Failed to scan video_dedup_status keys")?;

        Ok(!keys.is_empty())
    }

    pub async fn fetch_unique_videos_for_backfill(
        &self,
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        let mut conn = self.get_connection().await?;
        let pattern = format!("{}:*", keys::VIDEO_UNIQUE_V2);
        let mut cursor = 0u64;
        let mut unique_videos = Vec::new();

        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await
                .context("Failed to scan video_unique_v2 keys")?;

            for key in keys {
                if unique_videos.len() >= limit {
                    break;
                }

                let video_id = key
                    .strip_prefix(&format!("{}:", keys::VIDEO_UNIQUE_V2))
                    .unwrap_or("")
                    .to_string();

                if video_id.is_empty() {
                    continue;
                }

                let dedup_key = format!("{}:{}", keys::VIDEO_DEDUP_STATUS, video_id);
                let exists: bool = conn.exists(&dedup_key).await.unwrap_or(false);

                if exists {
                    continue; // Already processed
                }

                if let Some(phash_data) = self.get_videohash_phash(&video_id).await? {
                    unique_videos.push((video_id, phash_data.phash));
                }
            }

            cursor = new_cursor;
            if cursor == 0 || unique_videos.len() >= limit {
                break;
            }
        }

        log::info!(
            "Found {} unique videos for backfill from kvrocks scan",
            unique_videos.len()
        );
        Ok(unique_videos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_kvrocks_connection() {
        if env::var("KVROCKS_PASSWORD").is_err() {
            println!("Skipping test: KVROCKS_PASSWORD not set");
            return;
        }

        let client = init_kvrocks_client()
            .await
            .expect("Failed to init kvrocks client");

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

        client
            .del(test_key)
            .await
            .expect("Failed to delete test key");

        println!("Kvrocks cluster connection test passed!");
    }
}
