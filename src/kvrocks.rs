use anyhow::{Context, Result};
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::AsyncCommands;
use serde::Serialize;
use std::env;

const KVROCKS_TLS_PORT: u16 = 6666;

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
    let mut normalized = pem
        .replace("\\n", "\n")
        .replace("\\r\\n", "\n")
        .replace("\\r", "")
        .replace("\r\n", "\n")
        .replace("\r", "")
        .trim()
        .to_string();

    // Handle case where PEM is all on one line (newlines were stripped)
    if !normalized.contains('\n') && normalized.contains("-----") {
        // Split at the markers and reconstruct with newlines
        normalized = normalized
            .replace("-----BEGIN ", "\n-----BEGIN ")
            .replace("----- ", "-----\n")
            .replace(" -----END", "\n-----END")
            .replace("-----END ", "-----END ")
            .trim()
            .to_string();
    }

    if !normalized.ends_with('\n') {
        normalized.push('\n');
    }
    normalized.into_bytes()
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
    pub async fn store_event(&self, event_id: &str, event_data: &serde_json::Value) -> Result<()> {
        let key = format!("{}:{}", keys::TEST_EVENTS_ANALYTICS, event_id);
        self.set_json(&key, event_data).await
    }

    pub async fn store_video_nsfw(
        &self,
        video_id: &str,
        nsfw_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_NSFW, video_id);
        self.set_json(&key, nsfw_data).await
    }

    pub async fn store_video_nsfw_agg(
        &self,
        video_id: &str,
        data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_NSFW_AGG, video_id);
        self.set_json(&key, data).await
    }

    pub async fn store_video_embeddings_agg(
        &self,
        video_id: &str,
        data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_EMBEDDINGS_AGG, video_id);
        self.set_json(&key, data).await
    }

    pub async fn store_videohash_phash(
        &self,
        video_id: &str,
        phash_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEOHASH_PHASH, video_id);
        self.set_json(&key, phash_data).await
    }

    pub async fn store_videohash_original(
        &self,
        video_id: &str,
        hash_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEOHASH_ORIGINAL, video_id);
        self.set_json(&key, hash_data).await
    }

    pub async fn store_video_unique(&self, video_id: &str, data: &serde_json::Value) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_UNIQUE, video_id);
        self.set_json(&key, data).await
    }

    pub async fn store_video_unique_v2(
        &self,
        video_id: &str,
        data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_UNIQUE_V2, video_id);
        self.set_json(&key, data).await
    }

    pub async fn store_video_deleted(
        &self,
        video_id: &str,
        delete_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_DELETED, video_id);
        self.set_json(&key, delete_data).await
    }

    pub async fn store_video_dedup_status(
        &self,
        video_id: &str,
        status_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_DEDUP_STATUS, video_id);
        self.set_json(&key, status_data).await
    }

    pub async fn store_video_dedup_status_with_table(
        &self,
        table_suffix: &str,
        video_id: &str,
        status_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}:{}", keys::VIDEO_DEDUP_STATUS, table_suffix, video_id);
        self.set_json(&key, status_data).await
    }

    pub async fn store_ugc_content_approval(
        &self,
        video_id: &str,
        approval_data: &serde_json::Value,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::UGC_CONTENT_APPROVAL, video_id);
        self.set_json(&key, approval_data).await
    }

    pub async fn update_ugc_approval_status(
        &self,
        video_id: &str,
        is_approved: bool,
    ) -> Result<()> {
        let key = format!("{}:{}", keys::UGC_CONTENT_APPROVAL, video_id);
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

    pub async fn delete_ugc_content_approval(&self, video_id: &str) -> Result<()> {
        let key = format!("{}:{}", keys::UGC_CONTENT_APPROVAL, video_id);
        self.del(&key).await
    }

    pub async fn delete_video_unique(&self, video_id: &str) -> Result<()> {
        let key = format!("{}:{}", keys::VIDEO_UNIQUE, video_id);
        self.del(&key).await
    }

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
