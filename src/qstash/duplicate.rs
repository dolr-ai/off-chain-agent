use std::time::SystemTime;

use crate::{
    app_state, async_dedup_index, consts::OFF_CHAIN_AGENT_URL,
    duplicate_video::videohash::VideoHash,
};
use anyhow::{anyhow, Context};
use google_cloud_bigquery::http::job::query::QueryRequest;
use http::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VideoPublisherData {
    pub canister_id: String,
    pub publisher_principal: String,
    pub post_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DuplicateVideoEvent {
    pub original_video_id: String,
    pub parent_video_id: String,
    pub similarity_percentage: f64,
    pub exact_duplicate: bool,
    pub publisher_canister_id: String,
    pub publisher_principal: String,
    pub post_id: u64,
    pub timestamp: String,
}

// Add these structures to support the indexer API response
#[derive(Debug, Deserialize)]
struct VideoHashIndexerResponse {
    match_found: bool,
    match_details: Option<MatchDetails>,
    hash_added: bool,
}

#[derive(Debug, Deserialize)]
struct MatchDetails {
    video_id: String,
    similarity_percentage: f64,
    is_duplicate: bool,
}

// The VideoHashDuplication struct will contain the deduplication logic
pub struct VideoHashDuplication<'a> {
    client: &'a reqwest::Client,
    base_url: &'a reqwest::Url,
}

impl<'a> VideoHashDuplication<'a> {
    pub fn new(client: &'a reqwest::Client, base_url: &'a reqwest::Url) -> Self {
        Self { client, base_url }
    }

    pub async fn publish_duplicate_video_event(
        &self,
        duplicate_event: DuplicateVideoEvent,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/duplicate_video_detected")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!(duplicate_event);

        log::info!(
            "Publishing duplicate video event: video_id [{}], parent_video_id [{}], score={}",
            duplicate_event.original_video_id,
            duplicate_event.parent_video_id,
            duplicate_event.similarity_percentage
        );

        self.client
            .post(url.clone())
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .send()
            .await?;

        Ok(())
    }

    pub async fn publish_deduplication_completed(
        &self,
        video_id: &str,
        canister_id: &str,
        post_id: u64,
        publisher_user_id: &str,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/deduplication_completed")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
            "canister_id": canister_id,
            "post_id": post_id,
            "publisher_user_id": publisher_user_id,
            "timestamp": chrono::Utc::now().to_rfc3339()
        });

        log::info!(
            "Publishing deduplication completed event for video_id [{}]",
            video_id
        );

        self.client
            .post(url.clone())
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .send()
            .await?;

        Ok(())
    }

    pub async fn process_video_deduplication(
        &self,
        dedup_index_ctx: &async_dedup_index::WrappedContext,
        bigquery_client: &google_cloud_bigquery::client::Client,
        video_id: &str,
        video_url: &str,
        publisher_data: VideoPublisherData,
        publish_video_callback: impl FnOnce(
            &str,
            &str,
            u64,
            String,
            &str,
        )
            -> futures::future::BoxFuture<'a, Result<(), anyhow::Error>>,
    ) -> Result<(), anyhow::Error> {
        log::info!("Calculating videohash for video URL: {}", video_url);
        let video_hash = VideoHash::from_url(video_url)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to generate videohash: {}", e))?;

        // Store the original hash regardless of duplication status
        self.store_videohash_to_spacetime(dedup_index_ctx, video_id, &video_hash.hash)
            .await?;
        self.store_videohash_original(bigquery_client, video_id, &video_hash.hash)
            .await?;

        // TODO: the following call will be replaced with spacetimedb in
        // https://github.com/dolr-ai/product-roadmap/issues/569

        // Call the video hash indexer API to check for duplicates
        let client = reqwest::Client::new();
        let response = client
            .post("https://videohash-indexer.fly.dev/search")
            .json(&serde_json::json!({
                "video_id": video_id,
                "hash": video_hash.hash,
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "VideoHash Indexer API failed: {} - {}",
                status,
                error_text
            ));
        }

        let indexer_response: VideoHashIndexerResponse = response.json().await?;
        log::info!(
            "VideoHash Indexer response for video_id [{}]: {:?}",
            video_id,
            indexer_response
        );

        let is_duplicate = indexer_response.match_found;

        if is_duplicate {
            // A similar video was found - record as duplicate
            if let Some(match_details) = indexer_response.match_details {
                self.store_duplicate_video(
                    video_id,
                    &video_hash.hash,
                    &match_details,
                    &publisher_data,
                )
                .await?;

                log::info!(
                    "Duplicate video detected: video_id [{}] is similar to parent_video_id [{}] (score: {})",
                    video_id,
                    match_details.video_id,
                    match_details.similarity_percentage
                );

                let exact_duplicate = match_details.similarity_percentage > 98.0;
                let _duplicate_event = DuplicateVideoEvent {
                    original_video_id: video_id.to_string(),
                    parent_video_id: match_details.video_id.clone(),
                    similarity_percentage: match_details.similarity_percentage,
                    exact_duplicate,
                    publisher_canister_id: publisher_data.canister_id.clone(),
                    publisher_principal: publisher_data.publisher_principal.clone(),
                    post_id: publisher_data.post_id,
                    timestamp: chrono::Utc::now().to_rfc3339(),
                };
            }
        } else {
            self.store_unique_video(video_id, &video_hash.hash).await?;
            log::info!("Unique video recorded: video_id [{}]", video_id);
        }

        // Always proceed with normal video processing, regardless of duplicate status
        let timestamp = chrono::Utc::now().to_rfc3339();
        publish_video_callback(
            video_id,
            &publisher_data.canister_id,
            publisher_data.post_id,
            timestamp,
            &publisher_data.publisher_principal,
        )
        .await?;

        Ok(())
    }

    async fn store_videohash_original(
        &self,
        bigquery_client: &google_cloud_bigquery::client::Client,
        video_id: &str,
        hash: &str,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.videohash_original` 
             (video_id, videohash, created_at) 
             VALUES ('{}', '{}', CURRENT_TIMESTAMP())",
            video_id, hash
        );

        let request = QueryRequest {
            query,
            ..Default::default()
        };

        log::info!(
            "Storing hash in videohash_original for video_id [{}]",
            video_id
        );

        bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &request)
            .await?;

        Ok(())
    }

    async fn store_videohash_to_spacetime(
        &self,
        ctx: &async_dedup_index::WrappedContext,
        video_id: &str,
        hash: &str,
    ) -> anyhow::Result<()> {
        ctx.add(video_id, hash, SystemTime::now())
            .await
            .context("Couldn't send request to stdb")?
            .map_err(|err| anyhow!("{err}"))
            .context("Module returned error")
            .inspect_err(|err| {
                log::error!("module error: {err:#?}");
            })?;

        Ok(())
    }

    async fn store_unique_video(&self, video_id: &str, hash: &str) -> Result<(), anyhow::Error> {
        let bigquery_client = app_state::init_bigquery_client().await;

        let query = format!(
            "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.video_unique` 
             (video_id, videohash, created_at) 
             VALUES ('{}', '{}', CURRENT_TIMESTAMP())",
            video_id, hash
        );

        let request = QueryRequest {
            query,
            ..Default::default()
        };

        log::info!(
            "Storing unique video in video_unique for video_id [{}]",
            video_id
        );

        bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &request)
            .await?;

        Ok(())
    }

    async fn store_duplicate_video(
        &self,
        video_id: &str,
        _hash: &str,
        match_details: &MatchDetails,
        publisher_data: &VideoPublisherData,
    ) -> Result<(), anyhow::Error> {
        let bigquery_client = app_state::init_bigquery_client().await;
        let exact_duplicate = match_details.similarity_percentage > 99.0;
        let query = format!(
            "INSERT INTO `hot-or-not-feed-intelligence.yral_ds.duplicate_videos` (
                publisher_canister_id, publisher_principal, post_id,
                original_video_id, parent_video_id, parent_canister_id,
                parent_principal, parent_post_id, exact_duplicate,
                duplication_score
            ) VALUES (
                '{}', '{}', {},
                '{}', '{}', NULL,
                NULL, NULL, {},
                {}
            )",
            publisher_data.canister_id,
            publisher_data.publisher_principal,
            publisher_data.post_id,
            video_id,
            match_details.video_id,
            exact_duplicate,
            match_details.similarity_percentage
        );

        let request = QueryRequest {
            query,
            ..Default::default()
        };

        log::info!(
            "Storing duplicate video in duplicate_video: video_id [{}], parent_video_id [{}], score={}",
            video_id,
            match_details.video_id,
            match_details.similarity_percentage
        );

        bigquery_client
            .job()
            .query("hot-or-not-feed-intelligence", &request)
            .await?;

        Ok(())
    }
}
