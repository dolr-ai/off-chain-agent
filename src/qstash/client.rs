use std::env;
use std::sync::Arc;

use candid::Principal;
use chrono::Timelike;
use futures::StreamExt;
use http::{
    header::{AUTHORIZATION, CONTENT_TYPE},
    HeaderMap, HeaderValue,
};
use reqwest::{Client, Url};
use sentry::configure_scope;
use serde_json::json;
use tracing::instrument;

use crate::{
    canister::snapshot::snapshot_v2::BackupUserCanisterPayload, consts::OFF_CHAIN_AGENT_URL,
    events::event::UploadVideoInfo, posts::report_post::ReportPostRequestV2,
    videogen::qstash_types::QstashVideoGenRequest,
};
use videogen_common::VideoGenerator;

#[derive(Clone, Debug)]
pub struct QStashClient {
    pub client: Client,
    pub base_url: Arc<Url>,
}

impl QStashClient {
    pub fn new(auth_token: &str) -> Self {
        let mut bearer: HeaderValue = format!("Bearer {}", auth_token)
            .parse()
            .expect("Invalid QStash auth token");
        bearer.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, bearer);

        let client = Client::builder()
            .default_headers(headers)
            .build()
            .expect("Failed to create QStash client");

        // Support custom QStash URL for local development
        let base_url_str =
            env::var("QSTASH_URL").unwrap_or_else(|_| "https://qstash.upstash.io/v2/".to_string());
        let base_url = Url::parse(&base_url_str).expect("Invalid QSTASH_URL");

        log::info!("QStash client initialized with base URL: {}", base_url);

        Self {
            client,
            base_url: Arc::new(base_url),
        }
    }

    #[instrument(skip(self))]
    pub async fn duplicate_to_storj(
        &self,
        data: storj_interface::duplicate::Args,
    ) -> anyhow::Result<()> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL.join("qstash/storj_ingest").unwrap();
        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;

        self.client
            .post(url)
            .json(&data)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("Upstash-Flow-Control-Key", "STORJ_INGESTION")
            .header("Upstash-Flow-Control-Value", "Rate=20,Parallelism=10")
            .send()
            .await?;

        sentry::with_scope(
            |scope| {
                scope.set_tag("yral.video_id", &data.video_id);
                scope.set_tag("yral.publisher_user_id", &data.publisher_user_id);
                scope.set_tag("yral.is_nsfw", data.is_nsfw);
                scope.set_extra(
                    "yral.metadata",
                    serde_json::to_value(&data.metadata)
                        .expect("metadata to be serializable as json"),
                );
            },
            || sentry::capture_message("enqueing for storj duplication", sentry::Level::Info),
        );

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn publish_video(
        &self,
        video_id: &str,
        post_id: u64,
        timestamp_str: String,
        publisher_user_id: &str,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL.join("qstash/upload_video_gcs").unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
            "post_id": post_id,
            "timestamp": timestamp_str,
            "publisher_user_id": publisher_user_id
        });

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .send()
            .await?;

        sentry::with_scope(
            |scope| {
                scope.set_tag("yral.video_id", &video_id);
                scope.set_tag("yral.publisher_user_id", &publisher_user_id);
            },
            || sentry::capture_message("enqueing for upload to gcs", sentry::Level::Info),
        );

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn publish_video_frames(
        &self,
        video_id: &str,
        video_info: &UploadVideoInfo,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/enqueue_video_frames")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
            "video_info": video_info,
        });

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .send()
            .await?;

        sentry::with_scope(
            |scope| {
                scope.set_tag("yral.video_id", &video_id);
                scope.set_tag("yral.publisher_user_id", &video_info.publisher_user_id);
                scope.set_extra(
                    "yral.upload_info",
                    serde_json::to_value(&video_info)
                        .expect("upload video info to be serializable as json"),
                );
            },
            || sentry::capture_message("enqueing for video frames", sentry::Level::Info),
        );

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn publish_video_nsfw_detection(
        &self,
        video_id: &str,
        video_info: &UploadVideoInfo,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/enqueue_video_nsfw_detection")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
            "video_info": video_info,
        });

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .send()
            .await?;

        sentry::with_scope(
            |scope| {
                scope.set_tag("yral.video_id", &video_id);
                scope.set_tag("yral.publisher_user_id", &video_info.publisher_user_id);
                scope.set_extra(
                    "yral.upload_info",
                    serde_json::to_value(&video_info)
                        .expect("upload video info to be serializable as json"),
                );
            },
            || {
                sentry::capture_message(
                    "enqueing for nsfw detection v1 (shouldn't happen)",
                    sentry::Level::Info,
                )
            },
        );

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn publish_video_nsfw_detection_v2(
        &self,
        video_id: &str,
        video_info: UploadVideoInfo,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/enqueue_video_nsfw_detection_v2")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
            "video_info": video_info,
        });

        // Calculate delay until next :20 minute of any hour
        let now = chrono::Utc::now();
        let current_minute = now.minute();
        let minutes_until_20 = if current_minute >= 20 {
            60 - current_minute + 20
        } else {
            20 - current_minute
        };

        let jitter = now.nanosecond() % 601;
        let delay_seconds = minutes_until_20 * 60 + jitter + 3600;

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("upstash-delay", format!("{}s", delay_seconds))
            .send()
            .await?;

        sentry::with_scope(
            |scope| {
                scope.set_tag("yral.video_id", &video_id);
                scope.set_tag("yral.publisher_user_id", &video_info.publisher_user_id);
                scope.set_extra("yral.delay_seconds", delay_seconds.into());
                scope.set_extra(
                    "yral.upload_info",
                    serde_json::to_value(&video_info)
                        .expect("upload video info to be serializable as json"),
                );
            },
            || sentry::capture_message("enqueing for nsfw detection v2", sentry::Level::Info),
        );

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn publish_hls_processing(
        &self,
        video_id: &str,
        video_info: &UploadVideoInfo,
        is_nsfw: bool,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL.join("qstash/process_hls").unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
            "video_info": video_info,
            "is_nsfw": is_nsfw,
        });

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("Upstash-Flow-Control-Key", "HLS_PROCESSING")
            .header("Upstash-Flow-Control-Value", "Rate=5,Parallelism=3")
            .send()
            .await?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn publish_video_finalize_v2(
        &self,
        video_id: &str,
        video_info: &UploadVideoInfo,
        is_nsfw: bool,
        hls_response: &crate::events::hls::HlsProcessingResponse,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL.join("qstash/finalize_video_v2").unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!({
            "video_id": video_id,
            "video_info": video_info,
            "is_nsfw": is_nsfw,
            "hls_response": hls_response,
        });

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .send()
            .await?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn publish_report_post(
        &self,
        report_request: ReportPostRequestV2,
    ) -> Result<(), anyhow::Error> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL.join("qstash/report_post").unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;
        let req = serde_json::json!(report_request);

        self.client
            .post(url)
            .json(&req)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .send()
            .await?;

        Ok(())
    }

    #[instrument(skip(self, canister_ids))]
    pub async fn backup_canister_batch(
        &self,
        canister_ids: Vec<Principal>,
        rate_limit: u32,
        parallelism: u32,
        date_str: String,
    ) -> anyhow::Result<()> {
        let destination_url = OFF_CHAIN_AGENT_URL
            .join("qstash/backup_user_canister")?
            .to_string();
        let qstash_batch_url = self.base_url.join("batch")?;

        log::info!("Backup canister batch URL: {}", qstash_batch_url);

        let requests: Vec<serde_json::Value> = canister_ids
            .iter()
            .map(|&canister_id| {
                let payload = BackupUserCanisterPayload {
                    canister_id,
                    date_str: date_str.clone(),
                };
                let body_str = serde_json::to_string(&payload).unwrap_or_else(|e| {
                    tracing::error!("Failed to serialize BackupUserCanisterPayload: {}", e);
                    "{}".to_string() // Use an empty JSON object as fallback
                });

                json!({
                    "destination": destination_url,
                    "headers": {
                        "Upstash-Forward-Content-Type": "application/json",
                        "Upstash-Forward-Method": "POST",
                        "Upstash-Flow-Control-Key": "BACKUP_CANISTER",
                        "Upstash-Flow-Control-Value": format!("Rate={},Parallelism={}", rate_limit, parallelism),
                        "Upstash-Content-Based-Deduplication": "true",
                        "Upstash-Retries": "2",
                    },
                    "body": body_str,
                })
            })
            .collect();

        log::info!("Backup canister batch requests: {}", requests.len());

        let chunk_size = 100;

        let mut futures = Vec::new();
        for request_chunk in requests.chunks(chunk_size) {
            let client = self.client.clone();
            let qstash_batch_url = qstash_batch_url.clone();
            futures.push(async move {
                client
                    .post(qstash_batch_url.clone())
                    .json(&request_chunk)
                    .send()
                    .await
            });
        }

        log::info!("Backup canister batch futures: {}", futures.len());

        let responses = futures::stream::iter(futures)
            .buffer_unordered(80) // less than qstash limit per sec = 100
            .collect::<Vec<_>>()
            .await;

        log::info!("Backup canister batch responses: {}", responses.len());

        for response in responses {
            match response {
                Ok(response) => {
                    if !response.status().is_success() {
                        tracing::error!("QStash batch request failed: {}", response.status());
                    }
                }
                Err(e) => tracing::error!("QStash batch request failed: {}", e),
            }
        }

        log::info!("Backup canister batch completed");

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn queue_video_generation(
        &self,
        request: &QstashVideoGenRequest,
        callback_url: &str,
    ) -> anyhow::Result<()> {
        let off_chain_ep = OFF_CHAIN_AGENT_URL
            .join("qstash/process_video_gen")
            .unwrap();

        let url = self.base_url.join(&format!("publish/{}", off_chain_ep))?;

        // Get flow control from the model using the VideoGenerator trait
        let flow_control = request.input.flow_control_config().map(|(rate, parallel)| {
            let key = request.input.flow_control_key();
            let value = format!("Rate={},Parallelism={}", rate, parallel);
            (key, value)
        });

        let mut req_builder = self
            .client
            .post(url)
            .json(&request)
            .header(CONTENT_TYPE, "application/json")
            .header("upstash-method", "POST")
            .header("Upstash-Callback", callback_url)
            .header("Upstash-Retries", "0");

        // Add flow control headers only if flow control is configured
        if let Some((fc_key, fc_value)) = flow_control {
            req_builder = req_builder
                .header("Upstash-Flow-Control-Key", fc_key)
                .header("Upstash-Flow-Control-Value", fc_value);
        }

        req_builder.send().await?;

        Ok(())
    }
}
