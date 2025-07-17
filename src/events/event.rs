#![allow(deprecated)]
use crate::consts::OFF_CHAIN_AGENT_URL;
use crate::events::types::VideoDurationWatchedPayload;
#[cfg(not(feature = "local-bin"))]
use crate::events::utils::parse_success_history_params;
use crate::{
    app_state::AppState,
    events::warehouse_events::WarehouseEvent,
    AppError,
};
#[cfg(not(feature = "local-bin"))]
use crate::consts::BIGQUERY_INGESTION_URL;
use axum::{extract::State, Json};
use candid::Principal;
use chrono::{DateTime, Utc};
use http::header::CONTENT_TYPE;
use log::error;
#[cfg(not(feature = "local-bin"))]
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
#[cfg(not(feature = "local-bin"))]
use std::env;
#[cfg(not(feature = "local-bin"))]
use firestore::errors::FirestoreError;
#[cfg(not(feature = "local-bin"))]
use google_cloud_bigquery::http::job::query::QueryRequest;
#[cfg(not(feature = "local-bin"))]
use crate::utils::cf_images::upload_base64_image;
#[cfg(not(feature = "local-bin"))]
use crate::consts::CLOUDFLARE_ACCOUNT_ID;
#[cfg(not(feature = "local-bin"))]
use crate::events::queries::get_icpump_insert_query;
use tracing::instrument;
#[cfg(not(feature = "local-bin"))]
use yral_ml_feed_cache::consts::{
    USER_LIKE_HISTORY_PLAIN_POST_ITEM_SUFFIX, USER_LIKE_HISTORY_PLAIN_POST_ITEM_SUFFIX_V2,
    USER_SUCCESS_HISTORY_CLEAN_SUFFIX_V2, USER_SUCCESS_HISTORY_NSFW_SUFFIX_V2,
    USER_WATCH_HISTORY_CLEAN_SUFFIX_V2, USER_WATCH_HISTORY_NSFW_SUFFIX_V2,
    USER_WATCH_HISTORY_PLAIN_POST_ITEM_SUFFIX, USER_WATCH_HISTORY_PLAIN_POST_ITEM_SUFFIX_V2,
};
#[cfg(not(feature = "local-bin"))]
use yral_ml_feed_cache::types::{BufferItem, PlainPostItem};
#[cfg(not(feature = "local-bin"))]
use yral_ml_feed_cache::types_v2::{BufferItemV2, MLFeedCacheHistoryItemV2};
#[cfg(not(feature = "local-bin"))]
use yral_ml_feed_cache::{
    consts::{
        USER_SUCCESS_HISTORY_CLEAN_SUFFIX, USER_SUCCESS_HISTORY_NSFW_SUFFIX,
        USER_WATCH_HISTORY_CLEAN_SUFFIX, USER_WATCH_HISTORY_NSFW_SUFFIX,
    },
    types::MLFeedCacheHistoryItem,
};


pub mod login_successful;
pub mod storj;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TokenListItem {
    user_id: String,
    name: String,
    token_name: String,
    token_symbol: String,
    logo: String,
    description: String,
    #[serde(with = "firestore::serialize_as_timestamp")]
    created_at: DateTime<Utc>,
    link: String,
    #[serde(default)]
    is_nsfw: bool,
    #[serde(default)]
    nsfw_ec: String,
    #[serde(default)]
    nsfw_gore: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ICPumpTokenMetadata {
    pub canister_id: String,
    pub description: String,
    pub host: String,
    pub link: String,
    pub logo: String,
    pub token_name: String,
    pub token_symbol: String,
    pub user_id: String,
    pub created_at: String,
    #[serde(default)]
    is_nsfw: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoginSuccessfulParams {
    pub canister_id: Principal,
    pub user_id: Principal,
}

#[derive(Debug)]
pub struct Event {
    pub event: WarehouseEvent,
}

impl Event {
    pub fn new(event: WarehouseEvent) -> Self {
        Self { event }
    }

    #[cfg(not(feature = "local-bin"))]
    pub fn stream_to_bigquery(&self, app_state: &AppState) {
        let event_str = self.event.event.clone();
        let params_str = self.event.params.clone();
        let app_state = app_state.clone();

        tokio::spawn(async move {
            let timestamp = chrono::Utc::now().to_rfc3339();

            let data = serde_json::json!({
                "kind": "bigquery#tableDataInsertAllRequest",
                "rows": [
                    {
                        "json": {
                            "event": event_str,
                            "params": params_str,
                            "timestamp": timestamp,
                        }
                    }
                ]
            });

            let res = stream_to_bigquery(&app_state, data).await;
            if res.is_err() {
                error!("Error sending data to BigQuery: {}", res.err().unwrap());
            }
        });
    }

    #[cfg(feature = "local-bin")]
    #[allow(dead_code)]
    pub fn stream_to_bigquery(&self, _app_state: &AppState) {
        // No-op for local-bin
    }

    #[allow(dead_code)]
    pub fn stream_to_bigquery_token_metadata(&self, app_state: &AppState) {
        if self.event.event == "token_creation_completed" {
            let params: Value = serde_json::from_str(&self.event.params).expect("Invalid JSON");
            let app_state = app_state.clone();

            tokio::spawn(async move {
                let timestamp = chrono::Utc::now().to_rfc3339();

                let data = ICPumpTokenMetadata {
                    canister_id: params["canister_id"].as_str().unwrap().to_string(),
                    description: params["description"].as_str().unwrap().to_string(),
                    host: params["host"].as_str().unwrap().to_string(),
                    link: params["link"].as_str().unwrap().to_string(),
                    logo: params["logo"].as_str().unwrap().to_string(),
                    token_name: params["token_name"].as_str().unwrap().to_string(),
                    token_symbol: params["token_symbol"].as_str().unwrap().to_string(),
                    user_id: params["user_id"].as_str().unwrap().to_string(),
                    created_at: timestamp,
                    is_nsfw: params["is_nsfw"].as_bool().unwrap(),
                };

                let res = stream_to_bigquery_token_metadata_impl_v2(&app_state, data).await;
                if res.is_err() {
                    error!(
                        "stream_to_bigquery_token_metadata: Error sending data to BigQuery: {}",
                        res.err().unwrap()
                    );
                }
            });
        }
    }

    pub fn check_video_deduplication(&self, app_state: &AppState) {
        if self.event.event == "video_upload_successful" {
            let params: Value = match serde_json::from_str(&self.event.params) {
                Ok(params) => params,
                Err(e) => {
                    error!(
                        "Failed to parse video_upload_successful event params: {e}"
                    );
                    return;
                }
            };

            let qstash_client = app_state.qstash_client.clone();

            tokio::spawn(async move {
                // Extract required fields with error handling
                let video_id = match params.get("video_id").and_then(|v| v.as_str()) {
                    Some(id) => id,
                    None => {
                        error!("Missing video_id in video_upload_successful event");
                        return;
                    }
                };

                let canister_id = match params.get("canister_id").and_then(|v| v.as_str()) {
                    Some(id) => id,
                    None => {
                        error!("Missing canister_id in video_upload_successful event");
                        return;
                    }
                };

                let post_id = match params.get("post_id").and_then(|v| v.as_u64()) {
                    Some(id) => id,
                    None => {
                        error!("Missing post_id in video_upload_successful event");
                        return;
                    }
                };

                let publisher_user_id =
                    match params.get("publisher_user_id").and_then(|v| v.as_str()) {
                        Some(id) => id,
                        None => {
                            error!("Missing publisher_user_id in video_upload_successful event");
                            return;
                        }
                    };

                // Construct video URL
                let video_url = format!(
                    "https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{video_id}/downloads/default.mp4"
                );

                log::info!("Sending video for deduplication check: {video_id}");

                // Create request for video_deduplication endpoint
                let off_chain_ep = OFF_CHAIN_AGENT_URL
                    .join("qstash/video_deduplication")
                    .unwrap();
                let url = qstash_client
                    .base_url
                    .join(&format!("publish/{off_chain_ep}"))
                    .unwrap();

                let request_data = serde_json::json!({
                    "video_id": video_id,
                    "video_url": video_url,
                    "publisher_data": {
                        "canister_id": canister_id,
                        "publisher_principal": publisher_user_id,
                        "post_id": post_id
                    }
                });

                // Send to the "/video_deduplication" endpoint via QStash
                let result = qstash_client
                    .client
                    .post(url)
                    .json(&request_data)
                    .header(CONTENT_TYPE, "application/json")
                    .header("upstash-method", "POST")
                    .header("upstash-delay", "600s")
                    .send()
                    .await;

                match result {
                    Ok(_) => log::info!(
                        "Video deduplication check successfully queued for video_id: {video_id}"
                    ),
                    Err(e) => error!(
                        "Failed to queue video deduplication check for video_id {video_id}: {e:?}"
                    ),
                }
            });
        }
    }

    #[deprecated(
        note = "Use update_watch_history_v2 instead. will be removed post redis migration"
    )]
    #[cfg(not(feature = "local-bin"))]
    #[allow(dead_code)]
    pub fn update_watch_history(&self, app_state: &AppState) {
        if self.event.event == "video_duration_watched" {
            let params: Result<VideoDurationWatchedPayload, _> =
                serde_json::from_str(&self.event.params);

            let params = match params {
                Ok(params) => params,
                Err(e) => {
                    error!("Failed to parse video_duration_watched params: {e:?}");
                    return;
                }
            };

            let app_state = app_state.clone();

            tokio::spawn(async move {
                let ml_feed_cache = app_state.ml_feed_cache.clone();

                let percent_watched = params.percentage_watched;
                let nsfw_probability = params.nsfw_probability.unwrap_or_default();

                let user_canister_id = &params.canister_id;
                let publisher_canister_id = &params
                    .publisher_canister_id
                    .map(|f| f.to_string())
                    .unwrap_or_default();
                let post_id = params.post_id.unwrap_or_default();
                let video_id = params.video_id.unwrap_or_default();
                let item_type = "video_duration_watched".to_string();
                let timestamp = std::time::SystemTime::now();

                let watch_history_item = MLFeedCacheHistoryItem {
                    canister_id: publisher_canister_id.to_string(),
                    item_type: item_type.clone(),
                    nsfw_probability: nsfw_probability as f32,
                    post_id,
                    video_id: video_id.clone(),
                    timestamp,
                    percent_watched: percent_watched as f32,
                };

                let user_cache_key = format!(
                    "{}{}",
                    user_canister_id,
                    if nsfw_probability <= 0.4 {
                        USER_WATCH_HISTORY_CLEAN_SUFFIX
                    } else {
                        USER_WATCH_HISTORY_NSFW_SUFFIX
                    }
                );
                let res = ml_feed_cache
                    .add_user_watch_history_items(&user_cache_key, vec![watch_history_item.clone()])
                    .await;
                if res.is_err() {
                    error!("Error adding user watch history items: {:?}", res.err());
                }

                // Below is for dealing with hotornot evaluator for alloydb
                // Conditions:
                // if already present in history, return
                // else add to history and user buffer

                let plain_key = format!(
                    "{}{}",
                    user_canister_id, USER_WATCH_HISTORY_PLAIN_POST_ITEM_SUFFIX
                );

                match ml_feed_cache
                    .is_user_history_plain_item_exists(
                        plain_key.as_str(),
                        PlainPostItem {
                            canister_id: publisher_canister_id.to_string(),
                            post_id,
                        },
                    )
                    .await
                {
                    Ok(true) => {
                    }
                    Ok(false) => {
                        // add_user_buffer_items
                        if let Err(e) = ml_feed_cache
                            .add_user_buffer_items(vec![BufferItem {
                                publisher_canister_id: publisher_canister_id.to_string(),
                                post_id,
                                video_id,
                                item_type,
                                percent_watched: watch_history_item.percent_watched,
                                user_canister_id: user_canister_id.to_string(),
                                timestamp,
                            }])
                            .await
                        {
                            error!("Error adding user watch history buffer items: {:?}", e);
                        }
                    }
                    Err(e) => {
                        error!("Error checking user watch history plain item: {:?}", e);
                    }
                }
            });
        }
    }

    #[cfg(not(feature = "local-bin"))]
    pub fn update_watch_history_v2(&self, app_state: &AppState) {
        if self.event.event == "video_duration_watched" {
            let params: Result<VideoDurationWatchedPayload, _> =
                serde_json::from_str(&self.event.params);

            let params = match params {
                Ok(params) => params,
                Err(e) => {
                    error!("Failed to parse video_duration_watched params: {e:?}");
                    return;
                }
            };

            let app_state = app_state.clone();

            tokio::spawn(async move {
                let ml_feed_cache = app_state.ml_feed_cache.clone();

                let percent_watched = params.percentage_watched;
                let nsfw_probability = params.nsfw_probability.unwrap_or_default();

                let user_id = &params.user_id;
                let publisher_canister_id = &params
                    .publisher_canister_id
                    .map(|f| f.to_string())
                    .unwrap_or_default();
                let publisher_user_id = &params
                    .publisher_user_id
                    .map(|f| f.to_string())
                    .unwrap_or_default();
                let post_id = params.post_id.unwrap_or_default();
                let video_id = params.video_id.unwrap_or_default();
                let item_type = "video_duration_watched".to_string();
                let timestamp = std::time::SystemTime::now();

                let watch_history_item = MLFeedCacheHistoryItemV2 {
                    publisher_user_id: publisher_user_id.to_string(),
                    canister_id: publisher_canister_id.to_string(),
                    item_type: item_type.clone(),
                    post_id,
                    video_id: video_id.clone(),
                    timestamp,
                    percent_watched: percent_watched as f32,
                };

                let user_cache_key = format!(
                    "{}{}",
                    user_id,
                    if nsfw_probability <= 0.4 {
                        USER_WATCH_HISTORY_CLEAN_SUFFIX_V2
                    } else {
                        USER_WATCH_HISTORY_NSFW_SUFFIX_V2
                    }
                );
                let res = ml_feed_cache
                    .add_user_watch_history_items_v2(
                        &user_cache_key,
                        vec![watch_history_item.clone()],
                    )
                    .await;
                if res.is_err() {
                    error!("Error adding user watch history items: {:?}", res.err());
                }

                // Below is for dealing with hotornot evaluator for alloydb
                // Conditions:
                // if already present in history, return
                // else add to history and user buffer

                let plain_key = format!(
                    "{}{}",
                    user_id, USER_WATCH_HISTORY_PLAIN_POST_ITEM_SUFFIX_V2
                );

                match ml_feed_cache
                    .is_user_history_plain_item_exists(
                        plain_key.as_str(),
                        PlainPostItem {
                            canister_id: publisher_canister_id.to_string(),
                            post_id,
                        },
                    )
                    .await
                {
                    Ok(true) => {
                    }
                    Ok(false) => {
                        // add_user_buffer_items
                        if let Err(e) = ml_feed_cache
                            .add_user_buffer_items_v2(vec![BufferItemV2 {
                                publisher_user_id: publisher_user_id.to_string(),
                                post_id,
                                video_id,
                                item_type,
                                percent_watched: watch_history_item.percent_watched,
                                user_id: user_id.to_string(),
                                timestamp,
                            }])
                            .await
                        {
                            error!("Error adding user watch history buffer items: {:?}", e);
                        }
                    }
                    Err(e) => {
                        error!("Error checking user watch history plain item: {:?}", e);
                    }
                }
            });
        }
    }

    pub fn update_view_count_canister(&self, app_state: &AppState) {
        if self.event.event == "video_duration_watched" {
            let params: Result<VideoDurationWatchedPayload, _> =
                serde_json::from_str(&self.event.params);

            let params = match params {
                Ok(params) => params,
                Err(e) => {
                    error!("Failed to parse video_duration_watched params: {e:?}");
                    return;
                }
            };

            let app_state = app_state.clone();

            tokio::spawn(async move {
                use std::cmp::Ordering;
                use yral_canisters_client::individual_user_template::IndividualUserTemplate;
                use yral_canisters_client::individual_user_template::PostViewDetailsFromFrontend;

                let percentage_watched = params.percentage_watched as u8;
                if percentage_watched == 0 || percentage_watched > 100 {
                    error!("Invalid percentage_watched: {percentage_watched}");
                    return;
                }
                let post_id = params.post_id.unwrap_or_default();
                let publisher_canister_id = params.publisher_canister_id.unwrap();

                let watch_count = 1u8;

                let payload = match percentage_watched.cmp(&95) {
                    Ordering::Less => {
                        PostViewDetailsFromFrontend::WatchedPartially { percentage_watched }
                    }
                    _ => PostViewDetailsFromFrontend::WatchedMultipleTimes {
                        percentage_watched,
                        watch_count,
                    },
                };

                let individual_user_template =
                    IndividualUserTemplate(publisher_canister_id, &app_state.agent);

                if let Err(e) = individual_user_template
                    .update_post_add_view_details(post_id, payload)
                    .await
                {
                    error!(
                        "Failed to update view details for post {} in canister {}: {:?}",
                        post_id, publisher_canister_id, e
                    );
                }
            });
        }
    }

    #[cfg(feature = "local-bin")]
    pub fn update_watch_history_v2(&self, _app_state: &AppState) {
        // No-op for local-bin
    }

    #[deprecated(
        note = "Use update_success_history_v2 instead. will be removed post redis migration"
    )]
    #[cfg(not(feature = "local-bin"))]
    #[allow(dead_code)]
    pub fn update_success_history(&self, app_state: &AppState) {
        let params: Value = serde_json::from_str(&self.event.params).expect("Invalid JSON");
        let app_state = app_state.clone();

        let mut percent_watched = 0.0;

        if self.event.event != "video_duration_watched" && self.event.event != "like_video" {
            return;
        }
        if self.event.event == "video_duration_watched" {
            percent_watched = params["percentage_watched"].as_f64().unwrap();
            if percent_watched < 30.0 {
                return;
            }
        }

        let item_type = self.event.event.clone();

        tokio::spawn(async move {
            let ml_feed_cache = app_state.ml_feed_cache.clone();
            let user_canister_id = params["canister_id"].as_str().unwrap();
            let publisher_canister_id = params["publisher_canister_id"].as_str().unwrap();
            let nsfw_probability = params["nsfw_probability"].as_f64().unwrap_or_default();
            let post_id = params["post_id"].as_u64().unwrap();
            let video_id = params["video_id"].as_str().unwrap();
            let timestamp = std::time::SystemTime::now();

            let success_history_item = MLFeedCacheHistoryItem {
                canister_id: publisher_canister_id.to_string(),
                item_type: item_type.clone(),
                nsfw_probability: nsfw_probability as f32,
                post_id,
                video_id: video_id.to_string(),
                timestamp,
                percent_watched: percent_watched as f32,
            };

            let user_cache_key = format!(
                "{}{}",
                user_canister_id,
                if nsfw_probability <= 0.4 {
                    USER_SUCCESS_HISTORY_CLEAN_SUFFIX
                } else {
                    USER_SUCCESS_HISTORY_NSFW_SUFFIX
                }
            );
            let res = app_state
                .ml_feed_cache
                .add_user_success_history_items(&user_cache_key, vec![success_history_item.clone()])
                .await;
            if res.is_err() {
                error!("Error adding user success history items: {:?}", res.err());
            }

            // add to history plain items
            if item_type == "like_video" {
                let plain_key = format!(
                    "{}{}",
                    user_canister_id, USER_LIKE_HISTORY_PLAIN_POST_ITEM_SUFFIX
                );

                match ml_feed_cache
                    .is_user_history_plain_item_exists(
                        plain_key.as_str(),
                        PlainPostItem {
                            canister_id: publisher_canister_id.to_string(),
                            post_id,
                        },
                    )
                    .await
                {
                    Ok(true) => {
                    }
                    Ok(false) => {
                        // add_user_buffer_items
                        if let Err(e) = ml_feed_cache
                            .add_user_buffer_items(vec![BufferItem {
                                publisher_canister_id: publisher_canister_id.to_string(),
                                post_id,
                                video_id: video_id.to_string(),
                                item_type,
                                percent_watched: percent_watched as f32,
                                user_canister_id: user_canister_id.to_string(),
                                timestamp,
                            }])
                            .await
                        {
                            error!("Error adding user like history buffer items: {:?}", e);
                        }

                        // can do this here, because `like` is absolute. Unline watch which has percent varying everytime
                        if let Err(e) = ml_feed_cache
                            .add_user_history_plain_items(&plain_key, vec![MLFeedCacheHistoryItem {
                                canister_id: success_history_item.canister_id.clone(),
                                item_type: success_history_item.item_type.clone(),
                                post_id: success_history_item.post_id,
                                video_id: success_history_item.video_id.clone(),
                                timestamp: success_history_item.timestamp,
                                percent_watched: success_history_item.percent_watched,
                                nsfw_probability: 0.0,
                            }])
                            .await
                        {
                            error!("Error adding user like history plain items: {:?}", e);
                        }
                    }
                    Err(e) => {
                        error!("Error checking user like history plain item: {:?}", e);
                    }
                }
            }
        });
    }

    #[cfg(not(feature = "local-bin"))]
    pub fn update_success_history_v2(&self, app_state: &AppState) {
        if self.event.event != "video_duration_watched" && self.event.event != "like_video" {
            return;
        }

        let app_state = app_state.clone();
        let item_type = self.event.event.clone();
        let params_str = self.event.params.clone();

        tokio::spawn(async move {
            let ml_feed_cache = app_state.ml_feed_cache.clone();
            let timestamp = std::time::SystemTime::now();

            // Parse parameters using the helper function
            let params = match parse_success_history_params(&item_type, &params_str) {
                Ok(Some(p)) => p,
                Ok(None) => return, // Early return for video_duration_watched < 30%
                Err(e) => {
                    error!("Failed to parse params in update_success_history_v2: {}", e);
                    return;
                }
            };

            let success_history_item = MLFeedCacheHistoryItemV2 {
                publisher_user_id: params.publisher_user_id.clone(),
                canister_id: params.publisher_canister_id.clone(),
                item_type: item_type.clone(),
                post_id: params.post_id,
                video_id: params.video_id.clone(),
                timestamp,
                percent_watched: params.percent_watched as f32,
            };

            let user_cache_key = format!(
                "{}{}",
                params.user_id,
                if params.nsfw_probability <= 0.4 {
                    USER_SUCCESS_HISTORY_CLEAN_SUFFIX_V2
                } else {
                    USER_SUCCESS_HISTORY_NSFW_SUFFIX_V2
                }
            );
            let res = app_state
                .ml_feed_cache
                .add_user_success_history_items_v2(
                    &user_cache_key,
                    vec![success_history_item.clone()],
                )
                .await;
            if res.is_err() {
                error!("Error adding user success history items: {:?}", res.err());
            }

            // add to history plain items
            if item_type == "like_video" {
                let plain_key = format!(
                    "{}{}",
                    params.user_id, USER_LIKE_HISTORY_PLAIN_POST_ITEM_SUFFIX_V2
                );

                match ml_feed_cache
                    .is_user_history_plain_item_exists(
                        plain_key.as_str(),
                        PlainPostItem {
                            canister_id: params.publisher_user_id.clone(),
                            post_id: params.post_id,
                        },
                    )
                    .await
                {
                    Ok(true) => {
                    }
                    Ok(false) => {
                        // add_user_buffer_items
                        if let Err(e) = ml_feed_cache
                            .add_user_buffer_items_v2(vec![BufferItemV2 {
                                publisher_user_id: params.publisher_user_id.clone(),
                                post_id: params.post_id,
                                video_id: params.video_id.clone(),
                                item_type,
                                percent_watched: params.percent_watched as f32,
                                user_id: params.user_id.clone(),
                                timestamp,
                            }])
                            .await
                        {
                            error!("Error adding user like history buffer items: {:?}", e);
                        }

                        // can do this here, because `like` is absolute. Unline watch which has percent varying everytime
                        if let Err(e) = ml_feed_cache
                            .add_user_history_plain_items(&plain_key, vec![MLFeedCacheHistoryItem {
                                canister_id: success_history_item.canister_id.clone(),
                                item_type: success_history_item.item_type.clone(),
                                post_id: success_history_item.post_id,
                                video_id: success_history_item.video_id.clone(),
                                timestamp: success_history_item.timestamp,
                                percent_watched: success_history_item.percent_watched,
                                nsfw_probability: 0.0,
                            }])
                            .await
                        {
                            error!("Error adding user like history plain items: {:?}", e);
                        }
                    }
                    Err(e) => {
                        error!("Error checking user like history plain item: {:?}", e);
                    }
                }
            }
        });
    }

    #[cfg(feature = "local-bin")]
    pub fn update_success_history_v2(&self, _app_state: &AppState) {
        // No-op for local-bin
    }

    #[cfg(feature = "local-bin")]
    #[allow(dead_code)]
    pub fn stream_to_firestore(&self, _app_state: &AppState) {
        // No-op for local-bin feature
    }

    #[cfg(not(feature = "local-bin"))]
    pub fn stream_to_firestore(&self, app_state: &AppState) {
        if self.event.event == "token_creation_completed" {
            let app_state = app_state.clone();
            let params: Value = serde_json::from_str(&self.event.params).expect("Invalid JSON");

            tokio::spawn(async move {
                let data = TokenListItem {
                    user_id: params["user_id"].as_str().unwrap().to_string(),
                    name: params["name"].as_str().unwrap().to_string(),
                    token_name: params["token_name"].as_str().unwrap().to_string(),
                    token_symbol: params["token_symbol"].as_str().unwrap().to_string(),
                    logo: params["logo"].as_str().unwrap().to_string(),
                    description: params["description"].as_str().unwrap().to_string(),
                    created_at: Utc::now(),
                    link: params["link"].as_str().unwrap().to_string(),
                    is_nsfw: params["is_nsfw"].as_bool().unwrap(),
                    nsfw_ec: params["nsfw_ec"].as_str().unwrap().to_string(),
                    nsfw_gore: params["nsfw_gore"].as_str().unwrap().to_string(),
                };

                // link is in the format /token/info/NEW_ID/USER_PRICIPAL
                let parts: Vec<&str> = data.link.split('/').collect();
                let document_id = parts[3]; // Get the NEW_ID part

                let db = app_state.firestoredb.clone();

                let res: Result<TokenListItem, FirestoreError> = db
                    .fluent()
                    .insert()
                    .into("tokens-list")
                    .document_id(document_id)
                    .object(&data)
                    .execute()
                    .await;
                if res.is_err() {
                    log::error!("Error uploading to Firestore : {:?}", res.err());
                }
            });
        }
    }

    #[cfg(not(feature = "local-bin"))]
    pub fn handle_login_successful(&self, app_state: &AppState) -> Result<(), anyhow::Error> {
        if self.event.event == "login_successful" {
            let params: LoginSuccessfulParams = serde_json::from_str(&self.event.params)?;
            let bigquery_client = app_state.bigquery_client.clone();

            tokio::spawn(async move {
                let canister_id = params.canister_id;
                let user_id = params.user_id;

                if let Err(e) =
                    login_successful::handle_login_successful(bigquery_client, canister_id, user_id)
                        .await
                {
                    log::error!("Error handling login successful: {:?}", e);
                }
            });
        }

        Ok(())
    }

    #[cfg(feature = "local-bin")]
    pub fn handle_login_successful(&self, _app_state: &AppState) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[cfg(not(feature = "local-bin"))]
async fn stream_to_bigquery(
    app_state: &AppState,
    data: Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let token = app_state
        .get_access_token(&["https://www.googleapis.com/auth/bigquery.insertdata"])
        .await;
    let client = Client::new();
    let request_url = BIGQUERY_INGESTION_URL.to_string();
    let response = client
        .post(request_url)
        .bearer_auth(token)
        .json(&data)
        .send()
        .await?;

    match response.status().is_success() {
        true => Ok(()),
        false => Err(format!("Failed to stream data - {response_text:?}", response_text = response.text().await?).into()),
    }
}

#[cfg(feature = "local-bin")]
#[allow(dead_code)]
pub async fn stream_to_bigquery_token_metadata_impl_v2(
    _app_state: &AppState,
    _data: ICPumpTokenMetadata,
) -> Result<(), anyhow::Error> {
    Ok(())
}

#[cfg(not(feature = "local-bin"))]
pub async fn stream_to_bigquery_token_metadata_impl_v2(
    app_state: &AppState,
    data: ICPumpTokenMetadata,
) -> Result<(), anyhow::Error> {
    // Upload image to Cloudflare Images and get link

    let link = data.link.clone();
    let key_id = link.split("/").collect::<Vec<&str>>();
    let root_id = key_id[3];

    let base64_image_str = data.logo.clone();
    let base64_image_without_prefix = base64_image_str.replace("data:image/png;base64,", "");

    let cf_images_api_token = env::var("CF_IMAGES_API_TOKEN")?;

    let upload_res = upload_base64_image(
        CLOUDFLARE_ACCOUNT_ID,
        cf_images_api_token.as_str(),
        base64_image_without_prefix.as_str(),
        root_id,
    )
    .await?;

    let logo_link = upload_res.result.variants[0].clone();

    // Stream to BigQuery

    let bq_client = app_state.bigquery_client.clone();

    let query_str = get_icpump_insert_query(
        data.canister_id.clone(),
        data.description.clone(),
        data.host.clone(),
        data.link.clone(),
        logo_link,
        data.token_name.clone(),
        data.token_symbol.clone(),
        data.user_id.clone(),
        data.is_nsfw,
    );

    let request = QueryRequest {
        query: query_str.to_string(),
        ..Default::default()
    };

    match bq_client
        .query::<google_cloud_bigquery::query::row::Row>("hot-or-not-feed-intelligence", request)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            log::error!("Error streaming to BigQuery: {:?}", e);
            Err(anyhow::anyhow!("Error streaming to BigQuery"))
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UploadVideoInfo {
    pub video_id: String,
    pub canister_id: String,
    pub post_id: u64,
    pub timestamp: String,
    pub publisher_user_id: String,
    pub channel_id: Option<String>,
}

#[instrument(skip(state))]
pub async fn upload_video_gcs(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<UploadVideoInfo>,
) -> Result<Json<serde_json::Value>, AppError> {
    upload_gcs_impl(
        &payload.video_id,
        &payload.canister_id,
        &payload.publisher_user_id,
        payload.post_id,
        &payload.timestamp,
    )
    .await?;

    let qstash_client = state.qstash_client.clone();
    qstash_client
        .publish_video_frames(&payload.video_id, &payload)
        .await?;

    Ok(Json(
        serde_json::json!({ "message": "Video uploaded to GCS" }),
    ))
}

pub async fn upload_gcs_impl(
    uid: &str,
    canister_id: &str,
    publisher_user_id: &str,
    post_id: u64,
    timestamp_str: &str,
) -> Result<(), anyhow::Error> {
    let url = format!(
        "https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{}/downloads/default.mp4",
        uid
    );
    let name = format!("{}.mp4", uid);

    let file = reqwest::Client::new()
        .get(&url)
        .send()
        .await?
        .bytes_stream();

    // write to GCS
    let gcs_client = cloud_storage::Client::default();
    let mut res_obj = gcs_client
        .object()
        .create_streamed("yral-videos", file, None, &name, "video/mp4")
        .await?;

    let mut hashmap = HashMap::new();
    hashmap.insert("canister_id".to_string(), canister_id.to_string());
    hashmap.insert(
        "publisher_user_id".to_string(),
        publisher_user_id.to_string(),
    );
    hashmap.insert("post_id".to_string(), post_id.to_string());
    hashmap.insert("timestamp".to_string(), timestamp_str.to_string());
    res_obj.metadata = Some(hashmap);

    // update
    let _ = gcs_client.object().update(&res_obj).await?;

    Ok(())
}
