use crate::consts::OFF_CHAIN_AGENT_URL;
use crate::events::types::{VideoDurationWatchedPayload, VideoDurationWatchedPayloadV2};
use crate::events::utils::parse_success_history_params;
use crate::{
    app_state::AppState, consts::BIGQUERY_INGESTION_URL, events::warehouse_events::WarehouseEvent,
    AppError,
};
use axum::{extract::State, Json};
use candid::Principal;
use http::header::CONTENT_TYPE;
use log::error;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};
use tracing::instrument;
use yral_ml_feed_cache::consts::{
    USER_LIKE_HISTORY_PLAIN_POST_ITEM_SUFFIX, USER_LIKE_HISTORY_PLAIN_POST_ITEM_SUFFIX_V2,
    USER_SUCCESS_HISTORY_CLEAN_SUFFIX, USER_SUCCESS_HISTORY_CLEAN_SUFFIX_V2,
    USER_SUCCESS_HISTORY_NSFW_SUFFIX, USER_SUCCESS_HISTORY_NSFW_SUFFIX_V2,
    USER_WATCH_HISTORY_CLEAN_SUFFIX, USER_WATCH_HISTORY_CLEAN_SUFFIX_V2,
    USER_WATCH_HISTORY_NSFW_SUFFIX, USER_WATCH_HISTORY_NSFW_SUFFIX_V2,
    USER_WATCH_HISTORY_PLAIN_POST_ITEM_SUFFIX, USER_WATCH_HISTORY_PLAIN_POST_ITEM_SUFFIX_V2,
};
use yral_ml_feed_cache::types::{BufferItem, MLFeedCacheHistoryItem, PlainPostItem};
use yral_ml_feed_cache::types_v2::{BufferItemV2, MLFeedCacheHistoryItemV2, PlainPostItemV2};

pub mod storj;

#[derive(Debug)]
pub struct Event {
    pub event: WarehouseEvent,
}

impl Event {
    pub fn new(event: WarehouseEvent) -> Self {
        Self { event }
    }

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

    pub fn check_video_deduplication(&self, app_state: &AppState) {
        if self.event.event == "video_upload_successful" {
            let params: Value = match serde_json::from_str(&self.event.params) {
                Ok(params) => params,
                Err(e) => {
                    error!(
                        "Failed to parse video_upload_successful event params: {}",
                        e
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
                    "https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{}/downloads/default.mp4",
                    video_id
                );

                log::info!("Sending video for deduplication check: {}", video_id);

                // Create request for video_deduplication endpoint
                let off_chain_ep = OFF_CHAIN_AGENT_URL
                    .join("qstash/video_deduplication")
                    .unwrap();
                let url = qstash_client
                    .base_url
                    .join(&format!("publish/{}", off_chain_ep))
                    .unwrap();

                // TODO: change to struct
                let request_data = serde_json::json!({
                    "video_id": video_id,
                    "video_url": video_url,
                    "publisher_data": {
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
                        "Video deduplication check successfully queued for video_id: {}",
                        video_id
                    ),
                    Err(e) => error!(
                        "Failed to queue video deduplication check for video_id {}: {:?}",
                        video_id, e
                    ),
                }
            });
        }
    }

    #[deprecated(
        note = "Use update_watch_history_v2 instead. will be removed post redis migration"
    )]
    pub fn update_watch_history(&self, app_state: &AppState) {
        if self.event.event == "video_duration_watched" {
            let params: Result<VideoDurationWatchedPayload, _> =
                serde_json::from_str(&self.event.params);

            let params = match params {
                Ok(params) => params,
                Err(e) => {
                    error!("Failed to parse video_duration_watched params: {:?}", e);
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
                    Ok(true) => {}
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

    pub fn update_watch_history_v2(&self, app_state: &AppState) {
        if self.event.event == "video_duration_watched" {
            let params: Result<VideoDurationWatchedPayloadV2, _> =
                serde_json::from_str(&self.event.params);

            let params = match params {
                Ok(params) => params,
                Err(e) => {
                    error!("Failed to parse video_duration_watched params: {:?}", e);
                    return;
                }
            };

            let app_state = app_state.clone();

            tokio::spawn(async move {
                let ml_feed_cache = app_state.ml_feed_cache.clone();

                let percent_watched = params.percentage_watched;
                let nsfw_probability = params.nsfw_probability.unwrap_or_default();

                let user_id = &params.user_id;
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
                    canister_id: "deprecated".to_string(),
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
                    .is_user_history_plain_item_exists_v2(
                        plain_key.as_str(),
                        PlainPostItemV2 {
                            video_id: video_id.clone(),
                        },
                    )
                    .await
                {
                    Ok(true) => {}
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

    // TODO: canister_id being used
    pub fn update_view_count_canister(&self, app_state: &AppState) {
        if self.event.event == "video_duration_watched" {
            let params: Result<VideoDurationWatchedPayload, _> =
                serde_json::from_str(&self.event.params);

            let params = match params {
                Ok(params) => params,
                Err(e) => {
                    error!("Failed to parse video_duration_watched params: {:?}", e);
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
                    error!("Invalid percentage_watched: {}", percentage_watched);
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

    #[deprecated(
        note = "Use update_success_history_v2 instead. will be removed post redis migration"
    )]
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
                    Ok(true) => {}
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
                            .add_user_history_plain_items(&plain_key, vec![success_history_item])
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
                canister_id: "deprecated".to_string(), // Canister ID is not used in this context
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
                    .is_user_history_plain_item_exists_v2(
                        plain_key.as_str(),
                        PlainPostItemV2 {
                            video_id: params.video_id.clone(),
                        },
                    )
                    .await
                {
                    Ok(true) => {}
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
                            .add_user_history_plain_items_v2(&plain_key, vec![success_history_item])
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
}

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
        false => Err(format!("Failed to stream data - {:?}", response.text().await?).into()),
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UploadVideoInfo {
    pub video_id: String,
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
