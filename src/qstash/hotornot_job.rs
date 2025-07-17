use std::{collections::HashMap, sync::Arc};

use axum::{extract::State, response::IntoResponse};
use futures::{stream::FuturesUnordered, StreamExt};
use http::StatusCode;
use yral_ml_feed_cache::{
    consts::{
        USER_WATCH_HISTORY_PLAIN_POST_ITEM_SUFFIX, USER_WATCH_HISTORY_PLAIN_POST_ITEM_SUFFIX_V2,
    },
    types::{MLFeedCacheHistoryItem, PlainPostItem},
    types_v2::{MLFeedCacheHistoryItemV2, PlainPostItemV2},
};

use crate::app_state::AppState;

#[derive(Debug, Clone)]
pub struct InMemoryBufferItem {
    pub video_id: String,
    pub max_percent_watched: f32,
    pub liked_video: bool,
    pub publisher_canister_id: String,
    pub post_id: u64,
}

#[deprecated(note = "Use start_hotornot_job_v2 instead. will be removed post redis migration")]
pub async fn start_hotornot_job(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let ml_feed_cache = state.ml_feed_cache.clone();
    let now = std::time::SystemTime::now();
    let now_minus_1_minute = now - std::time::Duration::from_secs(60); // this will give enough time for latest like or watch event to get their complementary events
    let timestamps_secs = now_minus_1_minute
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // create the inmem index

    let user_buffer_items = ml_feed_cache
        .get_user_buffer_items_by_timestamp(timestamps_secs)
        .await
        .map_err(|e| {
            log::error!("Error getting user buffer items: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    let mut inmem_index = HashMap::<String, HashMap<PlainPostItem, InMemoryBufferItem>>::new();

    for user_buffer_item in user_buffer_items {
        let user_canister_id = user_buffer_item.user_canister_id;
        let publisher_canister_id = user_buffer_item.publisher_canister_id;
        let post_id = user_buffer_item.post_id;
        let res = inmem_index
            .entry(user_canister_id)
            .or_default();
        let post_item = PlainPostItem {
            canister_id: publisher_canister_id.clone(),
            post_id,
        };
        let existing_inmem_buffer_item = res.entry(post_item).or_insert(InMemoryBufferItem {
            video_id: user_buffer_item.video_id,
            max_percent_watched: 0.0,
            liked_video: false,
            publisher_canister_id: publisher_canister_id.clone(),
            post_id,
        });
        // merge the buffer item into the existing buffer item
        // percent_watched = max of the two
        existing_inmem_buffer_item.max_percent_watched = existing_inmem_buffer_item
            .max_percent_watched
            .max(user_buffer_item.percent_watched);
        existing_inmem_buffer_item.liked_video =
            existing_inmem_buffer_item.liked_video || user_buffer_item.item_type == "like_video";
    }

    // for each item, fire a request to alloydb
    let mut queries = Vec::new();

    for (user_canister_id, post_items) in inmem_index {
        let mut plain_post_items = Vec::new();
        let plain_key = format!(
            "{}{}",
            user_canister_id, USER_WATCH_HISTORY_PLAIN_POST_ITEM_SUFFIX
        );

        for (_, inmem_buffer_item) in post_items {
            let query = format!(
                "select hot_or_not_evaluator.update_counter('{}',{},{})",
                inmem_buffer_item.video_id,
                inmem_buffer_item.liked_video,
                inmem_buffer_item.max_percent_watched
            );
            queries.push(query);

            plain_post_items.push(MLFeedCacheHistoryItem {
                canister_id: inmem_buffer_item.publisher_canister_id.clone(),
                post_id: inmem_buffer_item.post_id,
                video_id: inmem_buffer_item.video_id.clone(),
                nsfw_probability: 0.0,
                item_type: "video_duration_watched".to_string(),
                timestamp: now,
                percent_watched: inmem_buffer_item.max_percent_watched,
            });
        }

        if let Err(e) = ml_feed_cache
            .add_user_history_plain_items(&plain_key, plain_post_items)
            .await
        {
            log::error!("Error adding user watch history plain items: {:?}", e);
        }
    }

    let alloydb_client = state.alloydb_client.clone();

    let futures = queries
        .into_iter()
        .map(|query| {
            let alloydb_client = alloydb_client.clone();
            async move {
                alloydb_client.execute_sql_raw(query).await.map_err(|e| {
                    log::error!("Error executing alloydb query: {:?}", e);
                    anyhow::anyhow!("Error executing alloydb query: {:?}", e)
                })
            }
        })
        .collect::<FuturesUnordered<_>>();

    let results = futures.collect::<Vec<_>>().await;
    let errors = results
        .iter()
        .filter_map(|r| r.as_ref().err())
        .collect::<Vec<_>>();

    if errors.len() < results.len() {
        // remove items from redis
        ml_feed_cache
            .remove_user_buffer_items_by_timestamp(timestamps_secs)
            .await
            .map_err(|e| {
                log::error!("Error removing user buffer items: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            })?;
    }

    if !errors.is_empty() {
        let err_str = format!(
            "Num Errors {} executing alloydb queries: {:?}",
            errors.len(),
            errors
        );
        log::error!("{}", err_str);
        return Err((StatusCode::INTERNAL_SERVER_ERROR, err_str));
    }

    Ok((StatusCode::OK, "OK"))
}

#[derive(Debug, Clone)]
pub struct InMemoryBufferItemV2 {
    pub video_id: String,
    pub max_percent_watched: f32,
    pub liked_video: bool,
    pub publisher_user_id: String,
    pub post_id: u64,
}

pub async fn start_hotornot_job_v2(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let ml_feed_cache = state.ml_feed_cache.clone();
    let now = std::time::SystemTime::now();
    let now_minus_1_minute = now - std::time::Duration::from_secs(60); // this will give enough time for latest like or watch event to get their complementary events
    let timestamps_secs = now_minus_1_minute
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // create the inmem index

    let user_buffer_items = ml_feed_cache
        .get_user_buffer_items_by_timestamp_v2(timestamps_secs)
        .await
        .map_err(|e| {
            log::error!("Error getting user buffer items: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    let mut inmem_index = HashMap::<String, HashMap<PlainPostItemV2, InMemoryBufferItemV2>>::new();

    for user_buffer_item in user_buffer_items {
        let user_id = user_buffer_item.user_id;
        let publisher_user_id = user_buffer_item.publisher_user_id;
        let post_id = user_buffer_item.post_id;
        let video_id = user_buffer_item.video_id;
        let res = inmem_index.entry(user_id).or_default();
        let post_item = PlainPostItemV2 {
            video_id: video_id.clone(),
        };
        let existing_inmem_buffer_item = res.entry(post_item).or_insert(InMemoryBufferItemV2 {
            video_id,
            max_percent_watched: 0.0,
            liked_video: false,
            publisher_user_id: publisher_user_id.clone(),
            post_id,
        });
        // merge the buffer item into the existing buffer item
        // percent_watched = max of the two
        existing_inmem_buffer_item.max_percent_watched = existing_inmem_buffer_item
            .max_percent_watched
            .max(user_buffer_item.percent_watched);
        existing_inmem_buffer_item.liked_video =
            existing_inmem_buffer_item.liked_video || user_buffer_item.item_type == "like_video";
    }

    // for each item, fire a request to alloydb
    let mut queries = Vec::new();

    for (user_id, post_items) in inmem_index {
        let mut plain_post_items = Vec::new();
        let plain_key = format!(
            "{}{}",
            user_id, USER_WATCH_HISTORY_PLAIN_POST_ITEM_SUFFIX_V2
        );

        for (_, inmem_buffer_item) in post_items {
            let query = format!(
                "select hot_or_not_evaluator.update_counter('{}',{},{})",
                inmem_buffer_item.video_id,
                inmem_buffer_item.liked_video,
                inmem_buffer_item.max_percent_watched
            );
            queries.push(query);

            plain_post_items.push(MLFeedCacheHistoryItemV2 {
                canister_id: "".to_string(),
                post_id: inmem_buffer_item.post_id,
                video_id: inmem_buffer_item.video_id.clone(),
                item_type: "video_duration_watched".to_string(),
                timestamp: now,
                percent_watched: inmem_buffer_item.max_percent_watched,
                publisher_user_id: inmem_buffer_item.publisher_user_id,
            });
        }

        if let Err(e) = ml_feed_cache
            .add_user_history_plain_items_v2(&plain_key, plain_post_items)
            .await
        {
            log::error!("Error adding user watch history plain items: {:?}", e);
        }
    }

    let alloydb_client = state.alloydb_client.clone();

    let futures = queries
        .into_iter()
        .map(|query| {
            let alloydb_client = alloydb_client.clone();
            async move {
                alloydb_client.execute_sql_raw(query).await.map_err(|e| {
                    log::error!("Error executing alloydb query: {:?}", e);
                    anyhow::anyhow!("Error executing alloydb query: {:?}", e)
                })
            }
        })
        .collect::<FuturesUnordered<_>>();

    let results = futures.collect::<Vec<_>>().await;
    let errors = results
        .iter()
        .filter_map(|r| r.as_ref().err())
        .collect::<Vec<_>>();

    if errors.len() < results.len() {
        // remove items from redis
        ml_feed_cache
            .remove_user_buffer_items_by_timestamp_v2(timestamps_secs)
            .await
            .map_err(|e| {
                log::error!("Error removing user buffer items: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            })?;
    }

    if !errors.is_empty() {
        let err_str = format!(
            "Num Errors {} executing alloydb queries: {:?}",
            errors.len(),
            errors
        );
        log::error!("{}", err_str);
        return Err((StatusCode::INTERNAL_SERVER_ERROR, err_str));
    }

    Ok((StatusCode::OK, "OK"))
}
