#![allow(deprecated)]
use crate::app_state::AppState;
use axum::{extract::State, http::StatusCode, response::IntoResponse};
#[cfg(not(feature = "local-bin"))]
use candid::Principal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use yral_alloydb_client::AlloyDbInstance;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileScore {
    pub user_id: String,
    pub score_delta: i64,
}

// Extension trait for AlloyDbInstance
#[cfg(not(feature = "local-bin"))]
pub trait AlloyDbInstanceExt {
    async fn increment_decrement_profile_scores(
        &self,
        scores: Vec<ProfileScore>,
    ) -> Result<(), anyhow::Error>;
}

#[cfg(not(feature = "local-bin"))]
impl AlloyDbInstanceExt for AlloyDbInstance {
    async fn increment_decrement_profile_scores(
        &self,
        scores: Vec<ProfileScore>,
    ) -> Result<(), anyhow::Error> {
        // TODO: Implement actual profile score increment/decrement logic
        // For now, just log and return success
        log::info!("Profile scores to update: {:?}", scores);
        Ok(())
    }
}
#[cfg(not(feature = "local-bin"))]
use std::collections::{HashMap, HashSet};
#[cfg(not(feature = "local-bin"))]
use yral_ml_feed_cache::consts::{
    USER_SUCCESS_HISTORY_CLEAN_SUFFIX, USER_SUCCESS_HISTORY_NSFW_SUFFIX,
    USER_WATCH_HISTORY_CLEAN_SUFFIX, USER_WATCH_HISTORY_NSFW_SUFFIX,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotOrNotProcessPayload {
    pub user_canister_id: String,
    pub publisher_canister_id: String,
    pub post_id: u64,
}

#[deprecated(note = "Use start_hotornot_job_v2 instead. will be removed post redis migration")]
#[cfg(not(feature = "local-bin"))]
#[allow(dead_code)]
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
        .get_history_items_v2("_success_buffer", 0, timestamps_secs)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get _success_buffer from cache {:?}", e),
            )
        })?;

    // get entries to increment/decrement scores (publisher, post_id, is_nsfw)
    let mut payloads: Vec<HotOrNotProcessPayload> = vec![];
    for item in user_buffer_items {
        let user_canister_id = item.canister_id.clone();
        let publisher_canister_id = item.canister_id.clone(); // TODO: Fix this to use correct publisher field
        let post_id = item.post_id;
        payloads.push(HotOrNotProcessPayload {
            user_canister_id,
            publisher_canister_id,
            post_id,
        });
    }

    let user_buffer_items = ml_feed_cache
        .get_history_items_v2("_watch_buffer", 0, timestamps_secs)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get _watch_buffer from cache {:?}", e),
            )
        })?;

    // get entries to increment/decrement scores (publisher, post_id, is_nsfw)
    for item in user_buffer_items {
        let user_canister_id = item.canister_id.clone();
        let publisher_canister_id = item.canister_id.clone(); // TODO: Fix this to use correct publisher field
        let post_id = item.post_id;
        payloads.push(HotOrNotProcessPayload {
            user_canister_id,
            publisher_canister_id,
            post_id,
        });
    }

    let client = state.qstash_client.clone();
    let client_fn = |payload: HotOrNotProcessPayload| {
        let client = client.clone();
        async move {
            let res = client
                .publish_json("hot_or_not_process", &payload, None)
                .await;
            if res.is_err() {
                log::error!("Failed to enqueue hot_or_not_process job: {:?}", res.err());
            }
        }
    };

    for payload in payloads {
        client_fn(payload).await;
    }

    let alloydb_client = state.alloydb_client.clone();
    let res = alloydb_client.increment_decrement_profile_scores(vec![
        ProfileScore {
            user_id: "3cb11a-test".to_string(),
            score_delta: 100,
        },
        ProfileScore {
            user_id: "3cb11a-test2".to_string(),
            score_delta: -100,
        },
        ProfileScore {
            user_id: "3cb11a-test3".to_string(),
            score_delta: -100,
        },
    ]);
    let res_result = res.await;
    log::info!(
        "Result of increment/decrement profile scores: {:?}",
        res_result
    );

    if res_result.is_err() {
        let err_str = format!(
            "Failed to increment/decrement profile scores: {:?}",
            res_result.err()
        );
        log::error!("{}", err_str);
        return Err((StatusCode::INTERNAL_SERVER_ERROR, err_str));
    }

    Ok((StatusCode::OK, "OK"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HotOrNotProcessPayloadV2 {
    pub user_user_id: String,
    pub publisher_user_id: String,
    pub post_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateProfileScorePayload {
    pub user_id: String,
    pub score_update: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateProfileScoresPayload {
    pub updates: Vec<UpdateProfileScorePayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateProfileScoresBulkPayload {
    pub publisher_user_id: String,
    pub post_id: u64,
}

#[cfg(not(feature = "local-bin"))]
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
        .get_history_items_v2(&USER_SUCCESS_HISTORY_CLEAN_SUFFIX, 0, timestamps_secs)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get success buffer items from cache {:?}", e),
            )
        })?;

    let user_buffer_items_nsfw = ml_feed_cache
        .get_history_items_v2(&USER_SUCCESS_HISTORY_NSFW_SUFFIX, 0, timestamps_secs)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get success buffer items from cache {:?}", e),
            )
        })?;

    let user_buffer_items_watch = ml_feed_cache
        .get_history_items_v2(&USER_WATCH_HISTORY_CLEAN_SUFFIX, 0, timestamps_secs)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get watch buffer items from cache {:?}", e),
            )
        })?;

    let user_buffer_items_watch_nsfw = ml_feed_cache
        .get_history_items_v2(&USER_WATCH_HISTORY_NSFW_SUFFIX, 0, timestamps_secs)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get watch buffer items from cache {:?}", e),
            )
        })?;

    let mut score_update_map: HashMap<(Principal, u64), HashSet<Principal>> = HashMap::new();

    for item in user_buffer_items
        .iter()
        .chain(user_buffer_items_nsfw.iter())
        .chain(user_buffer_items_watch.iter())
        .chain(user_buffer_items_watch_nsfw.iter())
    {
        let user_principal =
            Principal::from_text(&item.canister_id).expect("Failed to parse user principal");
        let publisher_principal = Principal::from_text(&item.publisher_user_id)
            .expect("Failed to parse publisher principal");
        let post_id = item.post_id;
        score_update_map
            .entry((publisher_principal, post_id))
            .or_insert_with(HashSet::new)
            .insert(user_principal);
    }

    let alloydb_client = state.alloydb_client.clone();
    let score_updates: Vec<ProfileScore> = score_update_map
        .iter()
        .map(|((publisher_principal, _), user_principals)| ProfileScore {
            user_id: publisher_principal.to_string(),
            score_delta: user_principals.len() as i64,
        })
        .collect();

    let res = alloydb_client
        .increment_decrement_profile_scores(score_updates)
        .await;

    if res.is_err() {
        let err_str = format!(
            "Failed to increment/decrement profile scores: {:?}",
            res.err()
        );
        log::error!("{}", err_str);
        return Err((StatusCode::INTERNAL_SERVER_ERROR, err_str));
    }

    let posts_to_update: Vec<UpdateProfileScoresBulkPayload> = score_update_map
        .iter()
        .map(
            |((publisher_principal, post_id), _)| UpdateProfileScoresBulkPayload {
                publisher_user_id: publisher_principal.to_string(),
                post_id: *post_id,
            },
        )
        .collect();

    let client = state.qstash_client.clone();
    let res = client
        .publish_json_batch("update_profile_scores_bulk", posts_to_update, None)
        .await;

    if res.is_err() {
        let err_str = format!("Failed to enqueue update_profile_scores_bulk job");
        log::error!("{}: {:?}", err_str, res.err());
        return Err((StatusCode::INTERNAL_SERVER_ERROR, err_str));
    }

    Ok((StatusCode::OK, "OK"))
}

#[cfg(feature = "local-bin")]
pub async fn start_hotornot_job_v2(
    State(_state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    Ok((
        StatusCode::OK,
        "Hotornot job not available in local-bin mode",
    ))
}
