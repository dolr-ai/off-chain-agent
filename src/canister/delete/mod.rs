use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use candid::Principal;
use chrono::Utc;
use futures::stream::StreamExt;
use ic_agent::Agent;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tracing::instrument;
use utoipa::ToSchema;
use yral_canisters_client::{
    individual_user_template::IndividualUserTemplate,
    user_index::{Result3, UserIndex},
};

use crate::{
    app_state::AppState,
    posts::{delete_post::bulk_insert_video_delete_rows, types::UserPost},
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CanisterInfo {
    #[schema(value_type = String)]
    pub canister_id: Principal,
    #[schema(value_type = String)]
    pub subnet_id: Principal,
}

#[derive(Debug, Serialize, Deserialize)]
struct FailedCanisterInfo {
    canister_id: String,
    subnet: String,
    user_principal: Option<String>,
    reason: String,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct DeleteAndReclaimCanistersRequest {
    pub canisters: Vec<CanisterInfo>,
}

pub async fn delete_canister_data(
    agent: &Agent,
    state: &Arc<AppState>,
    canister_id: Principal,
    user_principal: Principal,
    to_delete_posts_from_canister: bool,
) -> Result<(), anyhow::Error> {
    log::info!("Deleting canister data for canister {canister_id} and user {user_principal}");

    // 0. Delete user from YRAL auth Redis (this step is unique to user deletion)
    #[cfg(not(feature = "local-bin"))]
    {
        state
            .yral_auth_redis
            .delete_principal(user_principal)
            .await?;
    }

    // 1. Delete user metadata using yral_metadata_client (this step is unique to user deletion)
    // Wont return error if metadata deletion fails, just log it
    if let Err(e) = state
        .yral_metadata_client
        .delete_metadata_bulk(vec![user_principal])
        .await
    {
        log::error!("Failed to delete metadata for user {user_principal}: {e}");
    }

    // Step 2: Get all posts for the canister
    let posts = get_canister_posts(agent, canister_id).await?;

    // Step 3: Bulk insert into video_deleted table if posts exist
    //       : Handle duplicate posts cleanup (spawn as background task)
    if !posts.is_empty() {
        bulk_insert_video_delete_rows(&state.bigquery_client, posts.clone()).await?;

        let bigquery_client = state.bigquery_client.clone();
        let video_ids: Vec<String> = posts.iter().map(|p| p.video_id.clone()).collect();
        let agent = agent.clone();
        tokio::spawn(async move {
            handle_duplicate_posts_cleanup(&agent, bigquery_client, video_ids).await;
        });
    }

    // Step 4: Delete posts from canister (spawn as background task)
    if to_delete_posts_from_canister {
        let agent_clone = agent.clone();
        let posts_for_deletion = posts.clone();
        tokio::spawn(async move {
            delete_posts_from_canister(&agent_clone, posts_for_deletion).await;
        });
    }

    // Step 5: Delete from Redis caches
    // Wont return error if cache deletion fails, just log it
    if let Err(e) = state
        .ml_feed_cache
        .delete_user_caches(&canister_id.to_string())
        .await
    {
        log::error!("Failed to delete Redis caches for canister {canister_id}: {e}");
    }

    if let Err(e) = state
        .ml_feed_cache
        .delete_user_caches_v2(&user_principal.to_string())
        .await
    {
        log::error!("Failed to delete Redis caches for canister {canister_id}: {e}");
    }

    Ok(())
}

async fn get_canister_posts(
    agent: &Agent,
    canister_id: Principal,
) -> Result<Vec<UserPost>, anyhow::Error> {
    let mut all_posts = Vec::new();
    let mut start = 0u64;
    let batch_size = 100u64;

    loop {
        let end = start + batch_size;

        let result = IndividualUserTemplate(canister_id, agent)
            .get_posts_of_this_user_profile_with_pagination_cursor(start, end)
            .await?;

        match result {
            yral_canisters_client::individual_user_template::Result6::Ok(posts) => {
                let result_len = posts.len();
                all_posts.extend(posts.into_iter().map(|p| UserPost {
                    canister_id: canister_id.to_string(),
                    post_id: p.id,
                    video_id: p.video_uid,
                }));
                if result_len < batch_size as usize {
                    break;
                }
            }
            yral_canisters_client::individual_user_template::Result6::Err(_) => {
                break;
            }
        }

        start = end;
    }

    Ok(all_posts)
}

async fn delete_posts_from_canister(agent: &Agent, posts: Vec<UserPost>) {
    let futures: Vec<_> = posts
        .into_iter()
        .map(|post| {
            let agent = agent.clone();
            async move {
                let canister_principal = Principal::from_text(&post.canister_id)
                    .map_err(|e| format!("Invalid canister principal: {e}"))?;

                let individual_user_template = IndividualUserTemplate(canister_principal, &agent);

                individual_user_template
                    .delete_post(post.post_id)
                    .await
                    .map_err(|e| {
                        log::error!(
                            "Failed to delete post {} from canister {}: {}",
                            post.post_id,
                            post.canister_id,
                            e
                        );
                        format!("Failed to delete post: {e}")
                    })
            }
        })
        .collect();

    let mut buffered = futures::stream::iter(futures).buffer_unordered(10);
    while let Some(result) = buffered.next().await {
        if let Err(e) = result {
            log::error!("Post deletion error: {e}");
        }
    }
}

async fn handle_duplicate_posts_cleanup(
    agent: &Agent,
    bigquery_client: google_cloud_bigquery::client::Client,
    video_ids: Vec<String>,
) {
    let futures: Vec<_> = video_ids
        .into_iter()
        .map(|video_id| {
            let client = bigquery_client.clone();
            async move {
                crate::posts::delete_post::handle_duplicate_post_on_delete(
                    agent,
                    client,
                    video_id.clone(),
                )
                .await
                .map_err(|e| {
                    log::error!(
                        "Failed to handle duplicate post on delete for video {video_id}: {e}"
                    );
                    anyhow::anyhow!("Failed to handle duplicate post: {}", e)
                })
            }
        })
        .collect();

    let mut buffered = futures::stream::iter(futures).buffer_unordered(2);
    while let Some(result) = buffered.next().await {
        if let Err(e) = result {
            log::error!("Duplicate post cleanup error: {e}");
        }
    }
}

async fn get_user_principal_from_canister(
    agent: &Agent,
    canister_id: Principal,
) -> Result<Principal, anyhow::Error> {
    let individual_user_template = IndividualUserTemplate(canister_id, agent);

    match individual_user_template.get_profile_details_v_2().await {
        Ok(profile) => Ok(profile.principal_id),
        Err(e) => Err(anyhow::Error::msg(format!(
            "Failed to get user principal from canister {canister_id}: {e}"
        ))),
    }
}

async fn process_canister_deletion_with_error_handling(
    agent: &Agent,
    state: &Arc<AppState>,
    canister_info: CanisterInfo,
    timestamp_str: String,
) -> Result<String, String> {
    let canister_id = canister_info.canister_id;
    let subnet_id = canister_info.subnet_id;

    match process_single_canister_deletion(agent, state, canister_info, timestamp_str.clone()).await
    {
        Ok(result) => Ok(result),
        Err(deletion_error) => {
            let error_msg = deletion_error.error.to_string();

            log::error!(
                "Failed to process canister {canister_id} deletion from subnet {subnet_id}: {error_msg}"
            );

            // Store the failure in Redis
            #[cfg(not(feature = "local-bin"))]
            {
                let redis_key = format!("failed_canister_deletions:{timestamp_str}");
                if let Err(redis_err) = store_failed_canister_info(
                    &state.canister_backup_redis_pool,
                    &redis_key,
                    FailedCanisterInfo {
                        canister_id: canister_id.to_string(),
                        subnet: subnet_id.to_string(),
                        user_principal: deletion_error.user_principal.map(|p| p.to_string()),
                        reason: error_msg.clone(),
                    },
                )
                .await
                {
                    log::error!("Failed to store error in Redis: {redis_err}");
                }
            }

            Err(canister_id.to_string())
        }
    }
}

#[derive(Debug)]
struct CanisterDeletionError {
    user_principal: Option<Principal>,
    error: anyhow::Error,
}

async fn process_single_canister_deletion(
    agent: &Agent,
    state: &Arc<AppState>,
    canister_info: CanisterInfo,
    _timestamp_str: String,
) -> Result<String, CanisterDeletionError> {
    let canister_id = canister_info.canister_id;
    let subnet_id = canister_info.subnet_id;

    // Try to get user principal from canister
    let user_principal = match get_user_principal_from_canister(agent, canister_id).await {
        Ok(principal) => principal,
        Err(e) => {
            return Err(CanisterDeletionError {
                user_principal: None,
                error: anyhow::anyhow!("Failed to get user principal: {}", e),
            });
        }
    };

    // Delete canister data
    if let Err(e) = delete_canister_data(agent, state, canister_id, user_principal, false).await {
        return Err(CanisterDeletionError {
            user_principal: Some(user_principal),
            error: anyhow::anyhow!("Failed to delete canister data: {}", e),
        });
    }

    // Step 3: Delete posts from subnet
    let subnet = UserIndex(subnet_id, agent);
    match subnet.uninstall_individual_user_canister(canister_id).await {
        Ok(Result3::Ok) => Ok(canister_id.to_string()),
        Ok(Result3::Err(e)) => Err(CanisterDeletionError {
            user_principal: Some(user_principal),
            error: anyhow::anyhow!("Failed to uninstall canister: {}", e),
        }),
        Err(e) => Err(CanisterDeletionError {
            user_principal: Some(user_principal),
            error: anyhow::anyhow!("Agent error: {}", e),
        }),
    }
}

#[cfg(not(feature = "local-bin"))]
async fn store_failed_canister_info(
    redis_pool: &crate::types::RedisPool,
    key: &str,
    info: FailedCanisterInfo,
) -> Result<(), anyhow::Error> {
    let mut conn = redis_pool.get().await?;
    let json_data = serde_json::to_string(&info)?;
    conn.rpush::<&str, String, ()>(key, json_data)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to push to Redis list: {}", e))?;
    Ok(())
}

#[utoipa::path(
    post,
    path = "/delete_and_reclaim_canisters",
    request_body = DeleteAndReclaimCanistersRequest,
    tag = "canister",
    responses(
        (status = 200, description = "Bulk canister deletion initiated successfully", body = String),
        (status = 400, description = "Invalid request"),
        (status = 500, description = "Internal server error"),
    )
)]
#[instrument(skip(state, request))]
pub async fn handle_delete_and_reclaim_canisters(
    State(state): State<Arc<AppState>>,
    Json(request): Json<DeleteAndReclaimCanistersRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let agent = state.agent.clone();
    let timestamp_str = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let total_canisters = request.canisters.len();

    // Spawn the entire processing as a background task
    tokio::spawn(async move {
        let mut failed_canisters = Vec::new();

        // Process canisters concurrently with buffer_unordered(50)
        let futures: Vec<_> = request
            .canisters
            .into_iter()
            .map(|canister_info| {
                let state = state.clone();
                let agent = agent.clone();
                let timestamp_str = timestamp_str.clone();
                async move {
                    process_canister_deletion_with_error_handling(
                        &agent,
                        &state,
                        canister_info,
                        timestamp_str,
                    )
                    .await
                }
            })
            .collect();

        let mut buffered = futures::stream::iter(futures).buffer_unordered(50);
        while let Some(result) = buffered.next().await {
            if let Err(canister_id) = result {
                failed_canisters.push(canister_id);
            }
        }

        let processed = total_canisters - failed_canisters.len();
        log::info!(
            "Canister deletion batch completed: {} processed, {} failed",
            processed,
            failed_canisters.len()
        );
    });

    Ok((
        StatusCode::OK,
        format!("Deletion of {total_canisters} canisters initiated"),
    ))
}
