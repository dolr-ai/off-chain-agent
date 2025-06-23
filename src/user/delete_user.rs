use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use candid::Principal;
use futures::stream::StreamExt;
use google_cloud_bigquery::client::Client;
use ic_agent::Agent;
use serde::{Deserialize, Serialize};
use tracing::instrument;
use utoipa::ToSchema;
use yral_canisters_client::individual_user_template::{IndividualUserTemplate, Result6};

use crate::{
    app_state::AppState,
    posts::{delete_post::bulk_insert_video_delete_rows, types::UserPost},
    types::DelegatedIdentityWire,
    utils::delegated_identity::get_user_info_from_delegated_identity_wire,
};

use super::utils::get_agent_from_delegated_identity_wire;

#[derive(Serialize, Deserialize, ToSchema)]
pub struct DeleteUserRequest {
    pub delegated_identity_wire: DelegatedIdentityWire,
}

#[utoipa::path(
    delete,
    path = "/",
    request_body = DeleteUserRequest,
    tag = "user",
    responses(
        (status = 200, description = "User deleted successfully"),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    )
)]
#[instrument(skip(state, request))]
pub async fn handle_delete_user(
    State(state): State<Arc<AppState>>,
    Json(request): Json<DeleteUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let user_info =
        get_user_info_from_delegated_identity_wire(&state, request.delegated_identity_wire.clone())
            .await
            .map_err(|e| {
                (
                    StatusCode::UNAUTHORIZED,
                    format!("Failed to get user info: {}", e),
                )
            })?;

    let user_principal = user_info.user_principal;
    let user_canister = user_info.user_canister;

    let agent = get_agent_from_delegated_identity_wire(&request.delegated_identity_wire)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // 1. Get all posts for the user from canister
    let posts = get_user_posts(&agent, user_canister).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to get user posts: {}", e),
        )
    })?;

    // 2. Bulk insert into video_deleted table
    if !posts.is_empty() {
        bulk_insert_video_delete_rows(&state.bigquery_client, posts.clone())
            .await
            .map_err(|e| {
                log::error!("Failed to bulk insert video delete rows: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to bulk delete posts: {}", e),
                )
            })?;
    }

    // 3. Delete posts from canister (spawn as background task with concurrency)
    let posts_for_deletion = posts.clone();
    tokio::spawn(async move {
        delete_posts_from_canister(&agent, posts_for_deletion).await;
    });

    // 4. Handle duplicate posts cleanup (spawn as background task with concurrency)
    let bigquery_client = state.bigquery_client.clone();
    let video_ids: Vec<String> = posts.iter().map(|p| p.video_id.clone()).collect();
    tokio::spawn(async move {
        handle_duplicate_posts_cleanup(bigquery_client, video_ids).await;
    });

    // 5. Delete from Redis caches
    let ml_feed_cache = state.ml_feed_cache.clone();
    ml_feed_cache
        .delete_user_caches(&user_canister.to_string())
        .await
        .map_err(|e| {
            log::error!("Failed to delete user caches: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to delete user caches: {}", e),
            )
        })?;

    // TODO: uncomment this when we migrate principal. remove above as well
    // ml_feed_cache
    //     .delete_user_caches(&user_principal.to_string())
    //     .await
    //     .map_err(|e| {
    //         log::error!("Failed to delete user caches: {}", e);
    //         (
    //             StatusCode::INTERNAL_SERVER_ERROR,
    //             format!("Failed to delete user caches: {}", e),
    //         )
    //     })?;

    // 6. Delete user metadata using yral_metadata_client
    state
        .yral_metadata_client
        .delete_metadata_bulk(vec![user_principal])
        .await
        .map_err(|e| {
            log::error!("Failed to delete user metadata: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to delete user metadata: {}", e),
            )
        })?;

    // 7. Delete user from YRAL auth Redis
    #[cfg(not(feature = "local-bin"))]
    {
        state.yral_auth_redis.delete_principal(user_principal)
            .await
            .map_err(|e| {
                log::error!("Failed to delete user from YRAL auth Redis: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to delete user from YRAL auth Redis: {}", e),
                )
            })?;
    }

    // 8. Add deleted canister to SpaceTimeDB
    #[cfg(not(feature = "local-bin"))]
    {
        if let Err(e) = state
            .canisters_ctx
            .add_deleted_canister(user_canister, user_principal)
            .await
        {
            log::error!("Failed to add deleted canister to SpaceTimeDB: {}", e);
            // Don't fail the request if SpaceTimeDB call fails
        }
    }

    Ok((StatusCode::OK, "User deleted successfully".to_string()))
}

async fn get_user_posts(
    agent: &Agent,
    canister_id: Principal,
) -> Result<Vec<UserPost>, anyhow::Error> {
    let mut all_posts = Vec::new();
    let mut start = 0u64;
    let batch_size = 100u64; // Get 100 posts at a time

    loop {
        let end = start + batch_size;

        let result = IndividualUserTemplate(canister_id, agent)
            .get_posts_of_this_user_profile_with_pagination_cursor(start, end)
            .await?;

        match result {
            Result6::Ok(posts) => {
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
            Result6::Err(_) => {
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
        .map(|post| async move {
            let canister_principal = Principal::from_text(&post.canister_id)
                .map_err(|e| format!("Invalid canister principal: {}", e))?;

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
                    format!("Failed to delete post: {}", e)
                })
        })
        .collect();

    let mut buffered = futures::stream::iter(futures).buffer_unordered(10);
    while let Some(result) = buffered.next().await {
        if let Err(e) = result {
            log::error!("Post deletion error: {}", e);
        }
    }
}

async fn handle_duplicate_posts_cleanup(bigquery_client: Client, video_ids: Vec<String>) {
    let futures: Vec<_> = video_ids
        .into_iter()
        .map(|video_id| {
            let client = bigquery_client.clone();
            async move {
                crate::posts::delete_post::handle_duplicate_post_on_delete(client, video_id.clone())
                    .await
                    .map_err(|e| {
                        log::error!(
                            "Failed to handle duplicate post on delete for video {}: {}",
                            video_id,
                            e
                        );
                        anyhow::anyhow!("Failed to handle duplicate post: {}", e)
                    })
            }
        })
        .collect();

    let mut buffered = futures::stream::iter(futures).buffer_unordered(2); // conservative since BQ concurrent requests are limited
    while let Some(result) = buffered.next().await {
        if let Err(e) = result {
            log::error!("Duplicate post cleanup error: {}", e);
        }
    }
}
