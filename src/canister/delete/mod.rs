use std::sync::Arc;

use candid::Principal;
use futures::stream::StreamExt;
use ic_agent::Agent;
use redis::AsyncCommands;
use tracing::instrument;
use yral_canisters_client::{
    user_info_service::UserInfoService, user_post_service::UserPostService,
};

use crate::yral_auth::dragonfly::{format_to_dragonfly_key, YRAL_AUTH_REDIS_KEY_PREFIX};
use crate::{
    app_state::AppState,
    consts::{USER_INFO_SERVICE_CANISTER_ID, USER_POST_SERVICE_CANISTER_ID},
    posts::{delete_post::bulk_insert_video_delete_rows_v2, types::UserPostV2},
};

/// Delete all data associated with a user's canister:
/// 0. YRAL auth Redis keys (including AI/bot account slots)
/// 1. User metadata (via yral_metadata_client)
/// 2. User info from UserInfoService canister
/// 3. All user posts (fetched + recorded in BigQuery)
/// 4. Posts deleted from canister (background task)
/// 5. Duplicate post cleanup (background task)
#[instrument(skip(agent, state))]
pub async fn delete_canister_data(
    agent: &Agent,
    state: &Arc<AppState>,
    canister_id: Principal,
    user_principal: Principal,
    to_delete_posts_from_canister: bool,
) -> Result<(), anyhow::Error> {
    log::info!("Deleting canister data for canister {canister_id} and user {user_principal}");

    // Collect bot principals during Redis cleanup so we can delete their metadata/canister data too
    let mut bot_principals: Vec<Principal> = Vec::new();

    // 0. Delete user from YRAL auth Redis (this step is unique to user deletion)
    #[cfg(not(feature = "local-bin"))]
    {
        use ic_agent::identity::{Identity, Secp256k1Identity};

        let dragonfly = &state.yral_auth_dragonfly;
        let principal_text = user_principal.to_text();

        dragonfly.delete_principal(principal_text.clone()).await?;

        // First try the reverse lookup in Redis
        let reverse_key = format!("ai-account:{principal_text}");
        let formatted_reverse_key =
            format_to_dragonfly_key(YRAL_AUTH_REDIS_KEY_PREFIX, &reverse_key);

        let bot_owner: Option<String> = dragonfly
            .execute_with_retry(|mut conn| {
                let key = formatted_reverse_key.clone();
                async move { conn.hget::<_, _, Option<String>>(key, "auth").await }
            })
            .await?;

        // If reverse lookup is missing, fall back to querying the canister for account type
        let bot_owner = match bot_owner {
            Some(owner) => Some(owner),
            None => {
                let user_info_service =
                    UserInfoService(*USER_INFO_SERVICE_CANISTER_ID, &state.agent);
                match user_info_service
                    .get_user_profile_details_v_7(user_principal)
                    .await
                {
                    Ok(yral_canisters_client::user_info_service::Result7::Ok(profile)) => {
                        match profile.account_type {
                            yral_canisters_client::user_info_service::UserAccountType::BotAccount {
                                owner,
                            } => {
                                log::info!(
                                    "Reverse lookup missing for bot {principal_text}, found owner {} from canister",
                                    owner.to_text()
                                );
                                Some(owner.to_text())
                            }
                            _ => None,
                        }
                    }
                    Ok(yral_canisters_client::user_info_service::Result7::Err(e)) => {
                        log::warn!(
                            "Failed to get profile for {principal_text} from canister: {e}"
                        );
                        None
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to query canister for {principal_text}: {e}"
                        );
                        None
                    }
                }
            }
        };

        let mut bot_slots: Vec<(u8, String)> = Vec::new();
        for slot in 1..=3u8 {
            let slot_key = format!("{principal_text}-ai-account-{slot}");
            let jwk: Option<String> = dragonfly
                .execute_with_retry(|mut conn| {
                    let key = format_to_dragonfly_key(YRAL_AUTH_REDIS_KEY_PREFIX, &slot_key);
                    async move { conn.hget::<_, _, Option<String>>(key, "auth").await }
                })
                .await?;
            if let Some(jwk_str) = jwk {
                bot_slots.push((slot, jwk_str));
            }
        }

        match (bot_owner, bot_slots.is_empty()) {
            (Some(owner_text), _) => {
                log::info!(
                    "Deleting AI account keys for bot {principal_text} (owner: {owner_text})"
                );

                // Delete forward slot FIRST, then reverse lookup
                // This prevents orphaned slots if the slot cleanup fails
                for slot in 1..=3u8 {
                    let slot_key = format!("{owner_text}-ai-account-{slot}");
                    let jwk_str: Option<String> = dragonfly
                        .execute_with_retry(|mut conn| {
                            let key =
                                format_to_dragonfly_key(YRAL_AUTH_REDIS_KEY_PREFIX, &slot_key);
                            async move { conn.hget::<_, _, Option<String>>(key, "auth").await }
                        })
                        .await?;

                    let Some(jwk_str) = jwk_str else { continue };

                    let Ok(sk) = k256::SecretKey::from_jwk_str(&jwk_str) else {
                        continue;
                    };
                    let identity = Secp256k1Identity::from_private_key(sk);
                    if identity.sender().ok() == Some(user_principal) {
                        log::info!("Deleting forward slot {slot} for bot {principal_text}");
                        let _: () = dragonfly
                            .execute_with_retry(|mut conn| {
                                let key =
                                    format_to_dragonfly_key(YRAL_AUTH_REDIS_KEY_PREFIX, &slot_key);
                                async move { conn.del(key).await }
                            })
                            .await?;
                        break;
                    }
                }

                // Delete reverse lookup AFTER forward slot is cleaned up
                let _: () = dragonfly
                    .execute_with_retry(|mut conn| {
                        let key = formatted_reverse_key.clone();
                        async move { conn.del(key).await }
                    })
                    .await?;
            }
            (None, false) => {
                log::info!(
                    "Deleting AI account keys for main account {principal_text} ({} bots)",
                    bot_slots.len()
                );

                for (slot, jwk_str) in bot_slots {
                    if let Ok(sk) = k256::SecretKey::from_jwk_str(&jwk_str) {
                        let identity = Secp256k1Identity::from_private_key(sk);
                        if let Ok(bot_principal) = identity.sender() {
                            bot_principals.push(bot_principal);

                            // Delete bot's reverse lookup from Redis
                            let bot_reverse_key = format!("ai-account:{}", bot_principal.to_text());
                            let _: () = dragonfly
                                .execute_with_retry(|mut conn| {
                                    let key = format_to_dragonfly_key(
                                        YRAL_AUTH_REDIS_KEY_PREFIX,
                                        &bot_reverse_key,
                                    );
                                    async move { conn.del(key).await }
                                })
                                .await?;
                        }
                    }

                    let slot_key = format!("{principal_text}-ai-account-{slot}");
                    let _: () = dragonfly
                        .execute_with_retry(|mut conn| {
                            let key =
                                format_to_dragonfly_key(YRAL_AUTH_REDIS_KEY_PREFIX, &slot_key);
                            async move { conn.del(key).await }
                        })
                        .await?;
                }
            }
            (None, true) => {}
        }
    }

    // 1. Delete user metadata (including bots if this is a main account)
    let mut principals_to_delete = vec![user_principal];
    principals_to_delete.extend(bot_principals.iter().copied());
    if let Err(e) = state
        .yral_metadata_client
        .delete_metadata_bulk(principals_to_delete)
        .await
    {
        log::error!("Failed to delete metadata for user {user_principal}: {e}");
    }

    // 2. Delete user info from UserInfoService (including bots if this is a main account)
    let user_info_service = UserInfoService(*USER_INFO_SERVICE_CANISTER_ID, &state.agent);
    for bot_principal in &bot_principals {
        if let Err(e) = user_info_service.delete_user_info(*bot_principal).await {
            log::error!(
                "Failed to delete bot {} from canister: {e}",
                bot_principal.to_text()
            );
        }
    }
    if let Err(e) = user_info_service.delete_user_info(user_principal).await {
        log::error!("Failed to delete user info for user {user_principal}: {e}");
    }

    // 3. Get all posts for the user and their bots
    let mut posts = get_canister_posts(agent, canister_id, user_principal).await?;
    for bot_principal in &bot_principals {
        match get_canister_posts(agent, canister_id, *bot_principal).await {
            Ok(bot_posts) => posts.extend(bot_posts),
            Err(e) => log::error!(
                "Failed to get posts for bot {}: {e}",
                bot_principal.to_text()
            ),
        }
    }

    // Step 3: Bulk insert into video_deleted table if posts exist
    //       : Handle duplicate posts cleanup (spawn as background task)
    if !posts.is_empty() {
        bulk_insert_video_delete_rows_v2(
            &state.bigquery_client,
            &state.kvrocks_client,
            posts.clone(),
        )
        .await?;
        let bigquery_client = state.bigquery_client.clone();
        let kvrocks_client = state.kvrocks_client.clone();
        let video_ids: Vec<String> = posts.iter().map(|p| p.video_id.clone()).collect();
        tokio::spawn(async move {
            handle_duplicate_posts_cleanup(bigquery_client, kvrocks_client, video_ids).await;
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

    Ok(())
}

async fn get_canister_posts(
    agent: &Agent,
    canister_id: Principal,
    user_principal: Principal,
) -> Result<Vec<UserPostV2>, anyhow::Error> {
    let mut all_posts = Vec::new();
    let mut start = 0u64;
    let batch_size = 100u64;

    // Route based on canister
    if canister_id == *USER_INFO_SERVICE_CANISTER_ID {
        // Use UserPostService for users registered via USER_INFO_SERVICE
        loop {
            let end = start + batch_size;

            let posts = UserPostService(*USER_POST_SERVICE_CANISTER_ID, agent)
                .get_posts_of_this_user_profile_with_pagination_cursor(user_principal, start, end)
                .await?;

            let result_len = posts.len();
            all_posts.extend(posts.into_iter().map(|p| UserPostV2 {
                canister_id: canister_id.to_string(),
                post_id: p.id, // Already String, no conversion needed
                video_id: p.video_uid,
            }));

            if result_len < batch_size as usize {
                break;
            }

            start = end;
        }
    } else {
        // TODO: migrate to user_post_service/user_info_service
        // Individual user canisters have been decommissioned. Legacy canister_id
        // values that are not USER_INFO_SERVICE_CANISTER_ID are no longer supported.
        log::warn!(
            "Skipping posts fetch for non-user-info-service canister {canister_id} — \
             individual user canisters have been decommissioned"
        );
    }

    Ok(all_posts)
}

async fn delete_posts_from_canister(agent: &Agent, posts: Vec<UserPostV2>) {
    let futures: Vec<_> = posts
        .into_iter()
        .map(|post| {
            let agent = agent.clone();
            async move {
                let canister_principal = Principal::from_text(&post.canister_id)
                    .map_err(|e| format!("Invalid canister principal: {e}"))?;

                // Route based on canister
                if canister_principal == *USER_INFO_SERVICE_CANISTER_ID {
                    // Use UserPostService for USER_INFO_SERVICE users
                    let user_post_service = UserPostService(*USER_POST_SERVICE_CANISTER_ID, &agent);

                    match user_post_service
                        .delete_post(post.post_id.clone()) // post_id is already String
                        .await
                    {
                        Ok(yral_canisters_client::user_post_service::Result_::Ok) => Ok(()),
                        Ok(yral_canisters_client::user_post_service::Result_::Err(_)) => {
                            log::error!(
                                "Failed to delete post {} from UserPostService",
                                post.post_id
                            );
                            Err("Failed to delete post".to_string())
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to delete post {} from UserPostService: {}",
                                post.post_id,
                                e
                            );
                            Err(format!("Failed to delete post: {e}"))
                        }
                    }
                } else {
                    // TODO: migrate to user_post_service/user_info_service
                    // Individual user canisters have been decommissioned. Skip deletion
                    // for posts that still reference a legacy canister_id.
                    log::warn!(
                        "Skipping post deletion for legacy canister {} — \
                         individual user canisters have been decommissioned",
                        post.canister_id
                    );
                    Ok(())
                }
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
    bigquery_client: google_cloud_bigquery::client::Client,
    kvrocks_client: crate::kvrocks::KvrocksClient,
    video_ids: Vec<String>,
) {
    let futures: Vec<_> = video_ids
        .into_iter()
        .map(|video_id| {
            let client = bigquery_client.clone();
            let kvrocks = kvrocks_client.clone();
            async move {
                crate::posts::delete_post::handle_duplicate_post_on_delete(
                    client,
                    &kvrocks,
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
