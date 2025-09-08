use anyhow::{Context, Result};
use candid::Principal;
use chrono::Utc;
use futures::stream::{self, StreamExt};
use serde_json::json;
use std::sync::Arc;
use yral_canisters_common::utils::token::{
    SatsOperations, TokenOperations, TokenOperationsProvider,
};
use yral_username_gen::random_username_from_principal;

use crate::{
    app_state::AppState,
    events::types::{EventPayload, TournamentEndedWinnerPayload, TournamentStartedPayload},
    leaderboard::TokenType,
};
use yral_metadata_types::{
    NotificationPayload, SendNotificationReq, WebpushConfig, WebpushFcmOptions,
};

use super::{
    redis_ops::LeaderboardRedis,
    types::{
        calculate_reward, LeaderboardEntry, TournamentResult, TournamentStatus, UserLastTournament,
    },
};

/// Start a tournament and send notifications to all users
pub async fn start_tournament(tournament_id: &str, app_state: &Arc<AppState>) -> Result<()> {
    let redis = LeaderboardRedis::new(app_state.leaderboard_redis_pool.clone());

    // Get tournament info
    let mut tournament = redis
        .get_tournament_info(tournament_id)
        .await?
        .context("Tournament not found")?;

    // Check if tournament is in the right state (can be Upcoming or already Active for immediate start)
    if tournament.status != TournamentStatus::Upcoming
        && tournament.status != TournamentStatus::Active
    {
        return Err(anyhow::anyhow!(
            "Tournament cannot be started from status: {:?}",
            tournament.status
        ));
    }

    // Only update status if it's not already Active
    if tournament.status != TournamentStatus::Active {
        // Update tournament status to Active
        tournament.status = TournamentStatus::Active;
        tournament.updated_at = Utc::now().timestamp();
        redis.set_tournament_info(&tournament).await?;

        // Set as current tournament
        redis.set_current_tournament(tournament_id).await?;

        // Clear upcoming tournament since this one is now active
        redis.clear_upcoming_tournament().await?;
    }

    // Create tournament started payload
    let payload = TournamentStartedPayload {
        tournament_id: tournament.id.clone(),
        prize_pool: tournament.prize_pool,
        prize_token: tournament.prize_token.to_string(),
        end_time: tournament.end_time,
        metric_display_name: tournament.metric_display_name.clone(),
        metric_type: tournament.metric_type.to_string(),
    };

    // Send notification to all users in background (non-blocking)
    // Note: In production, this should be done via a queue/batch system
    let app_state_clone = app_state.clone();
    tokio::spawn(async move {
        if let Err(e) = send_tournament_start_broadcast(&payload, &app_state_clone).await {
            log::error!("Failed to send tournament start broadcast: {:?}", e);
        } else {
            log::info!("Tournament start broadcast sent successfully");
        }
    });

    // Schedule finalize for end_time
    let delay = tournament.end_time - Utc::now().timestamp();
    if delay > 0 {
        if let Err(e) = app_state
            .qstash_client
            .schedule_tournament_finalize(tournament_id, delay)
            .await
        {
            log::error!("Failed to schedule tournament finalize: {:?}", e);
        } else {
            log::info!(
                "Tournament {} started successfully. Scheduled finalize for {} (in {} seconds)",
                tournament_id,
                tournament.end_time,
                delay
            );
        }
    } else {
        log::warn!(
            "Tournament {} end_time {} is in the past, not scheduling finalize",
            tournament_id,
            tournament.end_time
        );
    }

    Ok(())
}

/// Finalize a tournament: calculate winners, distribute prizes, and send notifications
pub async fn finalize_tournament(tournament_id: &str, app_state: &Arc<AppState>) -> Result<()> {
    let redis = LeaderboardRedis::new(app_state.leaderboard_redis_pool.clone());

    // Get tournament info
    let mut tournament = redis
        .get_tournament_info(tournament_id)
        .await?
        .context("Tournament not found")?;

    // Check if tournament is in the right state
    if tournament.status != TournamentStatus::Active {
        return Err(anyhow::anyhow!("Tournament is not active, cannot finalize"));
    }

    // Update tournament status to Finalizing
    tournament.status = TournamentStatus::Finalizing;
    tournament.updated_at = Utc::now().timestamp();
    redis.set_tournament_info(&tournament).await?;

    // Get top 10 winners for prize distribution
    let top_players = redis
        .get_leaderboard(tournament_id, 0, 9, super::types::SortOrder::Desc)
        .await?;

    // Calculate prize distribution and prepare for token distribution
    let mut distribution_tasks = Vec::new();

    for (rank, (principal_str, score)) in top_players.iter().enumerate() {
        if let Ok(principal) = Principal::from_text(principal_str) {
            let rank = (rank + 1) as u32;
            if let Some(reward) = calculate_reward(rank, tournament.prize_pool as u64) {
                distribution_tasks.push((principal, reward, rank, *score));
            }
        }
    }

    // Distribute prizes if tournament uses YRAL (which is Sats internally)
    if tournament.prize_token == TokenType::YRAL && !distribution_tasks.is_empty() {
        // Get JWT token from environment
        let jwt_token = std::env::var("YRAL_HON_WORKER_JWT").ok();

        // Create Sats operations provider
        let token_ops = TokenOperationsProvider::Sats(SatsOperations::new(jwt_token));
        let token_ops = Arc::new(token_ops);

        // Process distributions concurrently, 5 at a time
        let results: Vec<_> = stream::iter(distribution_tasks.clone())
            .map(|(principal, reward, rank, _)| {
                let token_ops = token_ops.clone();
                async move {
                    match token_ops.add_balance(principal, reward).await {
                        Ok(_) => {
                            log::info!(
                                "Distributed {} SATS to {} (rank {})",
                                reward,
                                principal,
                                rank
                            );
                            Ok((principal, reward, rank))
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to distribute {} SATS to {} (rank {}): {:?}",
                                reward,
                                principal,
                                rank,
                                e
                            );
                            Err((principal, reward, rank, e))
                        }
                    }
                }
            })
            .buffer_unordered(5) // Process 5 concurrent requests at a time
            .collect()
            .await;

        //     // Log summary
        let successful = results.iter().filter(|r| r.is_ok()).count();
        let failed = results.iter().filter(|r| r.is_err()).count();
        log::info!(
            "Prize distribution complete: {} successful, {} failed",
            successful,
            failed
        );
    }

    // Build and save tournament results for winners
    let mut winner_entries = Vec::new();
    let mut total_prize_distributed = 0u64;

    // Collect winner data from distribution_tasks (these have the actual rewards)
    for (principal, reward, rank, score) in &distribution_tasks {
        // Get username for winner
        let username = match app_state
            .yral_metadata_client
            .get_user_metadata_v2(principal.to_string())
            .await
        {
            Ok(Some(metadata)) if !metadata.user_name.trim().is_empty() => metadata.user_name,
            _ => {
                let generated = random_username_from_principal(*principal, 15);
                // Cache generated username
                if let Err(e) = redis.cache_username(*principal, &generated).await {
                    log::warn!("Failed to cache generated username: {:?}", e);
                }
                generated
            }
        };

        winner_entries.push(LeaderboardEntry {
            principal_id: *principal,
            username,
            score: *score,
            rank: *rank,
            reward: Some(*reward),
        });

        total_prize_distributed += reward;

        // Send winner notification
        let winner_payload = TournamentEndedWinnerPayload {
            user_id: *principal,
            tournament_id: tournament_id.to_string(),
            rank: *rank,
            prize_amount: *reward,
            prize_token: tournament.prize_token.to_string(),
            total_participants: redis
                .get_total_participants(tournament_id)
                .await
                .unwrap_or(0),
        };

        // Send notification via the notification system
        let event = EventPayload::TournamentEndedWinner(winner_payload);
        event.send_notification(app_state).await;

        log::info!(
            "Sent winner notification to {} for rank {} with prize {}",
            principal,
            rank,
            reward
        );
    }

    // Create tournament result
    let tournament_result = TournamentResult {
        tournament_id: tournament_id.to_string(),
        user_results: winner_entries,
        total_participants: redis
            .get_total_participants(tournament_id)
            .await
            .unwrap_or(0),
        total_prize_distributed,
        finalized_at: Utc::now().timestamp(),
    };

    // Save tournament results
    if let Err(e) = redis.save_tournament_results(&tournament_result).await {
        log::error!("Failed to save tournament results: {:?}", e);
    }

    // Save last tournament info for all participants
    log::info!("Saving last tournament info for all participants");
    let all_participants = redis
        .get_leaderboard(
            tournament_id,
            0,
            -1, // Get all participants
            super::types::SortOrder::Desc,
        )
        .await?;

    let mut last_tournament_entries = Vec::new();

    for (index, (principal_str, _score)) in all_participants.iter().enumerate() {
        if let Ok(principal) = Principal::from_text(principal_str) {
            let rank = (index + 1) as u32;
            let reward = calculate_reward(rank, tournament.prize_pool as u64);

            let last_tournament_info = UserLastTournament {
                tournament_id: tournament_id.to_string(),
                rank,
                reward,
                status: "unseen".to_string(),
            };

            last_tournament_entries.push((principal, last_tournament_info));
        }
    }

    // Batch save all participants' last tournament info
    if let Err(e) = redis
        .save_batch_user_last_tournaments(last_tournament_entries)
        .await
    {
        log::error!("Failed to save batch user last tournament info: {:?}", e);
    } else {
        log::info!(
            "Successfully saved last tournament info for {} participants",
            all_participants.len()
        );
    }

    // Update tournament status to Completed
    tournament.status = TournamentStatus::Completed;
    tournament.updated_at = Utc::now().timestamp();
    redis.set_tournament_info(&tournament).await?;

    // Add to history
    redis.add_to_history(tournament_id).await?;

    // Clear current tournament if this was the current one
    if let Ok(Some(current)) = redis.get_current_tournament().await {
        if current == tournament_id {
            // Note: You might want to automatically start the next tournament here
            // For now, we'll just log
            log::info!(
                "Tournament {} finalized, no new tournament set as current",
                tournament_id
            );
        }
    }

    log::info!("Tournament {} finalized successfully", tournament_id);

    Ok(())
}

/// End a tournament manually (just change status to Ended without processing)
pub async fn end_tournament(tournament_id: &str, app_state: &Arc<AppState>) -> Result<()> {
    let redis = LeaderboardRedis::new(app_state.leaderboard_redis_pool.clone());

    // Get tournament info
    let mut tournament = redis
        .get_tournament_info(tournament_id)
        .await?
        .context("Tournament not found")?;

    // Check if tournament can be ended (Active or Finalizing)
    if tournament.status != TournamentStatus::Active
        && tournament.status != TournamentStatus::Finalizing
    {
        return Err(anyhow::anyhow!(
            "Tournament cannot be ended from status: {:?}",
            tournament.status
        ));
    }

    // Update tournament status to Ended
    tournament.status = TournamentStatus::Ended;
    tournament.updated_at = Utc::now().timestamp();
    redis.set_tournament_info(&tournament).await?;

    // Clear current tournament if this was the current one
    if let Ok(Some(current)) = redis.get_current_tournament().await {
        if current == tournament_id {
            log::info!(
                "Tournament {} manually ended, clearing current tournament",
                tournament_id
            );
            // You might want to clear the current tournament or set a new one
            // For now, we'll just log
        }
    }

    log::info!(
        "Tournament {} manually ended (status set to Ended)",
        tournament_id
    );

    Ok(())
}

/// Send broadcast notification for tournament start
/// Sends notifications to all users with a concurrency limit of 500
async fn send_tournament_start_broadcast(
    payload: &TournamentStartedPayload,
    app_state: &Arc<AppState>,
) -> Result<()> {
    let title = "New Tournament Started!";
    let body = "The new YRAL tournament is live!  Play to climb the leaderboard and win rewards.";

    // Create notification payload
    let notif_payload = SendNotificationReq {
        notification: Some(NotificationPayload {
            title: Some(title.to_string()),
            body: Some(body.to_string()),
            image: Some("https://yral.com/img/yral/android-chrome-384x384.png".to_string()),
        }),
        data: Some(json!({
            "event": "tournament_started",
            "tournament_id": payload.tournament_id,
            "prize_pool": payload.prize_pool,
            "prize_token": payload.prize_token,
            "metric_type": payload.metric_type,
        })),
        android: None,
        webpush: Some(WebpushConfig {
            fcm_options: Some(WebpushFcmOptions {
                link: Some("https://yral.com/leaderboard".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        apns: None,
        ..Default::default()
    };

    // PRODUCTION CODE (commented out for testing with internal users)
    // Fetch all user principals from the canister system
    // let user_principal_canister_list =
    //     match get_user_principal_canister_list_v2(&app_state.agent).await {
    //         Ok(list) => list,
    //         Err(e) => {
    //             log::error!("Failed to fetch user principals: {}", e);
    //             // Fallback to empty list or could use a cached list
    //             vec![]
    //         }
    //     };

    // // Extract just the user principals
    // let users: Vec<Principal> = user_principal_canister_list
    //     .into_iter()
    //     .map(|(user_principal, _canister_principal)| user_principal)
    //     .collect();

    // TESTING CODE - Fetch internal users from Redis
    let redis = LeaderboardRedis::new(app_state.leaderboard_redis_pool.clone());
    let internal_user_strings = match redis.get_internal_users().await {
        Ok(users) => users,
        Err(e) => {
            log::error!("Failed to fetch internal users from Redis: {}", e);
            vec![]
        }
    };

    // Convert string principals to Principal objects
    let users: Vec<Principal> = internal_user_strings
        .into_iter()
        .filter_map(|principal_str| {
            Principal::from_text(&principal_str)
                .map_err(|e| log::warn!("Invalid principal string '{}': {}", principal_str, e))
                .ok()
        })
        .collect();

    let total_users = users.len();

    if total_users == 0 {
        log::warn!("No users found to send tournament notifications to");
        return Ok(());
    }

    log::info!(
        "Sending tournament start notifications to {} users for tournament {}",
        total_users,
        payload.tournament_id
    );

    // Send notifications concurrently with a limit of 500
    let notification_client = app_state.notification_client.clone();
    let notif_payload_arc = Arc::new(notif_payload);

    let sent_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    stream::iter(users)
        .for_each_concurrent(500, |user_principal| {
            let client = notification_client.clone();
            let payload = notif_payload_arc.clone();
            let sent = sent_count.clone();

            async move {
                // Send notification and track result
                client
                    .send_notification((*payload).clone(), user_principal)
                    .await;

                // Track progress
                let count = sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

                // Log progress every 100 notifications
                if count % 100 == 0 {
                    log::info!("Notification progress: {}/{} sent", count, total_users);
                }
            }
        })
        .await;

    let final_sent = sent_count.load(std::sync::atomic::Ordering::Relaxed);

    log::info!(
        "Tournament broadcast completed: {}/{} notifications sent successfully for tournament {}",
        final_sent,
        total_users,
        payload.tournament_id
    );

    Ok(())
}

/// Check if a tournament should start or finalize based on current time
pub async fn check_tournament_lifecycle(app_state: &Arc<AppState>) -> Result<()> {
    let redis = LeaderboardRedis::new(app_state.leaderboard_redis_pool.clone());
    let now = Utc::now().timestamp();

    // Check current tournament
    if let Some(current_id) = redis.get_current_tournament().await? {
        if let Some(tournament) = redis.get_tournament_info(&current_id).await? {
            // Check if tournament should be finalized
            if tournament.status == TournamentStatus::Active && now >= tournament.end_time {
                log::info!(
                    "Tournament {} has reached end time, finalizing...",
                    current_id
                );
                finalize_tournament(&current_id, app_state).await?;
            }
        }
    }

    Ok(())
}
