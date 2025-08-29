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
};
use yral_metadata_types::{
    AndroidConfig, AndroidNotification, ApnsConfig, ApnsFcmOptions, NotificationPayload,
    SendNotificationReq, WebpushConfig, WebpushFcmOptions,
};

use super::{
    redis_ops::LeaderboardRedis,
    types::{calculate_reward, LeaderboardEntry, TokenType, TournamentResult, TournamentStatus},
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
    if tournament.status != TournamentStatus::Upcoming && tournament.status != TournamentStatus::Active {
        return Err(anyhow::anyhow!("Tournament cannot be started from status: {:?}", tournament.status));
    }

    // Only update status if it's not already Active
    if tournament.status != TournamentStatus::Active {
        // Update tournament status to Active
        tournament.status = TournamentStatus::Active;
        tournament.updated_at = Utc::now().timestamp();
        redis.set_tournament_info(&tournament).await?;

        // Set as current tournament
        redis.set_current_tournament(tournament_id).await?;
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

    // Send notification to all users (batch process)
    // Note: In production, this should be done via a queue/batch system
    // For now, we'll send a broadcast notification
    send_tournament_start_broadcast(&payload, app_state).await?;

    // TODO: Schedule finalize for end_time
    log::info!(
        "Tournament {} started successfully. Should schedule finalize for {}",
        tournament_id,
        tournament.end_time
    );

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
    let top_players = redis.get_leaderboard(tournament_id, 0, 9).await?;

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
                            log::info!("Distributed {} SATS to {} (rank {})", reward, principal, rank);
                            Ok((principal, reward, rank))
                        }
                        Err(e) => {
                            log::error!("Failed to distribute {} SATS to {} (rank {}): {:?}", 
                                       reward, principal, rank, e);
                            Err((principal, reward, rank, e))
                        }
                    }
                }
            })
            .buffer_unordered(5) // Process 5 concurrent requests at a time
            .collect()
            .await;

        // Log summary
        let successful = results.iter().filter(|r| r.is_ok()).count();
        let failed = results.iter().filter(|r| r.is_err()).count();
        log::info!("Prize distribution complete: {} successful, {} failed", successful, failed);
    }

    // Build and save tournament results for winners
    let mut winner_entries = Vec::new();
    let mut total_prize_distributed = 0u64;
    
    // Collect winner data from distribution_tasks (these have the actual rewards)
    for (principal, reward, rank, score) in &distribution_tasks {
        // Get username for winner
        let username = match app_state.yral_metadata_client.get_user_metadata_v2(principal.to_string()).await {
            Ok(Some(metadata)) if !metadata.user_name.trim().is_empty() => metadata.user_name,
            _ => {
                let generated = random_username_from_principal(*principal, 15);
                // Cache generated username
                if let Err(e) = redis.cache_username(*principal, &generated, 3600).await {
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
            principal, rank, reward
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
    if tournament.status != TournamentStatus::Active && tournament.status != TournamentStatus::Finalizing {
        return Err(anyhow::anyhow!("Tournament cannot be ended from status: {:?}", tournament.status));
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

    log::info!("Tournament {} manually ended (status set to Ended)", tournament_id);

    Ok(())
}

/// Send broadcast notification for tournament start
/// In production, this should be handled by a batch notification system
async fn send_tournament_start_broadcast(
    payload: &TournamentStartedPayload,
    app_state: &Arc<AppState>,
) -> Result<()> {
    // Format end time as human readable
    let end_time_str = chrono::DateTime::from_timestamp(payload.end_time, 0)
        .map(|dt| dt.format("%B %d at %I:%M %p UTC").to_string())
        .unwrap_or_else(|| "soon".to_string());

    let title = "New Tournament Started!";
    let body = format!(
        "Compete to win {} {} prizes! Track {} until {}",
        payload.prize_pool, payload.prize_token, payload.metric_display_name, end_time_str
    );

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

    // In a production system, you would:
    // 1. Query all users with notification tokens
    // 2. Batch them into groups
    // 3. Send notifications in parallel
    //
    // For now, we'll use a broadcast approach via a special broadcast endpoint
    // or topic subscription (e.g., all users subscribe to a "tournaments" topic)

    // Example: Send to a broadcast topic (requires metadata service support)
    // app_state.notification_client.send_broadcast_notification(notif_payload).await;

    // For now, just log the broadcast intent
    log::info!(
        "Tournament start broadcast notification prepared for tournament {}",
        payload.tournament_id
    );

    // TODO: Implement actual broadcast mechanism with metadata service
    // This might involve:
    // - Fetching all users with FCM tokens from metadata service
    // - Batching notifications (FCM has a limit of 500 devices per multicast)
    // - Sending via FCM topic subscriptions

    let users = vec!["ebp2n-emlpn-pz52l-oymqw-gwyt3-uadux-4wamr-sbkva-ruwat-i2dbf-qqe"];

    for user in users {
        log::info!("Would send notification to user: {}", user);
        app_state
            .notification_client
            .send_notification(notif_payload.clone(), Principal::from_text(user).unwrap())
            .await;
    }

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

    // Check for tournaments that should start
    // This would typically query a list of upcoming tournaments
    // For now, this is a placeholder for the logic

    Ok(())
}
