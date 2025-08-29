use anyhow::{Context, Result};
use candid::Principal;
use chrono::Utc;
use serde_json::json;
use std::sync::Arc;

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
    types::{calculate_reward, TournamentStatus},
};

/// Start a tournament and send notifications to all users
pub async fn start_tournament(tournament_id: &str, app_state: &Arc<AppState>) -> Result<()> {
    let redis = LeaderboardRedis::new(app_state.leaderboard_redis_pool.clone());

    // Get tournament info
    let mut tournament = redis
        .get_tournament_info(tournament_id)
        .await?
        .context("Tournament not found")?;

    // Check if tournament is in the right state
    if tournament.status != TournamentStatus::Upcoming {
        return Err(anyhow::anyhow!("Tournament is not in Upcoming status"));
    }

    // Update tournament status to Active
    tournament.status = TournamentStatus::Active;
    tournament.updated_at = Utc::now().timestamp();
    redis.set_tournament_info(&tournament).await?;

    // Set as current tournament
    redis.set_current_tournament(tournament_id).await?;

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

    log::info!("Tournament {} started successfully", tournament_id);

    Ok(())
}

/// End a tournament, calculate winners, and send notifications
pub async fn end_tournament(tournament_id: &str, app_state: &Arc<AppState>) -> Result<()> {
    let redis = LeaderboardRedis::new(app_state.leaderboard_redis_pool.clone());

    // Get tournament info
    let mut tournament = redis
        .get_tournament_info(tournament_id)
        .await?
        .context("Tournament not found")?;

    // Check if tournament is in the right state
    if tournament.status != TournamentStatus::Active {
        return Err(anyhow::anyhow!("Tournament is not active"));
    }

    // Update tournament status to Finalizing
    tournament.status = TournamentStatus::Finalizing;
    tournament.updated_at = Utc::now().timestamp();
    redis.set_tournament_info(&tournament).await?;

    // Get top 10 winners
    let winners = redis.get_leaderboard(tournament_id, 0, 9).await?;

    // Send notifications to winners
    for (index, (principal_str, _score)) in winners.iter().enumerate() {
        let rank = (index + 1) as u32;

        if let Ok(principal) = Principal::from_text(principal_str) {
            if let Some(prize_amount) = calculate_reward(rank, tournament.prize_pool as u64) {
                // Create winner payload
                let winner_payload = TournamentEndedWinnerPayload {
                    user_id: principal,
                    tournament_id: tournament_id.to_string(),
                    rank,
                    prize_amount,
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
                    prize_amount
                );
            }
        }
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
                "Tournament {} ended, no new tournament set as current",
                tournament_id
            );
        }
    }

    log::info!("Tournament {} ended successfully", tournament_id);

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

/// Check if a tournament should start or end based on current time
pub async fn check_tournament_lifecycle(app_state: &Arc<AppState>) -> Result<()> {
    let redis = LeaderboardRedis::new(app_state.leaderboard_redis_pool.clone());
    let now = Utc::now().timestamp();

    // Check current tournament
    if let Some(current_id) = redis.get_current_tournament().await? {
        if let Some(tournament) = redis.get_tournament_info(&current_id).await? {
            // Check if tournament should end
            if tournament.status == TournamentStatus::Active && now >= tournament.end_time {
                log::info!(
                    "Tournament {} has reached end time, finalizing...",
                    current_id
                );
                end_tournament(&current_id, app_state).await?;
            }
        }
    }

    // Check for tournaments that should start
    // This would typically query a list of upcoming tournaments
    // For now, this is a placeholder for the logic

    Ok(())
}
