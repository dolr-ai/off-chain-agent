use crate::app_state::AppState;
use candid::Principal;
use reqwest::Client;
use serde_json::json;
use std::{env, sync::Arc};

/// Send btc_video_viewed event to marketing analytics server
pub async fn send_btc_video_viewed_event(
    video_id: &str,
    publisher_user_id: &Principal,
    is_unique_view: bool,
    source: Option<String>,
    client_type: Option<String>,
    btc_video_view_count: u64,
    btc_video_view_tier: u64,
    share_count: u64,
    like_count: Option<u64>,
    view_count_reward_allocated: bool,
    reward_amount_inr: Option<f64>,
    user_id: &Principal,
    is_logged_in: bool,
    app_state: &Arc<AppState>,
) {
    // Spawn async task to avoid blocking
    let video_id = video_id.to_string();
    let publisher_user_id = publisher_user_id.to_text();
    let user_id_text = user_id.to_text();
    let user_id_principal = *user_id;
    let app_state = app_state.clone();

    tokio::spawn(async move {
        // Fetch canister_id inside spawn (non-blocking)
        let canister_id = app_state
            .get_individual_canister_by_user_principal(user_id_principal)
            .await
            .ok();

        // Build JSON payload
        let timestamp = chrono::Utc::now().timestamp();
        let payload = json!({
            "event": "btc_video_viewed",
            "video_id": video_id,
            "publisher_user_id": publisher_user_id,
            "user_id": user_id_text,
            "principal": user_id_text,
            "is_logged_in": is_logged_in,
            "canister_id": canister_id.map(|c| c.to_text()),
            "is_unique_view": is_unique_view,
            "source": source,
            "client_type": client_type,
            "btc_video_view_count": btc_video_view_count,
            "btc_video_view_tier": btc_video_view_tier,
            "share_count": share_count,
            "like_count": like_count,
            "view_count_reward_allocated": view_count_reward_allocated,
            "reward_amount_inr": reward_amount_inr,
            "ts": timestamp,
        });

        if let Err(e) = send_event_internal(payload).await {
            log::error!("Failed to send btc_video_viewed event: {}", e);
        }
    });
}

async fn send_event_internal(payload: serde_json::Value) -> anyhow::Result<()> {
    // Get analytics server token from environment
    let token = env::var("ANALYTICS_SERVER_TOKEN")
        .unwrap_or_else(|_| {
            log::warn!("ANALYTICS_SERVER_TOKEN not set, skipping analytics event");
            String::new()
        });

    if token.is_empty() {
        return Ok(());
    }

    let url = "https://marketing-analytics-server.fly.dev/api/send_event";

    let client = Client::new();
    let response = client
        .post(url)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", token))
        .json(&payload)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();
        log::error!(
            "Analytics server returned error {}: {}",
            status,
            error_text
        );
        anyhow::bail!("Analytics server error: {}", status);
    }

    // Extract fields from payload for logging
    log::debug!(
        "Successfully sent btc_video_viewed event for video {} (unique: {}, count: {})",
        payload
            .get("video_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown"),
        payload
            .get("is_unique_view")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        payload
            .get("btc_video_view_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
    );

    Ok(())
}
