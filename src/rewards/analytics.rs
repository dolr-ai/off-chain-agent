use crate::app_state::{AppState, MixpanelClient};
use crate::consts::OFF_CHAIN_AGENT_URL;
use candid::Principal;
use serde_json::json;
use std::sync::Arc;

pub struct BtcVideoViewedEventParams<'a> {
    pub video_id: &'a str,
    pub publisher_user_id: &'a Principal,
    pub is_unique_view: bool,
    pub source: Option<String>,
    pub client_type: Option<String>,
    pub btc_video_view_count: u64,
    pub btc_video_view_tier: u64,
    pub share_count: u64,
    pub like_count: Option<u64>,
    pub view_count_reward_allocated: bool,
    pub reward_amount_inr: Option<f64>,
    pub user_id: &'a Principal,
    pub is_logged_in: bool,
}

pub struct BtcRewardedEventParams<'a> {
    pub creator_id: &'a Principal,
    pub video_id: &'a str,
    pub milestone: u64,
    pub reward_btc: f64,
    pub reward_inr: f64,
    pub view_count: u64,
    pub tx_id: Option<String>,
}

/// Send btc_video_viewed event via the event pipeline
pub async fn send_btc_video_viewed_event(
    params: BtcVideoViewedEventParams<'_>,
    app_state: &Arc<AppState>,
) {
    let mixpanel_client = app_state.mixpanel_client.clone();

    // Spawn async task to avoid blocking
    let video_id = params.video_id.to_string();
    let publisher_user_id = params.publisher_user_id.to_text();
    let user_id_text = params.user_id.to_text();
    let user_id_principal = *params.user_id;
    let is_unique_view = params.is_unique_view;
    let source = params.source;
    let client_type = params.client_type;
    let btc_video_view_count = params.btc_video_view_count;
    let btc_video_view_tier = params.btc_video_view_tier;
    let share_count = params.share_count;
    let like_count = params.like_count;
    let view_count_reward_allocated = params.view_count_reward_allocated;
    let reward_amount_inr = params.reward_amount_inr;
    let is_logged_in = params.is_logged_in;
    let app_state = app_state.clone();

    tokio::spawn(async move {
        // Fetch canister_id inside spawn (non-blocking)
        let canister_id = app_state
            .get_individual_canister_by_user_principal(user_id_principal)
            .await
            .ok();

        // Build JSON payload for params
        let timestamp = chrono::Utc::now().timestamp();
        let event_params = json!({
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

        if let Err(e) =
            send_event_to_pipeline(&mixpanel_client, "btc_video_viewed", event_params.clone()).await
        {
            log::error!("Failed to send btc_video_viewed event to pipeline: {}", e);
        }

        // Send directly to Mixpanel proxy
        if let Err(e) =
            send_event_to_mixpanel(&mixpanel_client, "btc_video_viewed", event_params).await
        {
            log::error!("Failed to send btc_video_viewed event to Mixpanel: {}", e);
        }
    });
}

/// Send btc_rewarded event via the event pipeline
pub async fn send_btc_rewarded_event(
    params: BtcRewardedEventParams<'_>,
    app_state: &Arc<AppState>,
) {
    let mixpanel_client = app_state.mixpanel_client.clone();

    // Spawn async task to avoid blocking
    let creator_id = *params.creator_id;
    let creator_id_text = params.creator_id.to_text();
    let video_id = params.video_id.to_string();
    let milestone = params.milestone;
    let reward_btc = params.reward_btc;
    let reward_inr = params.reward_inr;
    let view_count = params.view_count;
    let tx_id = params.tx_id.clone();
    let app_state = app_state.clone();

    tokio::spawn(async move {
        // Fetch canister_id inside spawn (non-blocking)
        let canister_id = app_state
            .get_individual_canister_by_user_principal(creator_id)
            .await
            .ok();

        // Build JSON payload for params
        let timestamp = chrono::Utc::now().timestamp();
        let event_params = json!({
            "creator_id": creator_id_text,
            "principal": creator_id_text,
            "canister_id": canister_id.map(|c| c.to_text()),
            "video_id": video_id,
            "milestone": milestone,
            "reward_btc": reward_btc,
            "reward_inr": reward_inr,
            "view_count": view_count,
            "tx_id": tx_id,
            "ts": timestamp,
        });

        // Send to event pipeline (BigQuery etc.)
        if let Err(e) =
            send_event_to_pipeline(&mixpanel_client, "btc_rewarded", event_params.clone()).await
        {
            log::error!("Failed to send btc_rewarded event to pipeline: {}", e);
        }

        // Send directly to Mixpanel proxy
        if let Err(e) = send_event_to_mixpanel(&mixpanel_client, "btc_rewarded", event_params).await
        {
            log::error!("Failed to send btc_rewarded event to Mixpanel: {}", e);
        } else {
            log::info!(
                "Successfully sent btc_rewarded event for creator {} (video: {}, milestone: {}, BTC: {:.8}, INR: {:.2})",
                creator_id_text,
                video_id,
                milestone,
                reward_btc,
                reward_inr
            );
        }
    });
}

/// Send event directly to the Mixpanel analytics proxy
async fn send_event_to_mixpanel(
    mixpanel_client: &MixpanelClient,
    event_name: &str,
    params: serde_json::Value,
) -> anyhow::Result<()> {
    let mut body = params;
    if let serde_json::Value::Object(ref mut map) = body {
        map.insert(
            "event".to_string(),
            serde_json::Value::String(event_name.to_string()),
        );
    }

    let response = mixpanel_client
        .client
        .post(&mixpanel_client.url)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", mixpanel_client.token))
        .json(&body)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();
        anyhow::bail!("Mixpanel proxy error {status}: {error_text}");
    }

    log::debug!("Successfully sent '{event_name}' event to Mixpanel");
    Ok(())
}

/// Send event to the /api/v2/events endpoint for processing through the event pipeline
async fn send_event_to_pipeline(
    mixpanel_client: &MixpanelClient,
    event_name: &str,
    params: serde_json::Value,
) -> anyhow::Result<()> {
    let url = OFF_CHAIN_AGENT_URL.join("api/v2/events").unwrap();

    // Format as EventRequest { event: String, params: String }
    let request_body = json!({
        "event": event_name,
        "params": params.to_string(),
    });

    let response = mixpanel_client
        .client
        .post(url.as_str())
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await
        .map_err(|e| {
            log::error!(
                "HTTP request failed for event '{}': {} (url: {})",
                event_name,
                e,
                url
            );
            e
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let error_text = response.text().await.unwrap_or_default();
        log::error!(
            "Event pipeline returned error {} for event '{}': {}",
            status,
            event_name,
            error_text
        );
        anyhow::bail!("Event pipeline error: {}", status);
    }

    log::debug!("Successfully sent '{}' event to pipeline", event_name);

    Ok(())
}
