use axum::{body::Bytes, extract::State, http::HeaderMap, response::IntoResponse};
use hmac::{Hmac, Mac};
use http::StatusCode;
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use std::sync::Arc;

use crate::{app_state::AppState, offchain_service::send_message_gchat};

static GCHAT_SENTRY_WEBHOOK_URL: Lazy<String> = Lazy::new(|| {
    std::env::var("GCHAT_SENTRY_WEBHOOK_URL").expect("GCHAT_SENTRY_WEBHOOK_URL must be set")
});

static SENTRY_CLIENT_SECRET_1: Lazy<String> = Lazy::new(|| {
    std::env::var("SENTRY_CLIENT_SECRET_1").expect("SENTRY_CLIENT_SECRET_1 must be set")
});

static SENTRY_CLIENT_SECRET_2: Lazy<String> = Lazy::new(|| {
    std::env::var("SENTRY_CLIENT_SECRET_2").expect("SENTRY_CLIENT_SECRET_2 must be set")
});

fn verify_sentry_signature(body: &[u8], signature: &str, secret: &str) -> bool {
    type HmacSha256 = Hmac<Sha256>;

    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };

    mac.update(body);
    let expected = hex::encode(mac.finalize().into_bytes());

    expected == signature
}
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct AlertRulePayload {
    pub action: String, // Always "triggered" for alert rules
    pub data: AlertRuleData,
}

#[derive(Debug, Deserialize)]
pub struct AlertRuleData {
    pub event: AlertEvent,
    pub triggered_rule: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct AlertEvent {
    pub event_id: Option<String>,
    pub title: Option<String>,
    pub message: Option<String>,
    pub web_url: Option<String>,
    pub issue_url: Option<String>,
    pub issue_id: Option<String>,
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default)]
    pub environment: Option<String>,
    #[serde(default)]
    pub release: Option<String>,
    #[serde(default)]
    pub platform: Option<String>,
    #[serde(default)]
    pub location: Option<String>,
    #[serde(default)]
    pub culprit: Option<String>,
    #[serde(default)]
    pub project: Option<i64>,
    #[serde(default)]
    pub project_name: Option<String>,
    #[serde(default)]
    pub project_slug: Option<String>,
}

fn get_level_emoji(level: Option<&str>) -> &'static str {
    match level {
        Some("fatal") => "💀",
        Some("error") => "🔴",
        Some("warning") => "🟡",
        Some("info") => "🔵",
        Some("debug") => "⚪",
        _ => "🔴",
    }
}

fn build_gchat_message_from_alert(payload: &AlertRulePayload) -> Value {
    let event = &payload.data.event;

    let level = event.level.as_deref();
    let emoji = get_level_emoji(level);
    let level_text = level.unwrap_or("error").to_uppercase();

    let title = event.title.as_deref().unwrap_or("Unknown Error");
    let web_url = event.web_url.as_deref().unwrap_or("#");
    let environment = event.environment.as_deref().unwrap_or("production");
    let culprit = event
        .culprit
        .as_deref()
        .or(event.location.as_deref())
        .unwrap_or("Unknown");
    let project_name = event.project_name.as_deref().unwrap_or("Unknown Project");
    let triggered_rule = payload
        .data
        .triggered_rule
        .as_deref()
        .unwrap_or("Alert Rule");
    let event_id = event
        .event_id
        .as_deref()
        .unwrap_or("unknown")
        .chars()
        .take(8)
        .collect::<String>();

    json!({
        "cardsV2": [{
            "cardId": format!("sentry-alert-{}", event_id),
            "card": {
                "header": {
                    "title": format!("{} Sentry Alert", emoji),
                    "subtitle": format!("{} | {} | {}", triggered_rule, level_text, project_name)
                },
                "sections": [{
                    "widgets": [
                        {
                            "textParagraph": {
                                "text": format!("<b>{}</b>", title)
                            }
                        },
                        {
                            "decoratedText": {
                                "topLabel": "Culprit",
                                "text": culprit
                            }
                        },
                        {
                            "decoratedText": {
                                "topLabel": "Environment",
                                "text": environment
                            }
                        },
                        {
                            "buttonList": {
                                "buttons": [{
                                    "text": "View in Sentry",
                                    "onClick": {
                                        "openLink": {
                                            "url": web_url
                                        }
                                    }
                                }]
                            }
                        }
                    ]
                }]
            }
        }]
    })
}

/// Sentry webhook handler - forwards alerts to Google Chat
pub async fn sentry_webhook_handler(
    State(_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    log::info!(
        "=== SENTRY WEBHOOK RECEIVED === body_len={} headers={:?}",
        body.len(),
        headers
            .iter()
            .filter(|(k, _)| k.as_str().starts_with("sentry") || k.as_str() == "content-type")
            .map(|(k, v)| format!("{}={:?}", k, v))
            .collect::<Vec<_>>()
    );

    let signature = headers
        .get("sentry-hook-signature")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    log::info!("Sentry webhook: signature={}", signature);

    let sig_valid_1 = verify_sentry_signature(&body, signature, &SENTRY_CLIENT_SECRET_1);
    let sig_valid_2 = verify_sentry_signature(&body, signature, &SENTRY_CLIENT_SECRET_2);
    log::info!(
        "Sentry webhook: sig_valid_1={}, sig_valid_2={}",
        sig_valid_1,
        sig_valid_2
    );

    if !sig_valid_1 && !sig_valid_2 {
        log::warn!("Sentry webhook: invalid signature");
        if let Ok(raw) = String::from_utf8(body.to_vec()) {
            log::warn!("Raw body (first 200 chars): {}", &raw[..raw.len().min(200)]);
        }
        return (StatusCode::UNAUTHORIZED, "Invalid signature");
    }

    // Check the resource type from header
    let resource = headers
        .get("sentry-hook-resource")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    log::info!("Received Sentry webhook: resource={}", resource);

    // Only handle event_alert (from Alert Rules)
    if resource != "event_alert" {
        log::debug!("Skipping non-alert webhook: resource={}", resource);
        return (StatusCode::OK, "OK");
    }

    let payload: AlertRulePayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            log::error!("Failed to parse Sentry alert payload: {}", e);
            // Log the raw body for debugging
            if let Ok(raw) = String::from_utf8(body.to_vec()) {
                log::error!("Raw payload: {}", &raw[..raw.len().min(500)]);
            }
            return (StatusCode::BAD_REQUEST, "Invalid payload");
        }
    };

    log::info!(
        "Sentry alert: rule={}, event={}",
        payload.data.triggered_rule.as_deref().unwrap_or("unknown"),
        payload.data.event.title.as_deref().unwrap_or("unknown")
    );

    let gchat_message = build_gchat_message_from_alert(&payload);

    match send_message_gchat(&GCHAT_SENTRY_WEBHOOK_URL, gchat_message).await {
        Ok(_) => {
            log::info!("Successfully forwarded Sentry alert to Google Chat");
            (StatusCode::OK, "OK")
        }
        Err(e) => {
            log::error!("Failed to forward Sentry alert to Google Chat: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Failed to forward alert")
        }
    }
}
