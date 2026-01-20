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

static SENTRY_CLIENT_SECRET: Lazy<String> =
    Lazy::new(|| std::env::var("SENTRY_CLIENT_SECRET").expect("SENTRY_CLIENT_SECRET must be set"));

fn verify_sentry_signature(body: &[u8], signature: &str, secret: &str) -> bool {
    type HmacSha256 = Hmac<Sha256>;

    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };

    mac.update(body);
    let expected = hex::encode(mac.finalize().into_bytes());

    expected == signature
}

/// Sentry webhook payload (simplified - Sentry sends more fields)
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct SentryWebhookPayload {
    pub action: String,
    pub data: SentryData,
    #[serde(default)]
    actor: Option<SentryActor>,
}

#[derive(Debug, Deserialize)]
pub struct SentryData {
    pub issue: Option<SentryIssue>,
    pub event: Option<SentryEvent>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct SentryIssue {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub culprit: Option<String>,
    #[serde(default, rename = "shortId")]
    pub short_id: Option<String>,
    #[serde(rename = "permalink")]
    pub url: Option<String>,
    pub project: SentryProject,
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(rename = "firstSeen")]
    pub first_seen: Option<String>,
    #[serde(rename = "lastSeen")]
    pub last_seen: Option<String>,
    pub count: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct SentryEvent {
    event_id: Option<String>,
    title: Option<String>,
    message: Option<String>,
    #[serde(default)]
    pub environment: Option<String>,
    #[serde(default)]
    release: Option<String>,
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct SentryProject {
    id: String,
    pub name: String,
    slug: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct SentryActor {
    #[serde(rename = "type")]
    actor_type: String,
    id: Option<String>,
    name: Option<String>,
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

fn get_action_text(action: &str) -> &'static str {
    match action {
        "triggered" => "New Issue Triggered",
        "resolved" => "Issue Resolved ✅",
        "assigned" => "Issue Assigned",
        "archived" => "Issue Archived",
        "unresolved" => "Issue Unresolved",
        "ignored" => "Issue Ignored",
        _ => "Issue Update",
    }
}

fn build_gchat_message(payload: &SentryWebhookPayload) -> Value {
    let action_text = get_action_text(&payload.action);

    if let Some(issue) = &payload.data.issue {
        let level = issue.level.as_deref();
        let emoji = get_level_emoji(level);
        let level_text = level.unwrap_or("error").to_uppercase();

        let issue_url = issue.url.as_deref().unwrap_or("#");
        let short_id = issue.short_id.as_deref().unwrap_or(&issue.id);
        let culprit = issue.culprit.as_deref().unwrap_or("Unknown");
        let count = issue.count.as_deref().unwrap_or("1");
        let first_seen = issue.first_seen.as_deref().unwrap_or("Unknown");
        let last_seen = issue.last_seen.as_deref().unwrap_or("Unknown");

        // Get environment from event if available
        let environment = payload
            .data
            .event
            .as_ref()
            .and_then(|e| e.environment.as_deref())
            .unwrap_or("production");

        json!({
            "cardsV2": [{
                "cardId": format!("sentry-{}", issue.id),
                "card": {
                    "header": {
                        "title": format!("{} Sentry Alert", emoji),
                        "subtitle": format!("{} | {} | {}", action_text, level_text, issue.project.name)
                    },
                    "sections": [{
                        "header": short_id,
                        "widgets": [
                            {
                                "textParagraph": {
                                    "text": format!("<b>{}</b>", issue.title)
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
                                "decoratedText": {
                                    "topLabel": "Occurrences",
                                    "text": count
                                }
                            },
                            {
                                "decoratedText": {
                                    "topLabel": "First Seen",
                                    "text": first_seen
                                }
                            },
                            {
                                "decoratedText": {
                                    "topLabel": "Last Seen",
                                    "text": last_seen
                                }
                            },
                            {
                                "buttonList": {
                                    "buttons": [{
                                        "text": "View in Sentry",
                                        "onClick": {
                                            "openLink": {
                                                "url": issue_url
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
    } else {
        // Fallback for events without issue data
        json!({
            "text": format!("🔔 Sentry Alert: {}", action_text)
        })
    }
}

/// Sentry webhook handler - forwards alerts to Google Chat
pub async fn sentry_webhook_handler(
    State(_state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let signature = headers
        .get("sentry-hook-signature")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if !verify_sentry_signature(&body, signature, &SENTRY_CLIENT_SECRET) {
        log::warn!("Sentry webhook: invalid signature");
        return (StatusCode::UNAUTHORIZED, "Invalid signature");
    }

    let payload: SentryWebhookPayload = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            log::error!("Failed to parse Sentry webhook payload: {}", e);
            return (StatusCode::BAD_REQUEST, "Invalid payload");
        }
    };

    log::info!(
        "Received Sentry webhook: action={}, project={}",
        payload.action,
        payload
            .data
            .issue
            .as_ref()
            .map(|i| &i.project.name)
            .unwrap_or(&"unknown".to_string())
    );

    let gchat_message = build_gchat_message(&payload);

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
