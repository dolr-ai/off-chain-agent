use axum::{extract::State, response::IntoResponse, Json};
#[cfg(not(feature = "local-bin"))]
use futures::StreamExt;
use http::StatusCode;
use ic_agent::Agent;
#[cfg(not(feature = "local-bin"))]
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
#[cfg(not(feature = "local-bin"))]
use tracing::instrument;

use crate::{
    app_state::AppState,
    types::RedisPool,
};
#[cfg(not(feature = "local-bin"))]
use crate::canister::snapshot::utils::{
    get_platform_orch_ids_list_for_backup, get_subnet_orch_ids_list_for_backup,
    get_user_canister_list_for_backup,
};

use super::CanisterData;
#[cfg(not(feature = "local-bin"))]
use super::CanisterType;
#[cfg(not(feature = "local-bin"))]
use super::snapshot_v2::backup_canister_impl;

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotAlertJobPayload {
    pub date_str: String,
}

#[instrument(skip(state))]
#[cfg(not(feature = "local-bin"))]
pub async fn snapshot_alert_job(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SnapshotAlertJobPayload>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let agent = state.agent.clone();
    let canister_backup_redis_pool = state.canister_backup_redis_pool.clone();

    let _ = tokio::spawn(async move {
        snapshot_alert_job_impl(&agent, &canister_backup_redis_pool, payload.date_str)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
    });

    Ok(StatusCode::OK)
}

#[cfg(feature = "local-bin")]
pub async fn snapshot_alert_job(
    State(_state): State<Arc<AppState>>,
    Json(_payload): Json<SnapshotAlertJobPayload>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    Ok(StatusCode::OK)
}

#[instrument(skip(agent))]
#[cfg(not(feature = "local-bin"))]
pub async fn snapshot_alert_job_impl(
    agent: &Agent,
    redis_pool: &RedisPool,
    date_str: String,
) -> Result<(), anyhow::Error> {
    log::info!("Starting snapshot alert job");

    // Get all expected canisters
    log::info!("Fetching canister lists...");
    let mut canisters_backups = Vec::new();

    let user_canisters_left_for_backup =
        get_user_canister_list_for_backup(agent, redis_pool, date_str.clone()).await?;
    for canister_id in user_canisters_left_for_backup {
        let canister_data = CanisterData {
            canister_id,
            canister_type: CanisterType::User,
        };
        canisters_backups.push((canister_data, date_str.clone()));
    }

    // Add subnet orchestrators
    let subnet_orch_ids =
        get_subnet_orch_ids_list_for_backup(agent, redis_pool, date_str.clone()).await?;
    for canister_id in subnet_orch_ids {
        let canister_data = CanisterData {
            canister_id,
            canister_type: CanisterType::SubnetOrch,
        };
        canisters_backups.push((canister_data, date_str.clone()));
    }

    let platform_orch_ids =
        get_platform_orch_ids_list_for_backup(agent, redis_pool, date_str.clone()).await?;
    for canister_id in platform_orch_ids {
        let canister_data = CanisterData {
            canister_id,
            canister_type: CanisterType::PlatformOrch,
        };
        canisters_backups.push((canister_data, date_str.clone()));
    }

    if canisters_backups.is_empty() {
        log::info!("Snapshot alert job finished: All canisters have recent backups.");
    } else {
        log::warn!(
            "Snapshot alert job finished: {} canisters require attention",
            canisters_backups.len()
        );
    }

    let canisters_retry_backup_results =
        retry_backup_canisters(agent, redis_pool, canisters_backups, date_str).await?;

    send_google_chat_alert(canisters_retry_backup_results).await?;

    Ok(())
}

#[cfg(not(feature = "local-bin"))]
pub async fn retry_backup_canisters(
    agent: &Agent,
    redis_pool: &RedisPool,
    canister_list: Vec<(CanisterData, String)>,
    date_str: String,
) -> Result<HashMap<String, Vec<(String, String)>>, anyhow::Error> {
    let mut results = HashMap::new();

    // Regex to match and remove canister IDs from error strings
    // Example canister ID: kyi4y-eiaaa-aaaal-agvsq-cai
    let canister_id_regex = Regex::new(r"[a-z0-9\-]+-cai")
        .map_err(|e| anyhow::anyhow!("Failed to compile regex: {}", e))?;

    let futures = canister_list
        .into_iter()
        .map(|(canister_data, old_date_str)| {
            let agent = agent.clone();
            let date_str = date_str.clone();
            async move {
                let canister_id = canister_data.canister_id.to_string();
                if let Err(e) =
                    backup_canister_impl(&agent, redis_pool, canister_data, date_str.clone()).await
                {
                    let err_str = e.to_string();
                    Err((err_str, canister_id, old_date_str))
                } else {
                    Ok(())
                }
            }
        });

    let results_vec = futures::stream::iter(futures)
        .buffer_unordered(30)
        .collect::<Vec<_>>()
        .await;

    for res_item in results_vec {
        match res_item {
            Ok(()) => {}
            Err((original_err_str, canister_id, old_date_str)) => {
                // Strip canister IDs from the error string to group similar errors
                let cleaned_err_str = canister_id_regex
                    .replace_all(&original_err_str, "")
                    .trim()
                    .to_string();

                let final_err_key = if cleaned_err_str.is_empty() && !original_err_str.is_empty() {
                    original_err_str.clone()
                } else {
                    cleaned_err_str
                };

                let err_vec = results.entry(final_err_key).or_insert_with(Vec::new);
                err_vec.push((canister_id, old_date_str));
            }
        }
    }

    Ok(results)
}

#[cfg(feature = "local-bin")]
pub async fn retry_backup_canisters(
    _agent: &Agent,
    _redis_pool: &RedisPool,
    _canister_list: Vec<(CanisterData, String)>,
    _date_str: String,
) -> Result<HashMap<String, Vec<(String, String)>>, anyhow::Error> {
    Ok(HashMap::new())
}

#[cfg(not(feature = "local-bin"))]
async fn send_google_chat_alert(
    canisters_retry_backup_results: HashMap<String, Vec<(String, String)>>,
) -> Result<(), anyhow::Error> {
    let google_webhook_url = env::var("CANISTER_BACKUP_ALERT_GOOGLE_CHAT_WEBHOOK_URL")
        .map_err(|_| anyhow::anyhow!("CANISTER_BACKUP_ALERT_GOOGLE_CHAT_WEBHOOK_URL not set"))?;
    let client = reqwest::Client::new();
    // Calculate total count from the new structure
    let total_attention_count: usize = canisters_retry_backup_results
        .values()
        .map(|v| v.len())
        .sum();

    if total_attention_count == 0 {
        let body = json!({
            "text": "✅ Snapshot Retry Job Finished: All previously failing canisters backed up successfully or no canisters needed retries."
        });
        let res = client.post(&google_webhook_url).json(&body).send().await?;
        if res.status().is_success() {
            log::info!("Snapshot Retry Job Finished: 'All clear' status sent to Google Chat");
        } else {
            log::error!(
                "Snapshot Retry Job Finished: Failed to send 'All clear' status to Google Chat: {}",
                res.status()
            );
        }
        return Ok(());
    }

    let max_chars_per_message = 10000; // Google Chat message character limit
    let mut messages_to_send = Vec::new();
    let mut current_message = format!(
        "🚨 Snapshot Retry Job Finished: *{total_attention_count}* canisters still require attention after retry!\n\n"
    );
    let mut current_char_count = current_message.len();

    // Sort errors for consistent output
    let mut sorted_errors: Vec<_> = canisters_retry_backup_results.into_iter().collect();
    sorted_errors.sort_by(|a, b| a.0.cmp(&b.0));

    for (error_str, mut canister_list) in sorted_errors {
        if canister_list.is_empty() {
            continue;
        }

        // Sort canisters within the error group by date string ("missing" will typically be sorted after dates)
        canister_list.sort_by(|a, b| a.1.cmp(&b.1));

        let section_header = format!("*Error: {error_str}*\n");
        let continuation_header = format!("*(Cont...) Error: {error_str}*\n");

        // Check if header fits in the current message
        if current_char_count + section_header.len() > max_chars_per_message {
            messages_to_send.push(current_message.clone());
            current_message = String::new(); // Start fresh
            current_char_count = 0;
        }

        // Add header (either to new or existing message)
        if current_message.is_empty() {
            current_message.push_str(&section_header);
            current_char_count = current_message.len();
        } else {
            // Ensure newline separation if not starting fresh
            if !current_message.ends_with('\n') {
                current_message.push('\n');
                current_char_count += 1;
            }
            if !current_message.ends_with("\n\n") {
                current_message.push('\n');
                current_char_count += 1;
            }
            current_message.push_str(&section_header);
            current_char_count += section_header.len();
        }

        for (canister_id, date_str) in canister_list {
            let item_line = format!("- {canister_id}    {date_str}\n");
            let chars_to_add = item_line.len();

            if current_char_count + chars_to_add > max_chars_per_message {
                messages_to_send.push(current_message.clone()); // Finalize previous chunk
                current_message = continuation_header.clone(); // Start new chunk with continuation header
                current_char_count = current_message.len();
            }

            // Add the item line
            current_message.push_str(&item_line);
            current_char_count += chars_to_add;
        }
        // Add a newline after the list section for better separation between error groups
        if !current_message.ends_with("\n\n") {
            current_message.push('\n');
            current_char_count += 1;
        }
    }

    // Add the final message chunk if it contains content
    if !current_message.trim().is_empty() {
        messages_to_send.push(current_message);
    }

    // Send all the messages
    log::info!(
        "Sending {} alert message chunk(s) to Google Chat...",
        messages_to_send.len()
    );
    for (i, msg) in messages_to_send.iter().enumerate() {
        // Basic check to avoid sending empty messages or just headers
        if msg.trim().is_empty() || msg.starts_with("*(Cont...) Error:") && msg.lines().count() <= 1
        {
            log::warn!(
                "Skipping empty or header-only message chunk {}/{}",
                i + 1,
                messages_to_send.len()
            );
            continue;
        }

        let body = json!({ "text": msg });
        match client.post(&google_webhook_url).json(&body).send().await {
            Ok(res) => {
                if res.status().is_success() {
                    log::info!(
                        "Successfully sent message chunk {}/{} to Google Chat",
                        i + 1,
                        messages_to_send.len()
                    );
                } else {
                    log::error!(
                        "Failed to send message chunk {}/{} to Google Chat: Status {} Body: {:?}",
                        i + 1,
                        messages_to_send.len(),
                        res.status(),
                        res.text()
                            .await
                            .unwrap_or_else(|_| "<Failed to read body>".to_string())
                    );
                    // Consider adding error handling strategy here (e.g., stop sending?)
                }
            }
            Err(e) => {
                log::error!(
                    "Error sending message chunk {}/{} to Google Chat: {} ; body: {}, msg: {}, google_webhook_url: {}",
                    i + 1,
                    messages_to_send.len(),
                    e,
                    body,
                    msg,
                    google_webhook_url
                );
                // Consider adding error handling strategy here
            }
        }

        // Add a small delay between messages if needed to avoid rate limiting
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    log::info!("Snapshot alert job finished: Alert processing complete.");
    Ok(())
}
