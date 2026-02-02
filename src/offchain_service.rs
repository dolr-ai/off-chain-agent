use std::{collections::HashMap, sync::Arc};

use crate::{
    app_state::AppState,
    consts::GOOGLE_CHAT_REPORT_SPACE_URL,
    events::{event::Event, warehouse_events::WarehouseEvent},
    posts::report_post::repost_post_common_impl,
    AppError,
};
use anyhow::{Context, Result};
use axum::extract::State;
use candid::Principal;
use http::HeaderMap;
use jsonwebtoken::DecodingKey;
use reqwest::Client;
use serde_json::{json, Value};
use yral_canisters_client::{
    ic::USER_POST_SERVICE_ID,
    user_post_service::{Post, PostStatus, Result2, UserPostService},
};

use crate::offchain_service::off_chain::{Empty, ReportPostRequest};
use off_chain::off_chain_server::OffChain;

pub mod off_chain {
    tonic::include_proto!("off_chain");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("off_chain_descriptor");
}

pub struct OffChainService {
    pub shared_state: Arc<AppState>,
}

#[tonic::async_trait]
impl OffChain for OffChainService {
    async fn report_post(
        &self,
        request: tonic::Request<ReportPostRequest>,
    ) -> core::result::Result<tonic::Response<Empty>, tonic::Status> {
        let shared_state = self.shared_state.clone();
        let request = request.into_inner();

        let user_principal = Principal::from_text(&request.reporter_id)
            .map_err(|_| tonic::Status::new(tonic::Code::Unknown, "Invalid reporter id"))?;
        let user_canister_id = shared_state
            .get_individual_canister_by_user_principal(user_principal)
            .await
            .map_err(|_| {
                tonic::Status::new(tonic::Code::Unknown, "Failed to get user canister id")
            })?;
        let publisher_canister_id =
            Principal::from_text(&request.publisher_canister_id).map_err(|_| {
                tonic::Status::new(tonic::Code::Unknown, "Invalid publisher canister id")
            })?;
        let publisher_principal = Principal::from_text(&request.publisher_id).map_err(|_| {
            tonic::Status::new(tonic::Code::Unknown, "Invalid publisher canister id")
        })?;
        let post_report_request = crate::posts::report_post::ReportPostRequestV3 {
            publisher_principal,
            report_mode: crate::posts::report_post::ReportMode::Web,
            canister_id: publisher_canister_id,
            post_id: request
                .post_id
                .parse::<String>()
                .map_err(|_| tonic::Status::new(tonic::Code::Unknown, "Invalid post id"))?,
            video_id: request.video_id,
            user_canister_id,
            user_principal,
            reason: request.reason,
        };

        repost_post_common_impl(shared_state.clone(), post_report_request)
            .await
            .map_err(|e| {
                log::error!("Failed to report post: {e}");
                tonic::Status::new(tonic::Code::Unknown, "Failed to report post")
            })?;

        Ok(tonic::Response::new(Empty {}))
    }
}

/// Send a message to Google Chat as the Chat App (with OAuth authentication)
/// This allows interactive buttons (like "Ban Post") to work
pub async fn send_message_gchat(
    app_state: &AppState,
    request_url: &str,
    data: Value,
) -> Result<()> {
    let client = Client::new();

    let token = app_state.get_gchat_access_token().await;

    let response = client
        .post(request_url)
        .header("Content-Type", "application/json")
        .bearer_auth(token)
        .json(&data)
        .send()
        .await
        .context("Failed to send request to Google Chat")?;

    let status = response.status();
    let body = response.text().await.unwrap_or_default();

    if !status.is_success() {
        log::error!("Google Chat API error: status={}, body={}", status, body);
        return Err(anyhow::anyhow!(
            "Google Chat API error: status={}, body={}",
            status,
            body
        ));
    }

    log::debug!("Google Chat response: {}", body);
    Ok(())
}

/// Send a message to Google Chat via webhook (no OAuth, but interactive buttons won't work)
/// Use this for simple notifications like Sentry alerts
pub async fn send_message_gchat_webhook(request_url: &str, data: Value) -> Result<()> {
    let client = Client::new();

    let response = client
        .post(request_url)
        .header("Content-Type", "application/json")
        .json(&data)
        .send()
        .await
        .context("Failed to send request to Google Chat")?;

    let status = response.status();
    let body = response.text().await.unwrap_or_default();

    if !status.is_success() {
        log::error!("Google Chat API error: status={}, body={}", status, body);
        return Err(anyhow::anyhow!(
            "Google Chat API error: status={}, body={}",
            status,
            body
        ));
    }

    log::debug!("Google Chat response: {}", body);
    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct GoogleJWT {
    #[allow(dead_code)]
    aud: String,
    #[allow(dead_code)]
    iss: String,
}

#[derive(Debug, serde::Deserialize)]
struct GChatPayload {
    #[serde(rename = "type", default)]
    payload_type: Option<String>,
    #[serde(rename = "eventTime", default)]
    #[allow(dead_code)]
    event_time: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    message: Option<serde_json::Value>,
    #[serde(default)]
    #[allow(dead_code)]
    space: Option<serde_json::Value>,
    #[serde(default)]
    #[allow(dead_code)]
    user: Option<serde_json::Value>,
    #[serde(default)]
    action: Option<GChatPayloadAction>,
    #[serde(default)]
    #[allow(dead_code)]
    common: Option<serde_json::Value>,
    // Apps Script specific fields
    #[serde(rename = "commonEventObject", default)]
    #[allow(dead_code)]
    common_event_object: Option<serde_json::Value>,
}

#[derive(Debug, serde::Deserialize)]
struct GChatPayloadAction {
    #[serde(rename = "actionMethodName")]
    action_method_name: String,
    parameters: Vec<GChatPayloadActionParameter>,
}

#[derive(Debug, serde::Deserialize)]
struct GChatPayloadActionParameter {
    // key: String,
    value: String,
}

pub async fn report_approved_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: String,
) -> Result<axum::Json<Value>, AppError> {
    log::info!("report_approved_handler called");
    log::info!("report_approved_handler: headers: {:?}", headers);
    log::info!("report_approved_handler: body: {:?}", body);

    // Check if this is from Apps Script (no Authorization header means Apps Script proxy)
    let is_apps_script = headers.get("Authorization").is_none();

    if !is_apps_script {
        // authenticate the request for direct Google Chat calls

        let bearer = headers
            .get("Authorization")
            .context("Missing Authorization header")?;
        log::info!("Authorization header found");

        let bearer_str = bearer
            .to_str()
            .context("Failed to parse Authorization header")?;
        let auth_token = bearer_str
            .split("Bearer ")
            .last()
            .context("Failed to parse Bearer token")?;
        log::info!("Auth token extracted");

        // get PUBLIC_CERTS from GET https://www.googleapis.com/service_accounts/v1/metadata/x509/chat@system.gserviceaccount.com

        let client = reqwest::Client::new();
        let res = client
            .get("https://www.googleapis.com/service_accounts/v1/metadata/x509/chat@system.gserviceaccount.com")
            .send()
            .await?;
        let res_body = res.text().await?;
        log::info!("Fetched Google Chat public certs");

        let certs: HashMap<String, String> = serde_json::from_str(&res_body)?;

        // verify the JWT using jsonwebtoken crate

        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
        validation.set_issuer(&["chat@system.gserviceaccount.com"]);
        validation.set_audience(&["1035262663512"]);

        let mut valid = false;

        for v in certs.values() {
            let jwt = jsonwebtoken::decode::<GoogleJWT>(
                auth_token,
                &DecodingKey::from_rsa_pem(v.as_bytes())?,
                &validation,
            );

            if jwt.is_ok() {
                valid = true;
                break;
            }
        }
        if !valid {
            log::error!("Invalid JWT token from Google Chat");
            return Err(anyhow::anyhow!("Invalid JWT").into());
        }
        log::info!("JWT validation successful");
    } else {
        log::info!("Request from Apps Script proxy - skipping JWT validation");
    }

    // Get the data from the body
    let payload: GChatPayload = serde_json::from_str(&body)?;
    log::info!("Parsed GChat payload: type={:?}", payload.payload_type);

    // Extract viewType parameter - handle both Apps Script and direct Google Chat formats
    let view_type = if let Some(common_event_obj) = &payload.common_event_object {
        // Apps Script format: commonEventObject.parameters.viewType
        log::info!("Using Apps Script event format");
        common_event_obj
            .get("parameters")
            .and_then(|p| p.get("viewType"))
            .and_then(|v| v.as_str())
            .context("No viewType parameter in commonEventObject")?
            .to_string()
    } else if let Some(action) = &payload.action {
        // Direct Google Chat format: action.parameters[0].value
        log::info!("Using direct Google Chat event format");
        log::info!("Action method: {}", action.action_method_name);
        action
            .parameters
            .first()
            .context("No parameters in action")?
            .value
            .clone()
    } else {
        return Err(anyhow::anyhow!("No action or commonEventObject in payload").into());
    };

    log::info!("View type: {}", view_type);

    // view_type format : "canister_id post_id(int)"
    let view_type: Vec<&str> = view_type.split(" ").collect();
    let canister_id = view_type[0];
    let post_id = view_type[1];
    log::info!(
        "Banning post: canister_id={}, post_id={}",
        canister_id,
        post_id
    );

    let user_post_service = UserPostService(USER_POST_SERVICE_ID, &state.agent);

    let Result2::Ok(Post {
        video_uid,
        creator_principal,
        ..
    }) = user_post_service
        .get_individual_post_details_by_id(post_id.to_string())
        .await?
    else {
        return Err(anyhow::anyhow!(
            "Failed to fetch post details for {}/{}",
            canister_id,
            post_id
        )
        .into());
    };

    user_post_service
        .update_post_status(post_id.to_string(), PostStatus::BannedDueToUserReporting)
        .await?;
    log::info!("Post status updated to banned");

    // send confirmation to Google Chat
    let confirmation_msg = json!({
        "text": format!("Successfully banned post : {}/{}", canister_id, post_id)
    });
    send_message_gchat(&state, &GOOGLE_CHAT_REPORT_SPACE_URL, confirmation_msg).await?;
    log::info!("Sent confirmation message to Google Chat");

    let params = json!({
        "video_id": video_uid,
        "publisher_user_id": creator_principal,
        "canister_id": canister_id,
        "post_id": post_id
    });

    let event = Event::new(WarehouseEvent {
        event: "video_report_banned".to_string(),
        params: params.to_string(),
    });
    event.stream_to_bigquery(&state);
    log::info!("Ban event sent to BigQuery");

    log::info!("report_approved_handler completed successfully");

    // Return a simple message response
    Ok(axum::Json(json!({
        "text": format!("✅ Post {}/{} has been banned successfully", canister_id, post_id)
    })))
}
