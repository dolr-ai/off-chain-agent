use std::{fmt::Display, sync::Arc};

use crate::events::types::string_or_number;
use axum::{extract::State, response::IntoResponse, Json};
use candid::Principal;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tonic::transport::{Channel, ClientTlsConfig};
use tracing::instrument;
use utoipa::ToSchema;

use crate::{
    app_state::AppState,
    consts::{GOOGLE_CHAT_REPORT_SPACE_URL, ML_FEED_SERVER_GRPC_URL},
    offchain_service::send_message_gchat,
    utils::grpc_clients::ml_feed::{ml_feed_client::MlFeedClient, VideoReportRequestV3},
};

use super::{types::PostRequest, verify::VerifiedPostRequest};

#[derive(Debug, Default, Serialize, Deserialize, Clone, ToSchema)]
pub enum ReportMode {
    Web,
    #[default]
    Ios,
    Android,
}

impl Display for ReportMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
pub struct ReportPostRequestV2 {
    #[schema(value_type = String)]
    pub publisher_principal: Principal,
    #[schema(value_type = String)]
    pub canister_id: Principal,
    pub post_id: u64,
    pub video_id: String,
    #[schema(value_type = String)]
    pub user_canister_id: Principal,
    #[schema(value_type = String)]
    pub user_principal: Principal,
    pub reason: String,
    pub report_mode: ReportMode,
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
pub struct ReportPostRequestV3 {
    #[schema(value_type = String)]
    pub publisher_principal: Principal,
    #[schema(value_type = String)]
    pub canister_id: Principal,
    #[serde(deserialize_with = "string_or_number")]
    pub post_id: String, // Changed from u64 to String
    pub video_id: String,
    #[schema(value_type = String)]
    pub user_canister_id: Principal,
    #[schema(value_type = String)]
    pub user_principal: Principal,
    pub reason: String,
    pub report_mode: ReportMode,
}

impl From<ReportPostRequestV3> for ReportPostRequestV2 {
    fn from(request: ReportPostRequestV3) -> Self {
        Self {
            publisher_principal: request.publisher_principal,
            canister_id: request.canister_id,
            post_id: request.post_id.parse().unwrap_or(0), // Convert String to u64
            video_id: request.video_id,
            user_canister_id: request.user_canister_id,
            user_principal: request.user_principal,
            reason: request.reason,
            report_mode: request.report_mode,
        }
    }
}

impl From<ReportPostRequestV2> for ReportPostRequestV3 {
    fn from(request: ReportPostRequestV2) -> Self {
        Self {
            publisher_principal: request.publisher_principal,
            canister_id: request.canister_id,
            post_id: request.post_id.to_string(), // Convert u64 to String
            video_id: request.video_id,
            user_canister_id: request.user_canister_id,
            user_principal: request.user_principal,
            reason: request.reason,
            report_mode: request.report_mode,
        }
    }
}

// TODO: canister_id still being used
#[instrument(skip(state, verified_request))]
#[utoipa::path(
    post,
    path = "/report_v2",
    request_body = PostRequest<ReportPostRequestV2>,
    tag = "posts",
    responses(
        (status = 200, description = "Report post success"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn handle_report_post_v2(
    State(state): State<Arc<AppState>>,
    Json(verified_request): Json<VerifiedPostRequest<ReportPostRequestV2>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let request_body = verified_request.request.request_body;

    repost_post_common_impl(state, request_body.into())
        .await
        .map_err(|e| {
            log::error!("Failed to report post: {e}");

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to report post: {e}"),
            )
        })?;

    Ok((StatusCode::OK, "Post reported".to_string()))
}

#[instrument(skip(state, verified_request))]
#[utoipa::path(
    post,
    path = "/report",
    request_body = PostRequest<ReportPostRequestV3>,
    tag = "posts",
    responses(
        (status = 200, description = "Report post success"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn handle_report_post_v3(
    State(state): State<Arc<AppState>>,
    Json(verified_request): Json<VerifiedPostRequest<ReportPostRequestV3>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let request_body = verified_request.request.request_body;

    repost_post_common_impl(state, request_body)
        .await
        .map_err(|e| {
            log::error!("Failed to report post: {e}");

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to report post: {e}"),
            )
        })?;

    Ok((StatusCode::OK, "Post reported".to_string()))
}

pub async fn qstash_report_post(
    State(_state): State<Arc<AppState>>,
    Json(payload): Json<ReportPostRequestV3>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tls_config = ClientTlsConfig::new().with_webpki_roots();

    let channel = Channel::from_static(ML_FEED_SERVER_GRPC_URL)
        .tls_config(tls_config)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create channel: {e}"),
            )
        })?
        .connect()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to connect to ML feed server: {e}"),
            )
        })?;

    let mut client = MlFeedClient::new(channel);

    let request = VideoReportRequestV3 {
        reportee_user_id: payload.user_principal.to_string(),
        video_id: payload.video_id,
        reason: payload.reason,
    };

    client.report_video_v3(request).await.map_err(|e| {
        log::error!("Failed to report video: {e}");

        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to report video: {e}"),
        )
    })?;

    Ok((StatusCode::OK, "Report post success".to_string()))
}

/// Test endpoint for Google Chat report functionality
/// Returns detailed diagnostic information including errors
#[utoipa::path(
    get,
    path = "/test-gchat-report",
    tag = "posts",
    params(
        ("video_id" = Option<String>, Query, description = "Optional video ID to test with (defaults to test-video-123)")
    ),
    responses(
        (status = 200, description = "Test results with diagnostics"),
        (status = 500, description = "Test failed with error details"),
    )
)]
pub async fn test_gchat_report(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Use test data
    let test_canister_id = "4c4fd-caaaa-aaaaq-aaa3a-cai";
    let test_post_id = "123";
    let test_video_id = "test-video-456";
    let test_publisher = "aaaaa-aa";
    let test_reporter = "bbbbb-bb";

    let video_url = format!(
        "https://yral.com/hot-or-not/{}/{}",
        test_canister_id, test_post_id
    );

    let text_str = format!(
        "reporter_id: {} \n publisher_id: {} \n publisher_canister_id: {} \n post_id: {} \n video_id: {} \n reason: {} \n video_url: {} \n report_mode: {}",
        test_reporter, test_publisher, test_canister_id, test_post_id, test_video_id, "TEST REPORT", video_url, "Web"
    );

    let data = json!({
        "cardsV2": [
        {
            "cardId": "unique-card-id-test",
            "card": {
                "sections": [
                {
                    "header": "Report Post (TEST)",
                    "widgets": [
                    {
                        "textParagraph": {
                            "text": text_str
                        }
                    },
                    {
                        "buttonList": {
                            "buttons": [
                                {
                                "text": "View video",
                                "onClick": {
                                    "openLink": {
                                    "url": video_url
                                    }
                                }
                                },
                                {
                                "text": "Ban Post",
                                "onClick": {
                                    "action": {
                                    "function": "goToView",
                                    "parameters": [
                                        {
                                        "key": "viewType",
                                        "value": format!("{} {}", test_canister_id, test_post_id),
                                        }
                                    ]
                                    }
                                }
                                }
                            ]
                        }
                    }
                    ]
                }
                ]
            }
        }
        ]
    });

    // Check if URL is set
    let gchat_url = GOOGLE_CHAT_REPORT_SPACE_URL.clone();
    let url_status = if gchat_url.is_empty() {
        "ERROR: GOOGLE_CHAT_REPORT_SPACE_URL is empty"
    } else if !gchat_url.starts_with("http") {
        "ERROR: GOOGLE_CHAT_REPORT_SPACE_URL does not start with http"
    } else {
        "OK"
    };

    // Try to send
    let send_result = send_message_gchat(&state, &gchat_url, data.clone()).await;

    // Build diagnostic response
    let diagnostics = json!({
        "test_info": {
            "endpoint": "/test-gchat-report",
            "description": "Test endpoint for Google Chat report webhook"
        },
        "environment": {
            "gchat_url": gchat_url,
            "url_status": url_status,
            "url_length": gchat_url.len(),
        },
        "test_data": {
            "canister_id": test_canister_id,
            "post_id": test_post_id,
            "video_id": test_video_id,
            "video_url": video_url,
        },
        "request_payload": data,
        "result": {
            "success": send_result.is_ok(),
            "error": send_result.as_ref().err().map(|e| format!("{:#?}", e)),
            "error_display": send_result.as_ref().err().map(|e| e.to_string()),
        }
    });

    if send_result.is_ok() {
        Ok((StatusCode::OK, Json(diagnostics)))
    } else {
        Ok((StatusCode::INTERNAL_SERVER_ERROR, Json(diagnostics)))
    }
}

pub async fn repost_post_common_impl(
    state: Arc<AppState>,
    payload: ReportPostRequestV3,
) -> anyhow::Result<()> {
    let video_url = format!(
        "https://yral.com/hot-or-not/{}/{}",
        payload.canister_id, payload.post_id
    );

    let text_str = format!(
        "reporter_id: {} \n publisher_id: {} \n publisher_canister_id: {} \n post_id: {} \n video_id: {} \n reason: {} \n video_url: {} \n report_mode: {}",
        payload.user_principal, payload.publisher_principal, payload.canister_id, payload.post_id, payload.video_id, payload.reason, video_url, payload.report_mode
    );

    let data = json!({
        "cardsV2": [
        {
            "cardId": "unique-card-id",
            "card": {
                "sections": [
                {
                    "header": "Report Post",
                    "widgets": [
                    {
                        "textParagraph": {
                            "text": text_str
                        }
                    },
                    {
                        "buttonList": {
                            "buttons": [
                                {
                                "text": "View video",
                                "onClick": {
                                    "openLink": {
                                    "url": video_url
                                    }
                                }
                                },
                                {
                                "text": "Ban Post",
                                "onClick": {
                                    "action": {
                                    "function": "goToView",
                                    "parameters": [
                                        {
                                        "key": "viewType",
                                        "value": format!("{} {}", payload.canister_id, payload.post_id),
                                        }
                                    ]
                                    }
                                }
                                }
                            ]
                        }
                    }
                    ]
                }
                ]
            }
        }
        ]
    });

    let res = send_message_gchat(&state, &GOOGLE_CHAT_REPORT_SPACE_URL, data).await;
    if res.is_err() {
        log::error!("Error sending data to Google Chat: {res:?}");
    }

    #[cfg(not(any(feature = "local-bin", feature = "use-local-agent")))]
    {
        let qstash_client = state.qstash_client.clone();
        qstash_client.publish_report_post(payload).await?;
    }

    Ok(())
}
