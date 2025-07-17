use std::{fmt::Display, sync::Arc};

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
    utils::grpc_clients::ml_feed::{
        ml_feed_client::MlFeedClient, VideoReportRequestV3,
    },
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
        write!(f, "{:?}", self)
    }
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
pub struct ReportPostRequest {
    #[schema(value_type = String)]
    pub canister_id: Principal,
    pub post_id: u64,
    pub video_id: String,
    #[schema(value_type = String)]
    pub user_canister_id: Principal,
    #[schema(value_type = String)]
    pub user_principal: Principal,
    pub reason: String,
}

#[instrument(skip(state, verified_request))]
#[utoipa::path(
    post,
    path = "/report",
    request_body = PostRequest<ReportPostRequest>,
    tag = "posts",
    responses(
        (status = 200, description = "Report post success"),
        (status = 500, description = "Internal server error"),
    )
)]
#[deprecated = "Check /report_v2 instead"]
pub async fn handle_report_post(
    State(state): State<Arc<AppState>>,
    Json(verified_request): Json<VerifiedPostRequest<ReportPostRequest>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let request_body = verified_request.request.request_body;

    repost_post_common_impl(state, request_body.into())
        .await
        .map_err(|e| {
            log::error!("Failed to report post: {}", e);

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to report post: {}", e),
            )
        })?;

    Ok((StatusCode::OK, "Post reported".to_string()))
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

impl From<ReportPostRequest> for ReportPostRequestV2 {
    fn from(request: ReportPostRequest) -> Self {
        Self {
            publisher_principal: Principal::anonymous(),
            canister_id: request.canister_id,
            post_id: request.post_id,
            video_id: request.video_id,
            user_canister_id: request.user_canister_id,
            user_principal: request.user_principal,
            reason: request.reason,
            report_mode: ReportMode::default(),
        }
    }
}

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

    repost_post_common_impl(state, request_body)
        .await
        .map_err(|e| {
            log::error!("Failed to report post: {}", e);

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to report post: {}", e),
            )
        })?;

    Ok((StatusCode::OK, "Post reported".to_string()))
}

pub async fn qstash_report_post(
    State(_state): State<Arc<AppState>>,
    Json(payload): Json<ReportPostRequestV2>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let tls_config = ClientTlsConfig::new().with_webpki_roots();

    let channel = Channel::from_static(ML_FEED_SERVER_GRPC_URL)
        .tls_config(tls_config)
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create channel: {}", e),
            )
        })?
        .connect()
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to connect to ML feed server: {}", e),
            )
        })?;

    let mut client = MlFeedClient::new(channel);

    let request = VideoReportRequestV3 {
        reportee_user_id: payload.user_principal.to_string(),
        video_id: payload.video_id,
        reason: payload.reason,
    };

    client.report_video_v3(request).await.map_err(|e| {
        log::error!("Failed to report video: {}", e);

        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to report video: {}", e),
        )
    })?;

    Ok((StatusCode::OK, "Report post success".to_string()))
}

pub async fn repost_post_common_impl(
    state: Arc<AppState>,
    payload: ReportPostRequestV2,
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

    let res = send_message_gchat(GOOGLE_CHAT_REPORT_SPACE_URL, data).await;
    if res.is_err() {
        log::error!("Error sending data to Google Chat: {:?}", res);
    }

    let qstash_client = state.qstash_client.clone();
    qstash_client.publish_report_post(payload).await?;

    Ok(())
}
