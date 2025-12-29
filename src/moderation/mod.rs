use std::sync::Arc;

use axum::{
    extract::{Path, Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use candid::Principal;
use google_cloud_bigquery::http::job::query::QueryRequest;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::instrument;
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::kvrocks::KvrocksClient;
use crate::{
    app_state::AppState, consts::MODERATOR_PRINCIPALS, events::push_notifications::dispatch_notif,
    types::DelegatedIdentityWire,
    utils::delegated_identity::get_user_info_from_delegated_identity_wire, AppError,
};

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone)]
pub struct ModerationRequest {
    pub delegated_identity_wire: DelegatedIdentityWire,
}

#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct ModerationResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct PendingVideo {
    pub video_id: String,
    pub post_id: Option<String>,
    pub canister_id: Option<String>,
    pub user_id: Option<String>,
    pub created_at: Option<String>,
}

#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct PendingVideosResponse {
    pub videos: Vec<PendingVideo>,
    pub total_count: usize,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Default, Clone)]
pub struct PendingVideosQuery {
    /// Maximum number of videos to return (default: 100)
    pub limit: Option<u32>,
    /// Offset for pagination (default: 0)
    pub offset: Option<u32>,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone)]
pub struct PendingVideosRequest {
    pub delegated_identity_wire: DelegatedIdentityWire,
    #[serde(flatten)]
    pub query: PendingVideosQuery,
}

/// Check if a principal is a whitelisted moderator
fn is_moderator(principal: &Principal) -> bool {
    MODERATOR_PRINCIPALS.contains(principal)
}

/// Middleware to verify moderator access
pub async fn verify_moderator(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };

    let moderation_request: ModerationRequest = match serde_json::from_slice(&bytes) {
        Ok(req) => req,
        Err(_) => return Err(StatusCode::BAD_REQUEST),
    };

    let user_info = get_user_info_from_delegated_identity_wire(
        &state,
        moderation_request.delegated_identity_wire,
    )
    .await
    .map_err(|_| StatusCode::UNAUTHORIZED)?;

    if !is_moderator(&user_info.user_principal) {
        log::warn!(
            "Unauthorized moderation attempt by principal: {}",
            user_info.user_principal
        );
        return Err(StatusCode::FORBIDDEN);
    }

    log::info!(
        "Moderator access granted for principal: {}",
        user_info.user_principal
    );

    let request = Request::from_parts(parts, axum::body::Body::from(bytes));
    Ok(next.run(request).await)
}

#[instrument(skip(state))]
pub fn moderation_router(state: Arc<AppState>) -> OpenApiRouter {
    use axum::middleware;

    OpenApiRouter::new()
        .routes(routes!(get_pending_videos))
        .routes(routes!(approve_video))
        .routes(routes!(disapprove_video))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            verify_moderator,
        ))
        .with_state(state)
}

/// Get list of videos pending approval (is_approved = false)
#[utoipa::path(
    post,
    path = "/pending",
    request_body = PendingVideosRequest,
    tag = "moderation",
    responses(
        (status = 200, description = "List of pending videos", body = PendingVideosResponse),
        (status = 401, description = "Unauthorized - invalid delegated identity"),
        (status = 403, description = "Forbidden - not a moderator"),
        (status = 500, description = "Internal server error"),
    )
)]
#[instrument(skip(state))]
pub async fn get_pending_videos(
    State(state): State<Arc<AppState>>,
    Json(request): Json<PendingVideosRequest>,
) -> Result<impl IntoResponse, AppError> {
    let limit = request.query.limit.unwrap_or(100);
    let offset = request.query.offset.unwrap_or(0);

    let videos = fetch_pending_videos(&state.bigquery_client, limit, offset).await?;
    let total_count = videos.len();

    Ok((
        StatusCode::OK,
        Json(PendingVideosResponse {
            videos,
            total_count,
        }),
    ))
}

/// Approve a video by its video ID (sets is_approved = true)
#[utoipa::path(
    post,
    path = "/approve/{video_id}",
    request_body = ModerationRequest,
    params(
        ("video_id" = String, Path, description = "The video ID to approve")
    ),
    tag = "moderation",
    responses(
        (status = 200, description = "Video approved successfully", body = ModerationResponse),
        (status = 401, description = "Unauthorized - invalid delegated identity"),
        (status = 403, description = "Forbidden - not a moderator"),
        (status = 404, description = "Video not found"),
        (status = 500, description = "Internal server error"),
    )
)]
#[instrument(skip(state))]
pub async fn approve_video(
    Path(video_id): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(_request): Json<ModerationRequest>,
) -> Result<impl IntoResponse, AppError> {
    // First fetch the video info before updating
    let video_info = fetch_video_info(&state.bigquery_client, &video_id).await?;

    let updated =
        update_approval_status(&state.bigquery_client, &state.kvrocks_client, &video_id).await?;
    if updated {
        // Send notification to the video owner via event pipeline
        if let Some(info) = video_info {
            send_approval_notification(&state, &info, true).await;
        }

        Ok((
            StatusCode::OK,
            Json(ModerationResponse {
                success: true,
                message: format!("Successfully approved video {}", video_id),
            }),
        ))
    } else {
        Ok((
            StatusCode::NOT_FOUND,
            Json(ModerationResponse {
                success: false,
                message: format!("Video {} not found", video_id),
            }),
        ))
    }
}

/// Disapprove a video by its video ID (deletes entry from the table)
#[utoipa::path(
    post,
    path = "/disapprove/{video_id}",
    request_body = ModerationRequest,
    params(
        ("video_id" = String, Path, description = "The video ID to disapprove")
    ),
    tag = "moderation",
    responses(
        (status = 200, description = "Video disapproved successfully", body = ModerationResponse),
        (status = 401, description = "Unauthorized - invalid delegated identity"),
        (status = 403, description = "Forbidden - not a moderator"),
        (status = 404, description = "Video not found"),
        (status = 500, description = "Internal server error"),
    )
)]
#[instrument(skip(state))]
pub async fn disapprove_video(
    Path(video_id): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(_request): Json<ModerationRequest>,
) -> Result<impl IntoResponse, AppError> {
    // First fetch the video info before deleting
    let video_info = fetch_video_info(&state.bigquery_client, &video_id).await?;

    let deleted = delete_video(&state.bigquery_client, &state.kvrocks_client, &video_id).await?;
    if deleted {
        // Send notification to the video owner via event pipeline
        if let Some(info) = video_info {
            send_approval_notification(&state, &info, false).await;
        }

        Ok((
            StatusCode::OK,
            Json(ModerationResponse {
                success: true,
                message: format!("Successfully disapproved video {}", video_id),
            }),
        ))
    } else {
        Ok((
            StatusCode::NOT_FOUND,
            Json(ModerationResponse {
                success: false,
                message: format!("Video {} not found", video_id),
            }),
        ))
    }
}

#[instrument(skip(bigquery_client))]
async fn fetch_pending_videos(
    bigquery_client: &google_cloud_bigquery::client::Client,
    limit: u32,
    offset: u32,
) -> Result<Vec<PendingVideo>, anyhow::Error> {
    let query = format!(
        "SELECT video_id, post_id, canister_id, user_id, CAST(created_at AS STRING) as created_at
         FROM `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
         WHERE is_approved = FALSE
         ORDER BY created_at DESC
         LIMIT {} OFFSET {}",
        limit, offset
    );

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    let result = bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await?;

    let mut videos = Vec::new();
    if let Some(rows) = result.rows {
        for row in rows {
            let video_id = match &row.f[0].v {
                google_cloud_bigquery::http::tabledata::list::Value::String(s) => s.clone(),
                _ => continue,
            };

            let post_id = match &row.f[1].v {
                google_cloud_bigquery::http::tabledata::list::Value::String(s) => Some(s.clone()),
                _ => None,
            };

            let canister_id = match &row.f[2].v {
                google_cloud_bigquery::http::tabledata::list::Value::String(s) => Some(s.clone()),
                _ => None,
            };

            let user_id = match &row.f[3].v {
                google_cloud_bigquery::http::tabledata::list::Value::String(s) => Some(s.clone()),
                _ => None,
            };

            let created_at = match &row.f[4].v {
                google_cloud_bigquery::http::tabledata::list::Value::String(s) => Some(s.clone()),
                _ => None,
            };

            videos.push(PendingVideo {
                video_id,
                post_id,
                canister_id,
                user_id,
                created_at,
            });
        }
    }

    Ok(videos)
}

#[instrument(skip(bigquery_client, kvrocks_client))]
async fn update_approval_status(
    bigquery_client: &google_cloud_bigquery::client::Client,
    kvrocks_client: &KvrocksClient,
    video_id: &str,
) -> Result<bool, anyhow::Error> {
    // Escape video ID for SQL
    let escaped_video_id = video_id.replace('\'', "''");

    let query = format!(
        "UPDATE `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
         SET is_approved = TRUE
         WHERE video_id = '{}'",
        escaped_video_id
    );

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    let result = bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await?;

    // BigQuery returns num_dml_affected_rows for DML statements
    let affected = result.num_dml_affected_rows.unwrap_or(0) > 0;

    log::info!(
        "Updated approval status for video {}: {}",
        video_id,
        if affected { "success" } else { "not found" }
    );

    // Also update in kvrocks
    if affected {
        if let Err(e) = kvrocks_client
            .update_user_uploaded_content_approval_status(video_id, true)
            .await
        {
            log::error!("Error updating approval status in kvrocks: {}", e);
        }
    }

    Ok(affected)
}

#[instrument(skip(bigquery_client, kvrocks_client))]
async fn delete_video(
    bigquery_client: &google_cloud_bigquery::client::Client,
    kvrocks_client: &KvrocksClient,
    video_id: &str,
) -> Result<bool, anyhow::Error> {
    // Escape video ID for SQL
    let escaped_video_id = video_id.replace('\'', "''");

    let query = format!(
        "DELETE FROM `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
         WHERE video_id = '{}'",
        escaped_video_id
    );

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    let result = bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await?;

    // BigQuery returns num_dml_affected_rows for DML statements
    let deleted = result.num_dml_affected_rows.unwrap_or(0) > 0;

    log::info!(
        "Deleted video {} from ugc_content_approval: {}",
        video_id,
        if deleted { "success" } else { "not found" }
    );

    // Also delete from kvrocks
    if deleted {
        if let Err(e) = kvrocks_client
            .delete_user_uploaded_content_approval(video_id)
            .await
        {
            log::error!("Error deleting approval from kvrocks: {}", e);
        }
    }

    Ok(deleted)
}

/// Video info needed for sending notifications
#[derive(Debug, Clone)]
struct VideoInfo {
    video_id: String,
    post_id: Option<String>,
    canister_id: Option<String>,
    user_id: Option<String>,
}

#[instrument(skip(bigquery_client))]
async fn fetch_video_info(
    bigquery_client: &google_cloud_bigquery::client::Client,
    video_id: &str,
) -> Result<Option<VideoInfo>, anyhow::Error> {
    let escaped_video_id = video_id.replace('\'', "''");

    let query = format!(
        "SELECT video_id, post_id, canister_id, user_id
         FROM `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
         WHERE video_id = '{}'
         LIMIT 1",
        escaped_video_id
    );

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    let result = bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await?;

    if let Some(rows) = result.rows {
        if let Some(row) = rows.first() {
            let video_id = match &row.f[0].v {
                google_cloud_bigquery::http::tabledata::list::Value::String(s) => s.clone(),
                _ => return Ok(None),
            };

            let post_id = match &row.f[1].v {
                google_cloud_bigquery::http::tabledata::list::Value::String(s) => Some(s.clone()),
                _ => None,
            };

            let canister_id = match &row.f[2].v {
                google_cloud_bigquery::http::tabledata::list::Value::String(s) => Some(s.clone()),
                _ => None,
            };

            let user_id = match &row.f[3].v {
                google_cloud_bigquery::http::tabledata::list::Value::String(s) => Some(s.clone()),
                _ => None,
            };

            return Ok(Some(VideoInfo {
                video_id,
                post_id,
                canister_id,
                user_id,
            }));
        }
    }

    Ok(None)
}

#[instrument(skip(state))]
async fn send_approval_notification(state: &AppState, video_info: &VideoInfo, is_approved: bool) {
    let Some(user_id_str) = &video_info.user_id else {
        log::warn!(
            "Cannot send notification for video {}: missing user_id",
            video_info.video_id
        );
        return;
    };

    let user_principal = match Principal::from_text(user_id_str) {
        Ok(p) => p,
        Err(e) => {
            log::error!(
                "Failed to parse user_id {} as principal: {}",
                user_id_str,
                e
            );
            return;
        }
    };

    let event_type = if is_approved {
        "video_approved"
    } else {
        "video_disapproved"
    };

    let params = json!({
        "video_id": video_info.video_id,
        "post_id": video_info.post_id.clone().unwrap_or_default(),
        "canister_id": video_info.canister_id,
        "user_id": user_principal.to_text()
    });

    if let Err(e) = dispatch_notif(event_type, params, state).await {
        log::error!(
            "Failed to dispatch {} notification for video {}: {:?}",
            event_type,
            video_info.video_id,
            e
        );
    } else {
        log::info!(
            "Dispatched {} notification for video {} to user {}",
            event_type,
            video_info.video_id,
            user_principal
        );
    }
}
