#![allow(unused_imports)]
#![allow(unused_variables)]

use std::sync::Arc;
use crate::posts::queries::get_duplicate_children_query;

#[cfg(not(feature = "local-bin"))]
use super::types::UserPost;
#[cfg(not(feature = "local-bin"))]
use super::types::VideoDeleteRow;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
#[cfg(not(feature = "local-bin"))]
use chrono::Utc;
#[cfg(not(feature = "local-bin"))]
use google_cloud_bigquery::{
    client::Client,
    http::{
        job::query::QueryRequest,
        tabledata::insert_all::{InsertAllRequest, Row},
    },
    query::row::Row as QueryRow,
};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use types::PostRequest;
use verify::VerifiedPostRequest;
use yral_canisters_client::individual_user_template::{IndividualUserTemplate, Result_};

use crate::{
    app_state::AppState,
    user::utils::get_agent_from_delegated_identity_wire,
};

use super::{types, verify, DeletePostRequest};

#[utoipa::path(
    delete,
    path = "",
    request_body = PostRequest<DeletePostRequest>,
    tag = "posts",
    responses(
        (status = 200, description = "Delete post success"),
        (status = 400, description = "Delete post failed"),
        (status = 500, description = "Internal server error"),
        (status = 403, description = "Forbidden"),
    )
)]
#[instrument(skip(state, verified_request))]
#[allow(unused_variables)]
pub async fn handle_delete_post(
    State(state): State<Arc<AppState>>,
    Json(verified_request): Json<VerifiedPostRequest<DeletePostRequest>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Verify that the canister ID matches the user's canister
    if verified_request.request.request_body.canister_id != verified_request.user_canister {
        return Err((StatusCode::FORBIDDEN, "Forbidden".to_string()));
    }

    let request_body = verified_request.request.request_body;

    #[allow(unused_variables)]
    let canister_id = request_body.canister_id.to_string();
    let post_id = request_body.post_id;
    #[allow(unused_variables)]
    let video_id = request_body.video_id;

    let agent =
        get_agent_from_delegated_identity_wire(&verified_request.request.delegated_identity_wire)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let individual_user_template = IndividualUserTemplate(verified_request.user_canister, &agent);

    // Call the canister to delete the post
    let delete_res = individual_user_template.delete_post(post_id).await;
    match delete_res {
        Ok(Result_::Ok) => (),
        Ok(Result_::Err(_)) => {
            return Err((
                StatusCode::BAD_REQUEST,
                "Delete post failed - either the post doesn't exist or already deleted".to_string(),
            ))
        }
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Internal server error: {e}"),
            ))
        }
    }

    #[cfg(not(feature = "local-bin"))]
    {
        insert_video_delete_row_to_bigquery(state.clone(), canister_id, post_id, video_id.clone())
            .await
            .map_err(|e| {
                log::error!("Failed to insert video delete row to bigquery: {e}");

                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to insert video to bigquery: {e}"),
                )
            })?;
    }

    // spawn to not block the request since as far as user is concerned, the post is deleted
    #[cfg(not(feature = "local-bin"))]
    {
        let bigquery_client = state.bigquery_client.clone();
        let video_id_clone = video_id.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_duplicate_post_on_delete(bigquery_client, video_id_clone).await {
                log::error!("Failed to handle duplicate post on delete: {e}");
            }
        });
    }

    Ok((StatusCode::OK, "Post deleted".to_string()))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VideoUniqueRow {
    pub video_id: String,
    pub videohash: String,
    pub created_at: String,
}

#[instrument(skip(bq_client))]
#[cfg(not(feature = "local-bin"))]
pub async fn handle_duplicate_post_on_delete(
    bq_client: Client,
    video_id: String,
) -> Result<(), anyhow::Error> {
    // check if its unique
    let request = QueryRequest {
        query: format!(
            "SELECT * FROM `hot-or-not-feed-intelligence.yral_ds.video_unique` WHERE video_id = '{}'",
            video_id.clone()
        ),
        ..Default::default()
    };
    let mut response = bq_client
        .query::<QueryRow>("hot-or-not-feed-intelligence", request)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to query video_unique: {}", e))?;
    let mut res_list = Vec::new();
    while let Some(row) = response.next().await? {
        res_list.push(row);
    }
    // if its not unique, return
    if res_list.is_empty() {
        return Ok(());
    }

    let first_row = &res_list[0];
    const VIDEOHASH_COLUMN_INDEX: usize = 1; // Example: Replace with correct index for videohash

    let videohash: String = first_row.column(VIDEOHASH_COLUMN_INDEX).map_err(|e| {
        anyhow::anyhow!(
            "Failed to retrieve 'videohash' at index {}: {}",
            VIDEOHASH_COLUMN_INDEX,
            e
        )
    })?;

    // get children from videohash_original GROUP BY and filter from video_deleted table
    let request = QueryRequest {
        query: get_duplicate_children_query(videohash.clone(), video_id.clone()),
        ..Default::default()
    };
    let mut response = bq_client
        .query::<QueryRow>("hot-or-not-feed-intelligence", request)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to query videohash_original: {}", e))?;
    let mut res_list = Vec::new();
    while let Some(row) = response.next().await? {
        res_list.push(row);
    }

    let mut duplicate_videos = Vec::new();
    for row in res_list {
        duplicate_videos.push(
            row.column::<String>(0)
                .map_err(|e| anyhow::anyhow!("Failed to retrieve 'video_id' at index 0: {}", e))?,
        );
    }

    if !duplicate_videos.is_empty() {
        // add one of the children to video_unique table
        let new_parent_video_id = duplicate_videos[0].clone();
        let video_unique_row = VideoUniqueRow {
            video_id: new_parent_video_id,
            videohash,
            created_at: Utc::now().to_rfc3339(),
        };

        let request = InsertAllRequest {
            rows: vec![Row {
                insert_id: None,
                json: video_unique_row,
            }],
            ..Default::default()
        };

        let res = bq_client
            .tabledata()
            .insert(
                "hot-or-not-feed-intelligence",
                "yral_ds",
                "video_unique",
                &request,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to insert video unique row to bigquery: {}", e))?;

        if let Some(errors) = res.insert_errors {
            if !errors.is_empty() {
                log::error!("video_unique insert response : {errors:?}");
                return Err(anyhow::anyhow!(
                    "Failed to insert video unique row to bigquery"
                ));
            }
        }
    }

    // delete old parent from video_unique table
    let request = QueryRequest {
        query: format!(
            "DELETE FROM `hot-or-not-feed-intelligence.yral_ds.video_unique` WHERE video_id = '{video_id}'"
        ),
        ..Default::default()
    };

    let res = bq_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to delete video unique row to bigquery: {}", e))?;

    if let Some(errors) = res.errors {
        if !errors.is_empty() {
            log::error!("video_unique delete response : {errors:?}");
            return Err(anyhow::anyhow!(
                "Failed to delete video unique row to bigquery"
            ));
        }
    }
    Ok(())
}

#[allow(dead_code)]
#[cfg(feature = "local-bin")]
pub async fn insert_video_delete_row_to_bigquery(
    _state: Arc<AppState>,
    _canister_id: String,
    _post_id: u64,
    _video_id: String,
) -> Result<(), anyhow::Error> {
    Ok(())
}

#[cfg(not(feature = "local-bin"))]
pub async fn insert_video_delete_row_to_bigquery(
    state: Arc<AppState>,
    canister_id: String,
    post_id: u64,
    video_id: String,
) -> Result<(), anyhow::Error> {
    bulk_insert_video_delete_rows(
        &state.bigquery_client,
        vec![UserPost {
            canister_id,
            post_id,
            video_id,
        }],
    )
    .await?;

    Ok(())
}

#[cfg(not(feature = "local-bin"))]
pub async fn bulk_insert_video_delete_rows(
    bq_client: &Client,
    posts: Vec<UserPost>,
) -> Result<(), anyhow::Error> {
    // Process posts in batches of 500
    for chunk in posts.chunks(500) {
        let rows: Vec<Row<VideoDeleteRow>> = chunk
            .iter()
            .map(|post| {
                let video_delete_row = VideoDeleteRow {
                    canister_id: post.canister_id.clone(),
                    post_id: post.post_id,
                    video_id: post.video_id.clone(),
                    gcs_video_id: format!("gs://yral-videos/{}.mp4", post.video_id),
                };
                Row::<VideoDeleteRow> {
                    insert_id: None,
                    json: video_delete_row,
                }
            })
            .collect();

        let request = InsertAllRequest {
            rows,
            ..Default::default()
        };

        let res = bq_client
            .tabledata()
            .insert(
                "hot-or-not-feed-intelligence",
                "yral_ds",
                "video_deleted",
                &request,
            )
            .await?;

        if let Some(errors) = res.insert_errors {
            if !errors.is_empty() {
                log::error!("video_deleted bulk insert errors: {errors:?}");
                return Err(anyhow::anyhow!(
                    "Failed to bulk insert video deleted rows to bigquery"
                ));
            }
        }
    }

    Ok(())
}
