use std::sync::Arc;

use super::types::{UserPost, UserPostV2, VideoDeleteRow};
use anyhow::{anyhow, Context};
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use chrono::Utc;
use google_cloud_bigquery::{
    client::Client,
    http::{
        job::query::QueryRequest,
        tabledata::insert_all::{InsertAllRequest, Row},
    },
    query::row::Row as QueryRow,
};
use ic_agent::Agent;
use serde::{Deserialize, Serialize};
use tracing::instrument;
use types::PostRequest;
use verify::VerifiedPostRequest;
use yral_canisters_client::{
    dedup_index::DedupIndex,
    individual_user_template::{IndividualUserTemplate, Result_},
    user_post_service::UserPostService,
};

use crate::kvrocks::KvrocksClient;
use crate::{
    app_state::AppState,
    consts::{
        DEDUP_INDEX_CANISTER_ID, USER_INFO_SERVICE_CANISTER_ID, USER_POST_SERVICE_CANISTER_ID,
    },
    posts::queries::get_duplicate_children_query,
    user::utils::get_agent_from_delegated_identity_wire,
};

use super::{types, verify, DeletePostRequest, DeletePostRequestV2};

const BULK_INSERT_DELETE_BIGQUERY_BATCH_SIZE: usize = 500;

// TODO: canister_id still being used
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
pub async fn handle_delete_post(
    State(state): State<Arc<AppState>>,
    Json(verified_request): Json<VerifiedPostRequest<DeletePostRequest>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Verify that the canister ID matches the user's canister
    if verified_request.request.request_body.canister_id != verified_request.user_canister {
        return Err((StatusCode::FORBIDDEN, "Forbidden".to_string()));
    }

    let request_body = verified_request.request.request_body;

    let canister_id = request_body.canister_id.to_string();
    let post_id = request_body.post_id;
    let video_id = request_body.video_id;

    let user_ic_agent =
        get_agent_from_delegated_identity_wire(&verified_request.request.delegated_identity_wire)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let individual_user_template =
        IndividualUserTemplate(verified_request.user_canister, &user_ic_agent);

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

    insert_video_delete_row_to_bigquery(state.clone(), canister_id, post_id, video_id.clone())
        .await
        .map_err(|e| {
            log::error!("Failed to insert video delete row to bigquery: {e}");

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to insert video to bigquery: {e}"),
            )
        })?;

    // spawn to not block the request since as far as user is concerned, the post is deleted
    let bigquery_client = state.bigquery_client.clone();
    let video_id_clone = video_id.clone();
    tokio::spawn(async move {
        if let Err(e) =
            handle_duplicate_post_on_delete(&state.agent, bigquery_client, video_id_clone).await
        {
            log::error!("Failed to handle duplicate post on delete: {e}");
        }
    });

    Ok((StatusCode::OK, "Post deleted".to_string()))
}

#[utoipa::path(
    delete,
    path = "",
    request_body = PostRequest<DeletePostRequestV2>,
    tag = "posts",
    responses(
        (status = 200, description = "Delete post success"),
        (status = 400, description = "Delete post failed"),
        (status = 500, description = "Internal server error"),
        (status = 403, description = "Forbidden"),
    )
)]
#[instrument(skip(state, verified_request))]
pub async fn handle_delete_post_v2(
    State(state): State<Arc<AppState>>,
    Json(verified_request): Json<VerifiedPostRequest<DeletePostRequestV2>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let request_body = verified_request.request.request_body;
    let publisher_user_id = request_body.publisher_user_id;
    let post_id = request_body.post_id.clone();
    let video_id = request_body.video_id.clone();

    // Verify that the requesting user is the publisher
    if verified_request.user_principal != publisher_user_id {
        return Err((
            StatusCode::FORBIDDEN,
            "Only the publisher can delete their own post".to_string(),
        ));
    }

    // Get the publisher's canister
    let publisher_canister_id = state
        .get_individual_canister_by_user_principal(publisher_user_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get publisher canister: {e}"),
            )
        })?;

    let user_ic_agent =
        get_agent_from_delegated_identity_wire(&verified_request.request.delegated_identity_wire)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Route based on canister
    if publisher_canister_id == *USER_INFO_SERVICE_CANISTER_ID {
        // Use UserPostService
        let user_post_service = UserPostService(*USER_POST_SERVICE_CANISTER_ID, &user_ic_agent);

        // UserPostService.delete_post takes a String
        let delete_res = user_post_service.delete_post(post_id.clone()).await;
        match delete_res {
            Ok(yral_canisters_client::user_post_service::Result_::Ok) => (),
            Ok(yral_canisters_client::user_post_service::Result_::Err(_)) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "Delete post failed - either the post doesn't exist or already deleted"
                        .to_string(),
                ))
            }
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Internal server error: {e}"),
                ))
            }
        }
    } else {
        // Use IndividualUserTemplate
        let individual_user_template =
            IndividualUserTemplate(publisher_canister_id, &user_ic_agent);

        // IndividualUserTemplate.delete_post still takes u64, so parse the String
        let post_id_u64 = post_id.parse::<u64>().map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Invalid post_id format: {e}"),
            )
        })?;
        let delete_res = individual_user_template.delete_post(post_id_u64).await;
        match delete_res {
            Ok(Result_::Ok) => (),
            Ok(Result_::Err(_)) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "Delete post failed - either the post doesn't exist or already deleted"
                        .to_string(),
                ))
            }
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Internal server error: {e}"),
                ))
            }
        }
    }

    // Insert to BigQuery with V2 function (String post_id)
    insert_video_delete_row_to_bigquery_v2(
        state.clone(),
        publisher_canister_id.to_string(),
        post_id,
        video_id.clone(),
    )
    .await
    .map_err(|e| {
        log::error!("Failed to insert video delete row to bigquery: {e}");

        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to insert video to bigquery: {e}"),
        )
    })?;

    // spawn to not block the request since as far as user is concerned, the post is deleted
    let bigquery_client = state.bigquery_client.clone();
    let video_id_clone = video_id.clone();
    tokio::spawn(async move {
        if let Err(e) =
            handle_duplicate_post_on_delete(&state.agent, bigquery_client, video_id_clone).await
        {
            log::error!("Failed to handle duplicate post on delete: {e}");
        }
    });

    Ok((StatusCode::OK, "Post deleted".to_string()))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VideoUniqueRow {
    pub video_id: String,
    pub videohash: String,
    pub created_at: String,
}

#[instrument(skip(bq_client))]
async fn get_hash_from_videohash_original(
    bq_client: &Client,
    video_id: &str,
) -> anyhow::Result<Option<String>> {
    let request = QueryRequest {
        query: format!(
            "SELECT videohash FROM `hot-or-not-feed-intelligence.yral_ds.videohash_original` WHERE video_id = '{video_id}'",
        ),
        ..Default::default()
    };
    let mut response = bq_client
        .query::<QueryRow>("hot-or-not-feed-intelligence", request)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to query videohash_original: {}", e))?;
    let Some(first) = response.next().await.context("Couldn't get first row")? else {
        return Ok(None);
    };

    let videohash: String = first
        .column(0)
        .context("Couldn't decode videohash out of row")?;

    Ok(Some(videohash))
}

#[instrument(skip(bq_client))]
pub async fn handle_duplicate_post_on_delete(
    agent: &Agent,
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

    let videohash = get_hash_from_videohash_original(&bq_client, &video_id)
        .await
        .context("Couldn't get video hash")?
        .ok_or(anyhow!("No video hash associated with the given video id"))?;

    DedupIndex(*DEDUP_INDEX_CANISTER_ID, agent)
        .remove_video_id(yral_canisters_client::dedup_index::RemoveVideoIdArgs {
            video_hash: videohash.clone(),
            video_id: video_id.clone(),
        })
        .await
        .context("Couldn't delete video from dedup index")?;

    // if its not unique, return
    if res_list.is_empty() {
        return Ok(());
    }

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

        let res = bq_client
            .tabledata()
            .insert(
                "hot-or-not-feed-intelligence",
                "yral_ds",
                "video_unique_v2",
                &request,
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to insert video unique v2 row to bigquery: {}", e)
            })?;

        if let Some(errors) = res.insert_errors {
            if !errors.is_empty() {
                log::error!("video_unique_v2 insert response : {errors:?}");
                return Err(anyhow::anyhow!(
                    "Failed to insert video unique v2 row to bigquery"
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

    // delete from video unique v2 as well
    let request = QueryRequest {
        query: format!(
            "DELETE FROM `hot-or-not-feed-intelligence.yral_ds.video_unique_v2` WHERE video_id = '{video_id}'"
        ),
        ..Default::default()
    };

    let res = bq_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to delete video unique v2 row to bigquery: {}", e))?;

    if let Some(errors) = res.errors {
        if !errors.is_empty() {
            log::error!("video_unique delete response : {errors:?}");
            return Err(anyhow::anyhow!(
                "Failed to delete video unique v2 row to bigquery"
            ));
        }
    }
    Ok(())
}

pub async fn insert_video_delete_row_to_bigquery(
    state: Arc<AppState>,
    canister_id: String,
    post_id: u64,
    video_id: String,
) -> Result<(), anyhow::Error> {
    bulk_insert_video_delete_rows(
        &state.bigquery_client,
        &state.kvrocks_client,
        vec![UserPost {
            canister_id,
            post_id,
            video_id,
        }],
    )
    .await?;

    Ok(())
}

pub async fn insert_video_delete_row_to_bigquery_v2(
    state: Arc<AppState>,
    canister_id: String,
    post_id: String, // Changed from u64 to String
    video_id: String,
) -> Result<(), anyhow::Error> {
    bulk_insert_video_delete_rows_v2(
        &state.bigquery_client,
        &state.kvrocks_client,
        vec![UserPostV2 {
            canister_id,
            post_id,
            video_id,
        }],
    )
    .await?;

    Ok(())
}

pub async fn bulk_insert_video_delete_rows(
    bq_client: &Client,
    kvrocks_client: &Option<KvrocksClient>,
    posts: Vec<UserPost>,
) -> Result<(), anyhow::Error> {
    // Process posts in batches of 500
    for chunk in posts.chunks(BULK_INSERT_DELETE_BIGQUERY_BATCH_SIZE) {
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

        // Also push to kvrocks
        if let Some(ref kvrocks) = kvrocks_client {
            for post in chunk {
                let delete_data = serde_json::json!({
                    "canister_id": post.canister_id,
                    "post_id": post.post_id,
                    "video_id": post.video_id,
                    "gcs_video_id": format!("gs://yral-videos/{}.mp4", post.video_id),
                    "deleted_at": chrono::Utc::now().to_rfc3339(),
                });
                if let Err(e) = kvrocks
                    .store_video_deleted(&post.video_id, &delete_data)
                    .await
                {
                    log::error!(
                        "Error pushing video delete data to kvrocks for {}: {}",
                        post.video_id,
                        e
                    );
                }
            }
        }
    }

    Ok(())
}

pub async fn bulk_insert_video_delete_rows_v2(
    bq_client: &Client,
    kvrocks_client: &Option<KvrocksClient>,
    posts: Vec<UserPostV2>,
) -> Result<(), anyhow::Error> {
    // Process posts in batches of 500
    for chunk in posts.chunks(BULK_INSERT_DELETE_BIGQUERY_BATCH_SIZE) {
        let rows: Vec<Row<VideoDeleteRow>> = chunk
            .iter()
            .map(|post| {
                let video_delete_row = VideoDeleteRow {
                    canister_id: post.canister_id.clone(),
                    post_id: 0, // Post ID is not used in V2, so set to 0
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

        // Also push to kvrocks
        if let Some(ref kvrocks) = kvrocks_client {
            for post in chunk {
                let delete_data = serde_json::json!({
                    "canister_id": post.canister_id,
                    "post_id": post.post_id,
                    "video_id": post.video_id,
                    "gcs_video_id": format!("gs://yral-videos/{}.mp4", post.video_id),
                    "deleted_at": chrono::Utc::now().to_rfc3339(),
                });
                if let Err(e) = kvrocks
                    .store_video_deleted(&post.video_id, &delete_data)
                    .await
                {
                    log::error!(
                        "Error pushing video delete data to kvrocks for {}: {}",
                        post.video_id,
                        e
                    );
                }
            }
        }
    }

    Ok(())
}
