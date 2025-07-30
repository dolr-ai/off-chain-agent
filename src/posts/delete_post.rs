use std::sync::Arc;

use super::types::{UserPost, VideoDeleteRow};
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
};

use crate::{
    app_state::AppState, consts::DEDUP_INDEX_CANISTER_ID,
    posts::queries::get_duplicate_children_query,
    user::utils::get_agent_from_delegated_identity_wire,
};

use super::{types, verify, DeletePostRequest};

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
                format!("Internal server error: {}", e),
            ))
        }
    }

    insert_video_delete_row_to_bigquery(state.clone(), canister_id, post_id, video_id.clone())
        .await
        .map_err(|e| {
            log::error!("Failed to insert video delete row to bigquery: {}", e);

            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to insert video to bigquery: {}", e),
            )
        })?;

    // spawn to not block the request since as far as user is concerned, the post is deleted
    let bigquery_client = state.bigquery_client.clone();
    let video_id_clone = video_id.clone();
    tokio::spawn(async move {
        if let Err(e) =
            handle_duplicate_post_on_delete(&state.agent, bigquery_client, video_id_clone).await
        {
            log::error!("Failed to handle duplicate post on delete: {}", e);
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
            video_id: new_parent_video_id.clone(),
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

        copy_details_from_old_to_new_parent(&bq_client, &video_id, &new_parent_video_id).await?;

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
                log::error!("video_unique insert response : {:?}", errors);
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
                log::error!("video_unique_v2 insert response : {:?}", errors);
                return Err(anyhow::anyhow!(
                    "Failed to insert video unique v2 row to bigquery"
                ));
            }
        }
    }

    // delete old parent from video_unique table
    let request = QueryRequest {
        query: format!(
            "DELETE FROM `hot-or-not-feed-intelligence.yral_ds.video_unique` WHERE video_id = '{}'",
            video_id
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
            log::error!("video_unique delete response : {:?}", errors);
            return Err(anyhow::anyhow!(
                "Failed to delete video unique row to bigquery"
            ));
        }
    }

    // delete from video unique v2 as well
    let request = QueryRequest {
        query: format!(
            "DELETE FROM `hot-or-not-feed-intelligence.yral_ds.video_unique_v2` WHERE video_id = '{}'",
            video_id
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
            log::error!("video_unique delete response : {:?}", errors);
            return Err(anyhow::anyhow!(
                "Failed to delete video unique v2 row to bigquery"
            ));
        }
    }
    Ok(())
}

fn copy_embeddings(old_parent_video_id: &str, new_parent_video_id: &str) -> String {
    format!(
        r#"
        INSERT INTO `hot-or-not-feed-intelligence.yral_ds.video_embeddings_agg`
        SELECT
        	ml_generate_embedding_result,
        	ml_generate_embedding_status,
        	ml_generate_embedding_start_sec,
        	ml_generate_embedding_end_sec,
        	"gs://yral-videos/{new_parent_video_id}.mp4" as uri,
        	generation,
        	content_type,
        	size,
        	md5_hash,
        	updated,
        	metadata,
        	is_nsfw,
        	nsfw_ec,
        	nsfw_gore,
        	probability,
        	"{new_parent_video_id}" as video_id
        FROM `hot-or-not-feed-intelligence.yral_ds.video_embeddings_agg`
        WHERE video_id = "{old_parent_video_id}"
    "#
    )
}

fn copy_nsfw_details(old_parent_video_id: &str, new_parent_video_id: &str) -> String {
    format!(
        r#"
        INSERT INTO `hot-or-not-feed-intelligence.yral_ds.video_nsfw_agg`
        SELECT
        	"{new_parent_video_id}" as video_id,
        	"gs://yral-videos/{new_parent_video_id}.mp4" as gcs_video_id,
            nsfw_ec,
            nsfw_gore,
            is_nsfw,
            probability
        FROM `hot-or-not-feed-intelligence.yral_ds.video_nsfw_agg`
        WHERE video_id = "{old_parent_video_id}"
    "#
    )
}

async fn copy_details_from_old_to_new_parent(
    bq_client: &Client,
    old_parent_video_id: &str,
    new_parent_video_id: &str,
) -> anyhow::Result<()> {
    // running queries serially to make debugging easier in case this routine
    // starts to fail

    let request = QueryRequest {
        query: copy_embeddings(old_parent_video_id, new_parent_video_id),
        ..Default::default()
    };

    let res = bq_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await
        .context("Couldn't run query to copy embeddings for new parent")?;

    if let Some(errors) = res.errors {
        if !errors.is_empty() {
            log::error!("copy embeddings query response: {:?}", errors);
            return Err(anyhow::anyhow!("Couldn't copy embeddings for new parent"));
        }
    }

    let request = QueryRequest {
        query: copy_nsfw_details(old_parent_video_id, new_parent_video_id),
        ..Default::default()
    };

    let res = bq_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await
        .context("Couldn't run query to copy nsfw details for new parent")?;

    if let Some(errors) = res.errors {
        if !errors.is_empty() {
            log::error!("copy embeddings query response: {:?}", errors);
            return Err(anyhow::anyhow!("Couldn't copy nsfw details for new parent"));
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
        vec![UserPost {
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
                log::error!("video_deleted bulk insert errors: {:?}", errors);
                return Err(anyhow::anyhow!(
                    "Failed to bulk insert video deleted rows to bigquery"
                ));
            }
        }
    }

    Ok(())
}
