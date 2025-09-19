mod verify;

use std::sync::Arc;

use axum::middleware;
use axum::{extract::State, response::Response, routing::post, Json, Router};
use candid::Principal;
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::query::row::Row as QueryRow;
use hotornot_job::start_hotornot_job_v2;
use http::StatusCode;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::{json, Value};
use tower::ServiceBuilder;
use tracing::instrument;

use crate::pipeline::Step;
use crate::qstash::duplicate::VideoPublisherDataV2;
use crate::qstash::hotornot_job::start_hotornot_job_v3;
use crate::qstash::verify::verify_qstash_message;
use crate::setup_context;
use crate::{
    app_state::AppState,
    canister::{
        delete::handle_delete_and_reclaim_canisters,
        snapshot::{
            // alert::snapshot_alert_job,
            alert::snapshot_alert_job,
            snapshot_v2::{backup_canisters_job_v2, backup_user_canister},
        },
    },
    events::{
        event::{storj::storj_ingest, upload_video_gcs},
        nsfw::{extract_frames_and_upload, nsfw_job, nsfw_job_v2},
    },
    posts::report_post::qstash_report_post,
};

pub mod client;
pub mod duplicate;
pub mod hotornot_job;

#[derive(Clone)]
pub struct QStashState {
    decoding_key: Arc<DecodingKey>,
    validation: Arc<Validation>,
}

impl QStashState {
    pub fn init(verification_key: String) -> Self {
        let decoding_key = DecodingKey::from_secret(verification_key.as_bytes());
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&["Upstash"]);
        validation.set_audience(&[""]);
        Self {
            decoding_key: Arc::new(decoding_key),
            validation: Arc::new(validation),
        }
    }
}

#[derive(Debug, Deserialize)]
struct VideoHashIndexingRequest {
    video_id: String,
    video_url: String,
    publisher_data: VideoPublisherDataV2,
}

#[instrument(skip(state))]
async fn video_deduplication_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<VideoHashIndexingRequest>,
) -> Result<Response, StatusCode> {
    setup_context!(&req.video_id, Step::Deduplication);

    log::info!(
        "Processing video deduplication for video ID: {}",
        req.video_id
    );

    let publisher_data = req.publisher_data.clone();

    let qstash_client = state.qstash_client.clone();

    if let Err(e) = duplicate::VideoHashDuplication
        .process_video_deduplication(
            &state.agent,
            &state.bigquery_client,
            &req.video_id,
            &req.video_url,
            publisher_data,
            move |vid_id, post_id, timestamp, publisher_user_id| {
                // Clone the values to ensure they have 'static lifetime
                let vid_id = vid_id.to_string();
                let publisher_user_id = publisher_user_id.to_string();

                // Use the cloned qstash_client instead of accessing through state
                let qstash_client = qstash_client.clone();

                Box::pin(async move {
                    qstash_client
                        .publish_video(&vid_id, post_id, timestamp, &publisher_user_id)
                        .await
                })
            },
        )
        .await
    {
        log::error!("Video deduplication failed: {e}");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let response = Response::builder()
        .status(StatusCode::OK)
        .body("Video deduplication check completed".into())
        .unwrap();

    Ok(response)
}

#[derive(Deserialize)]
pub struct BackfillWatchedIndividual {
    principal: Principal,
}

async fn backfill_watched_all(State(state): State<Arc<AppState>>) -> Result<Response, StatusCode> {
    // Get all user principals from the canisters
    let user_principal_canister_list =
        crate::canister::utils::get_user_principal_canister_list_v2(&state.agent)
            .await
            .map_err(|e| {
                log::error!("Failed to get user principals: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

    log::info!(
        "Found {} user principals to backfill",
        user_principal_canister_list.len()
    );

    // Queue individual backfill jobs for each principal via QStash
    let mut queued_count = 0;
    let qstash_client = state.qstash_client.clone();

    for (user_principal, _canister_id) in user_principal_canister_list {
        // Create the payload for individual backfill
        let payload = json!({
            "principal": user_principal.to_string()
        });

        // Queue the job to QStash
        let off_chain_ep = crate::consts::OFF_CHAIN_AGENT_URL
            .join("qstash/backfill_watched/individual")
            .unwrap();

        let url = qstash_client
            .base_url
            .join(&format!("publish/{off_chain_ep}"))
            .unwrap();

        match qstash_client
            .client
            .post(url)
            .json(&payload)
            .header("Content-Type", "application/json")
            .header("upstash-method", "POST")
            .header("Upstash-Retries", "0")
            .header("upstash-delay", "1s") // Small delay between jobs
            .send()
            .await
        {
            Ok(_) => {
                queued_count += 1;
            }
            Err(e) => {
                log::error!(
                    "Failed to queue backfill for principal {}: {}",
                    user_principal,
                    e
                );
            }
        }
    }

    log::info!("Successfully queued {} backfill jobs", queued_count);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(
            json!({
                "status": "success",
                "queued_count": queued_count,
                "message": format!("Queued {} user backfill jobs", queued_count)
            })
            .to_string()
            .into(),
        )
        .unwrap())
}

async fn backfill_watched_individual(
    State(state): State<Arc<AppState>>,
    Json(BackfillWatchedIndividual { principal }): Json<BackfillWatchedIndividual>,
) -> Result<Response, StatusCode> {
    let mut next_cursor = None;
    let mut lookup_pairs = Vec::new();

    loop {
        let games_played = reqwest::Client::new()
            .post(format!(
                "https://yral-hot-or-not.go-bazzinga.workers.dev/v4/games/{principal}"
            ))
            .json(&json!({
                "page_size": 1000,
                "cursor": next_cursor
            }))
            .send()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;

        let data: Value = games_played
            .json()
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if let Some(games) = data["games"].as_array() {
            for game in games {
                if let (Some(publisher), Some(post_id)) = (
                    game["publisher_principal"].as_str(),
                    game["post_id"].as_str(),
                ) {
                    lookup_pairs.push(format!(
                        r#"STRUCT("{}" AS post_id, "{}" AS publisher)"#,
                        post_id, publisher
                    ));
                }
            }
        }

        if data["next"].is_null() {
            break;
        } else {
            next_cursor = Some(data["next"].clone());
        }
    }

    if lookup_pairs.is_empty() {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .body(
                json!({
                    "principal": principal.to_string(),
                    "video_ids_count": 0,
                    "status": "no_games_found"
                })
                .to_string()
                .into(),
            )
            .unwrap());
    }

    let query = format!(
        r#"
    WITH lookup_pairs AS (
        SELECT post_id, publisher FROM UNNEST([
            {}
        ])
    )
    SELECT DISTINCT
        REGEXP_EXTRACT(vi.uri, r'gs://yral-videos/([a-f0-9-]+)\.mp4') AS video_id
    FROM
        `hot-or-not-feed-intelligence.yral_ds.video_index` vi
    INNER JOIN
        lookup_pairs lp
    ON
        vi.post_id = lp.post_id
        AND vi.publisher_user_id = lp.publisher
    WHERE
        vi.uri IS NOT NULL
    "#,
        lookup_pairs.join(",\n            ")
    );

    let request = QueryRequest {
        query: query.clone(),
        use_legacy_sql: false,
        ..Default::default()
    };

    let mut result_set: google_cloud_bigquery::query::Iterator<QueryRow> = state
        .bigquery_client
        .query("hot-or-not-feed-intelligence", request)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Collect just the video UUIDs
    let mut video_ids: Vec<String> = Vec::new();

    while let Ok(Some(row)) = result_set.next().await {
        if let Ok(video_id) = row.column::<String>(0) {
            video_ids.push(video_id);
        }
    }

    if !video_ids.is_empty() {
        let principal_str = principal.to_string();

        let clean_key = format!(
            "{}{}",
            principal_str,
            yral_ml_feed_cache::consts::USER_WATCHED_VIDEO_IDS_SET_CLEAN_SUFFIX_V2
        );

        let nsfw_key = format!(
            "{}{}",
            principal_str,
            yral_ml_feed_cache::consts::USER_WATCHED_VIDEO_IDS_SET_NSFW_SUFFIX_V2
        );

        if let Err(e) = state
            .ml_feed_cache
            .add_watched_video_ids_to_set(&clean_key, video_ids.clone())
            .await
        {
            log::error!("Failed to add videos to clean set: {}", e);
        }

        if let Err(e) = state
            .ml_feed_cache
            .add_watched_video_ids_to_set(&nsfw_key, video_ids.clone())
            .await
        {
            log::error!("Failed to add videos to nsfw set: {}", e);
        }

        log::info!(
            "Added {} video IDs to Redis for principal {} (both clean and nsfw sets)",
            video_ids.len(),
            principal_str
        );
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(
            json!({
                "principal": principal.to_string(),
                "video_ids_count": video_ids.len(),
                "status": "success"
            })
            .to_string()
            .into(),
        )
        .unwrap())
}

#[instrument(skip(app_state))]
// QStash router remains the same but without the admin route
pub fn qstash_router<S>(app_state: Arc<AppState>) -> Router<S> {
    Router::new()
        .route("/video_deduplication", post(video_deduplication_handler))
        .route("/upload_video_gcs", post(upload_video_gcs))
        .route("/enqueue_video_frames", post(extract_frames_and_upload))
        .route("/enqueue_video_nsfw_detection", post(nsfw_job))
        .route("/enqueue_video_nsfw_detection_v2", post(nsfw_job_v2))
        .route("/storj_ingest", post(storj_ingest))
        .route("/report_post", post(qstash_report_post))
        .route(
            "/start_backup_canisters_job_v2",
            post(backup_canisters_job_v2),
        )
        .route(
            "/backfill_watched/individual",
            post(backfill_watched_individual),
        )
        .route("/backfill_watched/all", post(backfill_watched_all))
        .route("/backup_user_canister", post(backup_user_canister))
        .route("/snapshot_alert_job", post(snapshot_alert_job))
        .route("/start_hotornot_job_v2", post(start_hotornot_job_v2))
        .route("/start_hotornot_job_v3", post(start_hotornot_job_v3))
        .route(
            "/delete_and_reclaim_canisters",
            post(handle_delete_and_reclaim_canisters),
        )
        .route(
            "/process_video_gen",
            post(crate::videogen::qstash_process::process_video_generation),
        )
        .route(
            "/video_gen_callback",
            post(crate::videogen::qstash_callback::handle_video_gen_callback),
        )
        .route(
            "/tournament/create",
            post(crate::leaderboard::handlers::create_tournament_handler),
        )
        .route(
            "/tournament/start/{id}",
            post(crate::leaderboard::handlers::start_tournament_handler),
        )
        .route(
            "/tournament/finalize/{id}",
            post(crate::leaderboard::handlers::finalize_tournament_handler),
        )
        .route(
            "/tournament/end/{id}",
            post(crate::leaderboard::handlers::end_tournament_handler),
        )
        .layer(ServiceBuilder::new().layer(middleware::from_fn_with_state(
            app_state.qstash.clone(),
            verify_qstash_message,
        )))
        .with_state(app_state)
}
