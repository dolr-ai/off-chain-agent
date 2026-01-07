mod verify;

use std::sync::Arc;

use axum::middleware;
use axum::{extract::State, response::Response, routing::post, Json, Router};
use http::StatusCode;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use tower::ServiceBuilder;
use tracing::instrument;

use crate::pipeline::Step;
use crate::qstash::duplicate::VideoPublisherDataV2;
use crate::qstash::service_canister_migration::{
    migrate_individual_user_to_service_canister, transfer_all_posts_for_the_individual_user,
    update_the_metadata_mapping,
};
use crate::qstash::verify::verify_qstash_message;
use crate::setup_context;
use crate::{
    app_state::AppState,
    canister::{
        delete::handle_delete_and_reclaim_canisters,
        snapshot::{
            alert::snapshot_alert_job,
            ic_snapshot::take_ic_snapshot,
            snapshot_v2::{backup_canisters_job_v2, backup_user_canister},
        },
    },
    events::{
        event::{storj::storj_ingest, upload_video_gcs},
        nsfw::{extract_frames_and_upload, nsfw_job, nsfw_job_v2},
    },
    posts::report_post::qstash_report_post,
    rewards::api::update_reward_config,
};

pub mod client;
pub mod dedup_index_backfill;
pub mod duplicate;
#[cfg(not(feature = "local-bin"))]
pub mod milvus_ingest;
pub mod phash_bulk;
pub mod service_canister_migration;

#[derive(Clone)]
pub struct QStashState {
    #[allow(dead_code)]
    decoding_key: Arc<DecodingKey>,
    #[allow(dead_code)]
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

#[cfg(not(feature = "local-bin"))]
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
        .process_video_deduplication_v2(
            &state.agent,
            &state.bigquery_client,
            &state.milvus_client,
            &state.leaderboard_redis_pool,
            &state.kvrocks_client,
            &req.video_id,
            &req.video_url,
            publisher_data,
            30, // Default hamming threshold
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

#[instrument(skip(app_state))]
// QStash router remains the same but without the admin route
pub fn qstash_router<S>(app_state: Arc<AppState>) -> Router<S> {
    let mut router = Router::new();

    #[cfg(not(feature = "local-bin"))]
    {
        router = router.route("/video_deduplication", post(video_deduplication_handler));
    }

    let router = router
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
        .route("/backup_user_canister", post(backup_user_canister))
        .route("/snapshot_alert_job", post(snapshot_alert_job))
        .route("/take_ic_snapshot", post(take_ic_snapshot))
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
            "/upload_ai_generated_video_to_canister_in_drafts",
            post(crate::videogen::qstash_process::upload_ai_generated_video_to_canister_in_drafts),
        )
        .route(
            "/tournament/create",
            post(crate::leaderboard::handlers::create_tournament_handler),
        )
        .route(
            "/migrate_individual_user_to_service_canister",
            post(migrate_individual_user_to_service_canister),
        )
        .route(
            "/transfer_all_posts_for_individual_user",
            post(transfer_all_posts_for_the_individual_user),
        )
        .route(
            "/update_yral_metadata_mapping",
            post(update_the_metadata_mapping),
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
        .route("/rewards/update_config", post(update_reward_config))
        .route(
            "/compute_video_phash",
            post(phash_bulk::compute_video_phash_handler),
        )
        .route(
            "/bulk_compute_phash",
            post(phash_bulk::bulk_compute_phash_handler),
        )
        .route(
            "/backfill_dedup_index",
            post(dedup_index_backfill::backfill_dedup_index_handler),
        );

    #[cfg(not(feature = "local-bin"))]
    let router = router
        .route(
            "/milvus/ingest_phash",
            post(milvus_ingest::ingest_phash_to_milvus_handler),
        )
        .route(
            "/milvus/backfill_unique_videos",
            post(milvus_ingest::backfill_unique_videos_handler),
        )
        .route(
            "/milvus/bulk_ingest_unique_hashes",
            post(milvus_ingest::bulk_ingest_unique_hashes_handler),
        )
        .route(
            "/milvus/deduplicate_videos",
            post(milvus_ingest::deduplicate_videos_handler),
        );

    router
        .layer(ServiceBuilder::new().layer(middleware::from_fn_with_state(
            app_state.qstash.clone(),
            verify_qstash_message,
        )))
        .with_state(app_state)
}
