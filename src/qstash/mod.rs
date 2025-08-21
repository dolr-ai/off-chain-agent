mod verify;

use std::sync::Arc;

use axum::{extract::State, middleware, response::Response, routing::post, Json, Router};
use hotornot_job::start_hotornot_job_v2;
use http::StatusCode;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use tower::ServiceBuilder;
use tracing::instrument;
use verify::verify_qstash_message;

use crate::pipeline::Step;
use crate::qstash::duplicate::VideoPublisherDataV2;
use crate::qstash::hotornot_job::start_hotornot_job_v3;
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
        hls::process_hls,
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
    #[serde(default)]
    video_info: Option<crate::events::event::UploadVideoInfoV2>,
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
                    // Only trigger video processing (HLS will be triggered after NSFW)
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
    Router::new()
        .route("/video_deduplication", post(video_deduplication_handler))
        .route("/upload_video_gcs", post(upload_video_gcs))
        .route("/enqueue_video_frames", post(extract_frames_and_upload))
        .route("/enqueue_video_nsfw_detection", post(nsfw_job))
        .route("/enqueue_video_nsfw_detection_v2", post(nsfw_job_v2))
        .route("/process_hls", post(process_hls))
        .route("/storj_ingest", post(storj_ingest))
        .route("/report_post", post(qstash_report_post))
        .route(
            "/start_backup_canisters_job_v2",
            post(backup_canisters_job_v2),
        )
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
        .layer(ServiceBuilder::new().layer(middleware::from_fn_with_state(
            app_state.qstash.clone(),
            verify_qstash_message,
        )))
        .with_state(app_state)
}
