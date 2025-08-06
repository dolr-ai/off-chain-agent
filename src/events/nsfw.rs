use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use crate::{
    consts::{NSFW_SERVER_URL, NSFW_THRESHOLD},
    pipeline::Step,
    qstash::client::QStashClient,
    setup_context,
};
use anyhow::Error;
use axum::{extract::State, Json};
use google_cloud_bigquery::http::{
    job::query::QueryRequest,
    tabledata::{
        insert_all::{InsertAllRequest, Row},
        list::Value,
    },
};
use serde::{Deserialize, Serialize};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{metadata::MetadataValue, Request};
use tracing::instrument;

use crate::{app_state::AppState, AppError};

use super::event::UploadVideoInfo;

pub mod nsfw_detector {
    tonic::include_proto!("nsfw_detector");
}

fn create_output_directory(video_id: &str) -> Result<PathBuf, Error> {
    let video_name = Path::new(video_id)
        .file_stem()
        .ok_or(anyhow::anyhow!("Failed to get file stem"))?
        .to_str()
        .ok_or(anyhow::anyhow!("Failed to convert file stem to string"))?;
    let output_dir = Path::new(".").join(video_name);

    if !output_dir.exists() {
        fs::create_dir(&output_dir)?;
    }

    Ok(output_dir)
}

#[instrument]
pub async fn extract_frames(video_path: &str, output_dir: PathBuf) -> Result<Vec<Vec<u8>>, Error> {
    let output_pattern = output_dir.join("output-%04d.jpg");
    let video_path_clone = video_path.to_string();
    let output_pattern_str = output_pattern.to_string_lossy().to_string();

    let status = tokio::task::spawn_blocking(move || {
        Command::new("ffmpeg")
            .arg("-loglevel")
            .arg("error")
            .arg("-i")
            .arg(&video_path_clone)
            .arg("-vf")
            .arg("fps=1")
            .arg("-pix_fmt")
            .arg("rgb24")
            .arg(&output_pattern_str)
            .status()
    })
    .await??;

    if !status.success() {
        return Err(anyhow::anyhow!("Failed to extract frames"));
    }

    let mut frames = Vec::new();
    for entry in fs::read_dir(output_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            let frame = fs::read(&path)?;
            frames.push(frame);
        }
    }

    Ok(frames)
}

#[instrument(skip(gcs_client, frames))]
pub async fn upload_frames_to_gcs(
    gcs_client: &cloud_storage::Client,
    frames: Vec<Vec<u8>>,
    video_id: &str,
) -> Result<(), Error> {
    let bucket_name = "yral-video-frames";

    // Create a vector of futures for concurrent uploads
    let upload_futures = frames.into_iter().enumerate().map(|(i, frame)| {
        let frame_path = format!("{video_id}/frame-{i}.jpg");
        let bucket_name = bucket_name.to_string();

        async move {
            gcs_client
                .object()
                .create(&bucket_name, frame, &frame_path, "image/jpeg")
                .await
        }
    });

    // Execute all futures concurrently and collect results
    let results = futures::future::join_all(upload_futures).await;

    // Check if any upload failed
    for result in results {
        result?;
    }

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VideoRequest {
    video_id: String,
    video_info: UploadVideoInfo,
}

// extract_frames_and_upload API handler which takes video_id as queryparam in axum
#[instrument(skip(state))]
pub async fn extract_frames_and_upload(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VideoRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    setup_context!(&payload.video_id, Step::ExtractFrames, {
        "upload_info": &payload.video_info
    });

    let video_id = payload.video_id;
    let video_path = format!(
        "https://customer-2p3jflss4r4hmpnz.cloudflarestream.com/{video_id}/downloads/default.mp4"
    );
    let output_dir = create_output_directory(&video_id)?;
    let frames = extract_frames(&video_path, output_dir.clone()).await?;
    #[cfg(not(feature = "local-bin"))]
    upload_frames_to_gcs(&state.gcs_client, frames, &video_id).await?;
    // delete output directory
    fs::remove_dir_all(output_dir)?;

    // enqueue qstash job to detect nsfw
    let qstash_client = state.qstash_client.clone();
    qstash_client
        .publish_video_nsfw_detection(&video_id, &payload.video_info)
        .await?;

    Ok(Json(
        serde_json::json!({ "message": "Frames extracted and uploaded to GCS" }),
    ))
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct NSFWInfo {
    pub is_nsfw: bool,
    pub nsfw_ec: String,
    pub nsfw_gore: String,
    pub csam_detected: bool,
}

#[instrument]
pub async fn get_video_nsfw_info(video_id: String) -> Result<NSFWInfo, Error> {
    // create a new connection everytime and depend on fly proxy to load balance
    let tls_config = ClientTlsConfig::new().with_webpki_roots();
    let channel = Channel::from_static(NSFW_SERVER_URL)
        .tls_config(tls_config)
        .expect("Couldn't update TLS config for nsfw agent")
        .connect()
        .await
        .expect("Couldn't connect to nsfw agent");

    let nsfw_grpc_auth_token = env::var("NSFW_GRPC_TOKEN").expect("NSFW_GRPC_TOKEN");
    let token: MetadataValue<_> = format!("Bearer {nsfw_grpc_auth_token}").parse()?;

    let mut client = nsfw_detector::nsfw_detector_client::NsfwDetectorClient::with_interceptor(
        channel,
        move |mut req: Request<()>| {
            req.metadata_mut().insert("authorization", token.clone());
            Ok(req)
        },
    );

    let req = tonic::Request::new(nsfw_detector::NsfwDetectorRequestVideoId {
        video_id: video_id.clone(),
    });
    let res = client.detect_nsfw_video_id(req).await?;

    let nsfw_info = NSFWInfo::from(res.into_inner());

    Ok(nsfw_info)
}

#[derive(Serialize)]
struct VideoNSFWData {
    video_id: String,
    gcs_video_id: String,
    is_nsfw: bool,
    nsfw_ec: String,
    nsfw_gore: String,
}
#[cfg(feature = "local-bin")]
pub async fn nsfw_job(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VideoRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    Err(anyhow::anyhow!("not implemented for local binary").into())
}

#[cfg(not(feature = "local-bin"))]
#[instrument(skip(state))]
pub async fn nsfw_job(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VideoRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    use sentry_anyhow::capture_anyhow;

    setup_context!(&payload.video_id, Step::NsfwDetection, {
        "upload_info": &payload.video_info
    });

    let video_id = payload.video_id;
    let video_info = payload.video_info;

    let nsfw_info = get_video_nsfw_info(video_id.clone())
        .await
        .inspect_err(|e| {
            capture_anyhow(e);
        })?;

    // push nsfw info to bigquery table using google-cloud-bigquery
    let bigquery_client = state.bigquery_client.clone();

    push_nsfw_data_bigquery(bigquery_client, nsfw_info, video_id.clone()).await?;

    // enqueue qstash job to detect nsfw v2
    let qstash_client = state.qstash_client.clone();
    qstash_client
        .publish_video_nsfw_detection_v2(&video_id, video_info)
        .await?;

    Ok(Json(serde_json::json!({ "message": "NSFW job completed" })))
}

#[instrument(skip(qstash))]
async fn duplicate_to_storj(
    qstash: &QStashClient,
    video_info: UploadVideoInfo,
    is_nsfw: bool,
) -> Result<(), AppError> {
    let duplicate_args = storj_interface::duplicate::Args {
        publisher_user_id: video_info.publisher_user_id,
        video_id: video_info.video_id,
        is_nsfw,
        metadata: [
            ("post_id".into(), video_info.post_id.to_string()),
            // TODO: confirm with Tushar about this
            // ("canister_id".into(), video_info.canister_id),
            ("timestamp".into(), video_info.timestamp),
        ]
        .into(),
    };

    qstash.duplicate_to_storj(duplicate_args).await?;

    Ok(())
}

#[instrument(skip(bigquery_client))]
pub async fn push_nsfw_data_bigquery(
    bigquery_client: google_cloud_bigquery::client::Client,
    nsfw_info: NSFWInfo,
    video_id: String,
) -> Result<(), Error> {
    let row_data = VideoNSFWData {
        video_id: video_id.clone(),
        gcs_video_id: format!("gs://yral-videos/{video_id}.mp4"),
        is_nsfw: nsfw_info.is_nsfw,
        nsfw_ec: nsfw_info.nsfw_ec,
        nsfw_gore: nsfw_info.nsfw_gore,
    };

    let row = Row {
        insert_id: None,
        json: row_data,
    };

    let request = InsertAllRequest {
        rows: vec![row],
        ..Default::default()
    };

    bigquery_client
        .tabledata()
        .insert(
            "hot-or-not-feed-intelligence",
            "yral_ds",
            "video_nsfw",
            &request,
        )
        .await?;

    Ok(())
}

impl From<nsfw_detector::NsfwDetectorResponse> for NSFWInfo {
    fn from(item: nsfw_detector::NsfwDetectorResponse) -> Self {
        let is_nsfw = item.csam_detected
            || matches!(
                item.nsfw_gore.as_str(),
                "POSSIBLE" | "LIKELY" | "VERY_LIKELY"
            )
            || matches!(item.nsfw_ec.as_str(), "nudity" | "provocative" | "explicit");

        Self {
            is_nsfw,
            nsfw_ec: item.nsfw_ec,
            nsfw_gore: item.nsfw_gore,
            csam_detected: item.csam_detected,
        }
    }
}

#[cfg(feature = "local-bin")]
pub async fn nsfw_job_v2(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VideoRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    Err(anyhow::anyhow!("not implemented for local binary").into())
}

#[cfg(not(feature = "local-bin"))]
#[instrument(skip(state))]
pub async fn nsfw_job_v2(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VideoRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    use sentry_anyhow::capture_anyhow;

    setup_context!(&payload.video_id, Step::NsfwDetectionV2, {
        "upload_info": &payload.video_info
    });

    let video_id = payload.video_id;

    let nsfw_prob = get_video_nsfw_info_v2(video_id.clone())
        .await
        .inspect_err(|err| {
            capture_anyhow(err);
        })?;
    let is_nsfw = nsfw_prob >= NSFW_THRESHOLD;

    // push nsfw info to bigquery table using google-cloud-bigquery
    let bigquery_client = state.bigquery_client.clone();
    push_nsfw_data_bigquery_v2(bigquery_client, nsfw_prob, video_id.clone()).await?;

    duplicate_to_storj(&state.qstash_client, payload.video_info, is_nsfw).await?;

    Ok(Json(
        serde_json::json!({ "message": "NSFW v2 job completed" }),
    ))
}

#[instrument]
pub async fn get_video_nsfw_info_v2(video_id: String) -> Result<f32, Error> {
    // create a new connection everytime and depend on fly proxy to load balance
    let tls_config = ClientTlsConfig::new().with_webpki_roots();
    let channel = Channel::from_static(NSFW_SERVER_URL)
        .tls_config(tls_config)
        .expect("Couldn't update TLS config for nsfw agent")
        .connect()
        .await
        .expect("Couldn't connect to nsfw agent");

    let nsfw_grpc_auth_token = env::var("NSFW_GRPC_TOKEN").expect("NSFW_GRPC_TOKEN");
    let token: MetadataValue<_> = format!("Bearer {nsfw_grpc_auth_token}").parse()?;

    let mut client = nsfw_detector::nsfw_detector_client::NsfwDetectorClient::with_interceptor(
        channel,
        move |mut req: Request<()>| {
            req.metadata_mut().insert("authorization", token.clone());
            Ok(req)
        },
    );

    // get embedding nsfw
    let embedding_req = tonic::Request::new(nsfw_detector::EmbeddingNsfwDetectorRequest {
        video_id: video_id.clone(),
    });
    let embedding_res = client.detect_nsfw_embedding(embedding_req).await?;

    Ok(embedding_res.into_inner().probability)
}

#[derive(Serialize)]
struct VideoNSFWDataV2 {
    video_id: String,
    gcs_video_id: String,
    is_nsfw: bool,
    nsfw_ec: String,
    nsfw_gore: String,
    probability: f32,
}

#[derive(Serialize, Debug)]
struct VideoEmbeddingMetadata {
    name: String,
    value: String,
}

#[derive(Serialize, Debug)]
struct VideoEmbeddingAgg {
    ml_generate_embedding_result: Vec<f64>,
    ml_generate_embedding_status: Option<String>,
    ml_generate_embedding_start_sec: Option<i64>,
    ml_generate_embedding_end_sec: Option<i64>,
    uri: Option<String>,
    generation: Option<i64>,
    content_type: Option<String>,
    size: Option<i64>,
    md5_hash: Option<String>,
    updated: Option<String>,
    metadata: Vec<VideoEmbeddingMetadata>,
    is_nsfw: Option<bool>,
    nsfw_ec: Option<String>,
    nsfw_gore: Option<String>,
    probability: Option<f32>,
    video_id: Option<String>,
}

#[instrument(skip(bigquery_client))]
pub async fn push_nsfw_data_bigquery_v2(
    bigquery_client: google_cloud_bigquery::client::Client,
    nsfw_prob: f32,
    video_id: String,
) -> Result<(), Error> {
    // First query to get existing NSFW data
    let query = format!(
        "SELECT video_id, gcs_video_id, is_nsfw, nsfw_ec, nsfw_gore 
         FROM `hot-or-not-feed-intelligence.yral_ds.video_nsfw`
         WHERE video_id = '{video_id}'"
    );

    let request = QueryRequest {
        query,
        ..Default::default()
    };

    let result = bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await?;

    // Get the first row
    let row = result
        .rows
        .and_then(|mut rows| rows.pop())
        .ok_or(anyhow::anyhow!("No data found for video_id"))?;

    // Extract values from row
    let gcs_video_id = match &row.f[1].v {
        google_cloud_bigquery::http::tabledata::list::Value::String(s) => s.clone(),
        _ => return Err(anyhow::anyhow!("Invalid gcs_video_id")),
    };

    let is_nsfw = match &row.f[2].v {
        google_cloud_bigquery::http::tabledata::list::Value::String(b) => b == "true",
        _ => return Err(anyhow::anyhow!("Invalid is_nsfw")),
    };

    let nsfw_ec = match &row.f[3].v {
        google_cloud_bigquery::http::tabledata::list::Value::String(s) => s.clone(),
        _ => return Err(anyhow::anyhow!("Invalid nsfw_ec")),
    };

    let nsfw_gore = match &row.f[4].v {
        google_cloud_bigquery::http::tabledata::list::Value::String(s) => s.clone(),
        _ => return Err(anyhow::anyhow!("Invalid nsfw_gore")),
    };

    // Create row data for aggregated table
    let row_data = VideoNSFWDataV2 {
        video_id: video_id.clone(),
        gcs_video_id: gcs_video_id.clone(),
        is_nsfw,
        nsfw_ec: nsfw_ec.clone(),
        nsfw_gore: nsfw_gore.clone(),
        probability: nsfw_prob,
    };

    let row = Row {
        insert_id: None,
        json: row_data,
    };

    let request = InsertAllRequest {
        rows: vec![row],
        ..Default::default()
    };

    // Insert into aggregated table
    bigquery_client
        .tabledata()
        .insert(
            "hot-or-not-feed-intelligence",
            "yral_ds",
            "video_nsfw_agg",
            &request,
        )
        .await?;

    // Insert into video_embeddings_agg table
    // read embedding from bigquery hot-or-not-feed-intelligence.yral_ds.video_embeddings table
    // and push to bigquery hot-or-not-feed-intelligence.yral_ds.video_embeddings_agg table

    let embedding_query = format!(
        "SELECT * FROM `hot-or-not-feed-intelligence`.`yral_ds`.`video_embeddings` WHERE uri = '{gcs_video_id}'"
    );

    let embedding_request = QueryRequest {
        query: embedding_query,
        ..Default::default()
    };

    let embedding_result = bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &embedding_request)
        .await?;

    // in a loop convert each row to VideoEmbeddingAgg

    let mut video_embeddings = Vec::new();
    for row in embedding_result.rows.unwrap_or_default() {
        let embedding = VideoEmbeddingAgg {
            ml_generate_embedding_result: match &row.f[0].v {
                Value::Array(arr) => arr
                    .iter()
                    .filter_map(|cell| match &cell.v {
                        Value::String(s) => s.parse::<f64>().ok(),
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            },
            ml_generate_embedding_status: match &row.f[1].v {
                Value::String(s) => Some(s.clone()),
                _ => None,
            },
            ml_generate_embedding_start_sec: match &row.f[2].v {
                Value::String(s) => s.parse::<i64>().ok(),
                _ => None,
            },
            ml_generate_embedding_end_sec: match &row.f[3].v {
                Value::String(s) => s.parse::<i64>().ok(),
                _ => None,
            },
            uri: match &row.f[4].v {
                Value::String(s) => Some(s.clone()),
                _ => None,
            },
            generation: match &row.f[5].v {
                Value::String(s) => s.parse::<i64>().ok(),
                _ => None,
            },
            content_type: match &row.f[6].v {
                Value::String(s) => Some(s.clone()),
                _ => None,
            },
            size: match &row.f[7].v {
                Value::String(s) => s.parse::<i64>().ok(),
                _ => None,
            },
            md5_hash: match &row.f[8].v {
                Value::String(s) => Some(s.clone()),
                _ => None,
            },
            updated: match &row.f[9].v {
                Value::String(s) => Some(s.clone()),
                _ => None,
            },
            metadata: match &row.f[10].v {
                Value::Array(arr) => arr
                    .iter()
                    .filter_map(|cell| match &cell.v {
                        Value::Struct(tuple) => {
                            if tuple.f.len() >= 2 {
                                match (&tuple.f[0].v, &tuple.f[1].v) {
                                    (Value::String(key), Value::String(value)) => {
                                        Some(VideoEmbeddingMetadata {
                                            name: key.clone(),
                                            value: value.clone(),
                                        })
                                    }
                                    _ => None,
                                }
                            } else {
                                None
                            }
                        }
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            },
            is_nsfw: Some(is_nsfw),
            nsfw_ec: Some(nsfw_ec.clone()),
            nsfw_gore: Some(nsfw_gore.clone()),
            probability: Some(nsfw_prob),
            video_id: Some(video_id.clone()),
        };
        video_embeddings.push(embedding);
    }

    let rows = video_embeddings
        .into_iter()
        .map(|embedding| Row {
            insert_id: None,
            json: embedding,
        })
        .collect();

    // insert into bigquery
    let insert_request = InsertAllRequest {
        rows,
        ..Default::default()
    };

    let res = bigquery_client
        .tabledata()
        .insert(
            "hot-or-not-feed-intelligence",
            "yral_ds",
            "video_embeddings_agg",
            &insert_request,
        )
        .await?;

    log::info!("video_embeddings_agg insert response : {:?}", res);

    Ok(())
}
