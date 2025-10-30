use crate::app_state::AppState;
use crate::consts::DEDUP_INDEX_CANISTER_ID;
use axum::{extract::State, http::StatusCode, Json};
use google_cloud_bigquery::http::job::query::QueryRequest;
use google_cloud_bigquery::http::tabledata::list::Value;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::instrument;
use yral_canisters_client::dedup_index::{DedupIndex, SystemTime as CanisterSystemTime};

/// Request payload for backfilling dedup index
#[derive(Debug, Deserialize, Serialize)]
pub struct BackfillDedupIndexRequest {
    /// Optional limit on number of records to process (if None, fetch all)
    pub limit: Option<u32>,
}

/// Response for backfill operation
#[derive(Debug, Serialize)]
pub struct BackfillDedupIndexResponse {
    pub total_processed: usize,
    pub successful: usize,
    pub failed: usize,
}

/// Handler for backfilling dedup index from videohash_phash + video_unique tables
#[instrument(skip(state))]
pub async fn backfill_dedup_index_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BackfillDedupIndexRequest>,
) -> Result<Json<BackfillDedupIndexResponse>, StatusCode> {
    log::info!("Starting dedup index backfill with limit: {:?}", req.limit);

    // Build query with optional limit
    let query = if let Some(limit) = req.limit {
        format!(
            "SELECT DISTINCT t1.video_id, t1.phash AS videohash
             FROM `hot-or-not-feed-intelligence`.`yral_ds`.`videohash_phash` AS t1
             INNER JOIN `hot-or-not-feed-intelligence`.`yral_ds`.`video_unique` AS t2
             ON t1.video_id = t2.video_id
             LIMIT {}",
            limit
        )
    } else {
        "SELECT DISTINCT t1.video_id, t1.phash AS videohash
         FROM `hot-or-not-feed-intelligence`.`yral_ds`.`videohash_phash` AS t1
         INNER JOIN `hot-or-not-feed-intelligence`.`yral_ds`.`video_unique` AS t2
         ON t1.video_id = t2.video_id"
            .to_string()
    };

    log::debug!("BigQuery query: {}", query);

    // Execute query
    let request = QueryRequest {
        query,
        ..Default::default()
    };

    let response = state
        .bigquery_client
        .job()
        .query("hot-or-not-feed-intelligence", &request)
        .await
        .map_err(|e| {
            log::error!("BigQuery query failed: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // Parse results
    let rows = response.rows.as_ref().ok_or_else(|| {
        log::error!("No rows returned from BigQuery");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    log::info!("Retrieved {} rows from BigQuery", rows.len());

    let mut total_processed = 0;
    let mut successful = 0;
    let mut failed = 0;

    // Get current timestamp for all entries
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|e| {
        log::error!("Failed to get current timestamp: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let canister_time = CanisterSystemTime {
        nanos_since_epoch: now.subsec_nanos(),
        secs_since_epoch: now.as_secs(),
    };

    // Process each row
    for row in rows {
        total_processed += 1;

        // Extract video_id (first column) and phash (second column)
        let video_id = match &row.f.first() {
            Some(cell) => match &cell.v {
                Value::String(s) => s.clone(),
                _ => {
                    log::warn!("Skipping row {}: video_id is not a string", total_processed);
                    failed += 1;
                    continue;
                }
            },
            None => {
                log::warn!("Skipping row {}: missing video_id", total_processed);
                failed += 1;
                continue;
            }
        };

        let phash = match &row.f.get(1) {
            Some(cell) => match &cell.v {
                Value::String(s) => s.clone(),
                _ => {
                    log::warn!(
                        "Skipping row {} (video_id: {}): phash is not a string",
                        total_processed,
                        video_id
                    );
                    failed += 1;
                    continue;
                }
            },
            None => {
                log::warn!(
                    "Skipping row {} (video_id: {}): missing phash",
                    total_processed,
                    video_id
                );
                failed += 1;
                continue;
            }
        };

        // Add to dedup index
        let dedup_index = DedupIndex(*DEDUP_INDEX_CANISTER_ID, &state.agent);
        match dedup_index
            .add_video_to_index(video_id.clone(), (phash.clone(), canister_time.clone()))
            .await
        {
            Ok(_) => {
                log::debug!("Successfully added video {} to dedup index", video_id);
                successful += 1;
            }
            Err(e) => {
                log::error!("Failed to add video {} to dedup index: {}", video_id, e);
                failed += 1;
            }
        }
    }

    log::info!(
        "Backfill complete: processed={}, successful={}, failed={}",
        total_processed,
        successful,
        failed
    );

    Ok(Json(BackfillDedupIndexResponse {
        total_processed,
        successful,
        failed,
    }))
}
