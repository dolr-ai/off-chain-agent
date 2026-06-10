use std::sync::Arc;

use anyhow::{Context, Result};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use crate::yral_auth::dragonfly::DragonflyPool;

const SCHEDULED_KEY: &str = "offchain:video_processing:scheduled";
const JOB_KEY_PREFIX: &str = "offchain:video_processing:job";
const LOCK_KEY_PREFIX: &str = "offchain:video_processing:lock";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VideoProcessingPhase {
    DedupPending,
    NsfwEnqueuePending,
    NsfwPollPending,
    Completed,
    TerminalFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoProcessingJob {
    pub video_id: String,
    pub publisher_user_id: String,
    pub post_id: String,
    pub canister_id: Option<String>,
    pub source_video_uri: String,
    pub source_object_version: String,
    pub upload_event_id: Option<String>,
    pub upload_created_at: Option<String>,
    pub policy_version: String,
    pub nsfw_job_id: String,
    pub trace_id: String,
    pub phase: VideoProcessingPhase,
    #[serde(default)]
    pub dedup_attempts: u32,
    #[serde(default)]
    pub nsfw_enqueue_attempts: u32,
    #[serde(default)]
    pub nsfw_poll_attempts: u32,
    #[serde(default)]
    pub nsfw_job_retry_attempts: u32,
    pub next_run_at: i64,
    pub created_at: String,
    pub updated_at: String,
    pub last_error: Option<String>,
    pub last_nsfw_status: Option<String>,
}

impl VideoProcessingJob {
    pub fn new(
        video_id: String,
        publisher_user_id: String,
        post_id: String,
        canister_id: Option<String>,
        source_video_uri: String,
        dedup_delay_seconds: i64,
    ) -> Self {
        let now = chrono::Utc::now();
        let policy_version = "nsfw_policy_v1".to_string();
        let source_object_version = String::new();
        let nsfw_job_id = nsfw_job_id(&video_id, &policy_version, &source_object_version);

        Self {
            trace_id: format!("offchain:{video_id}"),
            video_id,
            publisher_user_id,
            post_id,
            canister_id,
            source_video_uri,
            source_object_version,
            upload_event_id: None,
            upload_created_at: Some(now.to_rfc3339()),
            policy_version,
            nsfw_job_id,
            phase: VideoProcessingPhase::DedupPending,
            dedup_attempts: 0,
            nsfw_enqueue_attempts: 0,
            nsfw_poll_attempts: 0,
            nsfw_job_retry_attempts: 0,
            next_run_at: now.timestamp() + dedup_delay_seconds,
            created_at: now.to_rfc3339(),
            updated_at: now.to_rfc3339(),
            last_error: None,
            last_nsfw_status: None,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.phase,
            VideoProcessingPhase::Completed | VideoProcessingPhase::TerminalFailed
        )
    }

    pub fn touch(&mut self) {
        self.updated_at = chrono::Utc::now().to_rfc3339();
    }
}

pub async fn enqueue_video_processing_job(
    pool: &Arc<DragonflyPool>,
    job: VideoProcessingJob,
) -> Result<()> {
    let mut conn = pool.get().await?;
    let key = job_key(&job.video_id);

    let existing: Option<String> = conn.get(&key).await?;
    if let Some(existing) = existing {
        let existing_job: VideoProcessingJob =
            serde_json::from_str(&existing).context("failed to decode existing video job")?;
        if !existing_job.is_terminal() {
            let _: () = conn
                .zadd(
                    SCHEDULED_KEY,
                    &existing_job.video_id,
                    existing_job.next_run_at,
                )
                .await?;
        }
        log::info!(
            "Video processing job already exists for {} in phase {:?}",
            existing_job.video_id,
            existing_job.phase
        );
        return Ok(());
    }

    let payload = serde_json::to_string(&job)?;
    let _: () = conn.set(&key, payload).await?;
    let _: () = conn
        .zadd(SCHEDULED_KEY, &job.video_id, job.next_run_at)
        .await?;

    log::info!(
        "Queued video processing job for {} with due timestamp {}",
        job.video_id,
        job.next_run_at
    );

    Ok(())
}

pub async fn load_job(
    pool: &Arc<DragonflyPool>,
    video_id: &str,
) -> Result<Option<VideoProcessingJob>> {
    let mut conn = pool.get().await?;
    let payload: Option<String> = conn.get(job_key(video_id)).await?;
    payload
        .map(|payload| serde_json::from_str(&payload).context("failed to decode video job"))
        .transpose()
}

pub async fn save_and_schedule(
    pool: &Arc<DragonflyPool>,
    job: &mut VideoProcessingJob,
) -> Result<()> {
    job.touch();
    let mut conn = pool.get().await?;
    let payload = serde_json::to_string(job)?;
    let _: () = conn.set(job_key(&job.video_id), payload).await?;
    let _: () = conn
        .zadd(SCHEDULED_KEY, &job.video_id, job.next_run_at)
        .await?;
    Ok(())
}

pub async fn save_and_unschedule(
    pool: &Arc<DragonflyPool>,
    job: &mut VideoProcessingJob,
) -> Result<()> {
    job.touch();
    let mut conn = pool.get().await?;
    let payload = serde_json::to_string(job)?;
    let _: () = conn.set(job_key(&job.video_id), payload).await?;
    let _: () = conn.zrem(SCHEDULED_KEY, &job.video_id).await?;
    Ok(())
}

pub async fn mark_nsfw_enqueue_pending(pool: &Arc<DragonflyPool>, video_id: &str) -> Result<()> {
    let mut job = load_job(pool, video_id)
        .await?
        .with_context(|| format!("video processing job not found for {video_id}"))?;
    job.phase = VideoProcessingPhase::NsfwEnqueuePending;
    job.next_run_at = chrono::Utc::now().timestamp();
    job.last_error = None;
    save_and_schedule(pool, &mut job).await
}

pub async fn schedule_nsfw_handoff_job(
    pool: &Arc<DragonflyPool>,
    mut job: VideoProcessingJob,
) -> Result<()> {
    if let Some(mut existing_job) = load_job(pool, &job.video_id).await? {
        if existing_job.is_terminal() {
            log::info!(
                "Skipping NSFW handoff schedule for terminal video processing job {} in phase {:?}",
                existing_job.video_id,
                existing_job.phase
            );
            return Ok(());
        }

        existing_job.phase = VideoProcessingPhase::NsfwEnqueuePending;
        existing_job.next_run_at = chrono::Utc::now().timestamp();
        existing_job.last_error = None;
        return save_and_schedule(pool, &mut existing_job).await;
    }

    job.phase = VideoProcessingPhase::NsfwEnqueuePending;
    job.next_run_at = chrono::Utc::now().timestamp();
    job.last_error = None;
    save_and_schedule(pool, &mut job).await
}

pub async fn fetch_due_video_ids(
    pool: &Arc<DragonflyPool>,
    now_timestamp: i64,
    limit: usize,
) -> Result<Vec<String>> {
    let mut conn = pool.get().await?;
    let ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg(SCHEDULED_KEY)
        .arg("-inf")
        .arg(now_timestamp)
        .arg("LIMIT")
        .arg(0)
        .arg(limit)
        .query_async(&mut conn)
        .await?;
    Ok(ids)
}

pub async fn remove_from_schedule(pool: &Arc<DragonflyPool>, video_id: &str) -> Result<()> {
    let mut conn = pool.get().await?;
    let _: () = conn.zrem(SCHEDULED_KEY, video_id).await?;
    Ok(())
}

pub async fn try_acquire_lock(
    pool: &Arc<DragonflyPool>,
    video_id: &str,
    owner: &str,
    ttl_ms: usize,
) -> Result<bool> {
    let mut conn = pool.get().await?;
    let result: Option<String> = redis::cmd("SET")
        .arg(lock_key(video_id))
        .arg(owner)
        .arg("NX")
        .arg("PX")
        .arg(ttl_ms)
        .query_async(&mut conn)
        .await?;
    Ok(result.as_deref() == Some("OK"))
}

pub async fn release_lock(pool: &Arc<DragonflyPool>, video_id: &str, owner: &str) -> Result<()> {
    let mut conn = pool.get().await?;
    let key = lock_key(video_id);
    let current_owner: Option<String> = conn.get(&key).await?;
    if current_owner.as_deref() == Some(owner) {
        let _: () = conn.del(key).await?;
    }
    Ok(())
}

fn job_key(video_id: &str) -> String {
    format!("{JOB_KEY_PREFIX}:{video_id}")
}

fn lock_key(video_id: &str) -> String {
    format!("{LOCK_KEY_PREFIX}:{video_id}")
}

pub fn nsfw_job_id(video_id: &str, policy_version: &str, source_object_version: &str) -> String {
    format!("nsfw:{video_id}:{policy_version}:{source_object_version}")
}
