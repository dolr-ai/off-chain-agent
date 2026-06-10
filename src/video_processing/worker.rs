use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use uuid::Uuid;

use crate::{
    app_state::AppState,
    consts::get_storj_video_url,
    pipeline::Step,
    qstash::{self, duplicate::VideoPublisherDataV2},
    setup_context,
    video_processing::{
        nsfw_api::{NsfwApiClient, NsfwApiError, VideoDetectRequest},
        queue::{
            self, fetch_due_video_ids, load_job, release_lock, remove_from_schedule,
            save_and_schedule, save_and_unschedule, try_acquire_lock, VideoProcessingJob,
            VideoProcessingPhase,
        },
    },
};

#[derive(Clone)]
struct WorkerConfig {
    batch_size: usize,
    parallelism: usize,
    tick_seconds: u64,
    lock_ttl_ms: usize,
    max_dedup_attempts: u32,
    max_nsfw_enqueue_attempts: u32,
    max_nsfw_poll_attempts: u32,
    max_nsfw_job_retry_attempts: u32,
    nsfw_poll_delay_seconds: i64,
}

impl WorkerConfig {
    fn from_env() -> Self {
        Self {
            batch_size: env_parse("VIDEO_PROCESSING_WORKER_BATCH_SIZE", 20),
            parallelism: env_parse("VIDEO_PROCESSING_WORKER_PARALLELISM", 5),
            tick_seconds: env_parse("VIDEO_PROCESSING_WORKER_TICK_SECONDS", 5),
            lock_ttl_ms: env_parse("VIDEO_PROCESSING_WORKER_LOCK_TTL_MS", 15 * 60 * 1000),
            max_dedup_attempts: env_parse("VIDEO_PROCESSING_MAX_DEDUP_ATTEMPTS", 5),
            max_nsfw_enqueue_attempts: env_parse("VIDEO_PROCESSING_MAX_NSFW_ENQUEUE_ATTEMPTS", 20),
            max_nsfw_poll_attempts: env_parse("VIDEO_PROCESSING_MAX_NSFW_POLL_ATTEMPTS", 180),
            max_nsfw_job_retry_attempts: env_parse("VIDEO_PROCESSING_MAX_NSFW_JOB_RETRIES", 3),
            nsfw_poll_delay_seconds: env_parse("VIDEO_PROCESSING_NSFW_POLL_DELAY_SECONDS", 60),
        }
    }
}

pub fn spawn_worker(state: Arc<AppState>) -> Result<()> {
    if !env_parse("VIDEO_PROCESSING_WORKER_ENABLED", true) {
        log::info!("Video processing worker disabled by VIDEO_PROCESSING_WORKER_ENABLED=false");
        return Ok(());
    }

    let config = WorkerConfig::from_env();
    // Build the client before spawning so missing signing config fails startup, not the first background tick.
    let nsfw_client = NsfwApiClient::from_env()?;

    tokio::spawn(async move {
        if let Err(err) = run_worker(state, nsfw_client, config).await {
            log::error!("Video processing worker stopped: {err:?}");
            sentry_anyhow::capture_anyhow(&err);
        }
    });

    Ok(())
}

pub fn dedup_delay_seconds_from_env() -> i64 {
    env_parse("VIDEO_PROCESSING_DEDUP_DELAY_SECONDS", 600)
}

async fn run_worker(
    state: Arc<AppState>,
    nsfw_client: NsfwApiClient,
    config: WorkerConfig,
) -> Result<()> {
    log::info!(
        "Starting video processing worker: batch_size={}, parallelism={}, tick_seconds={}",
        config.batch_size,
        config.parallelism,
        config.tick_seconds
    );

    loop {
        if let Err(err) = process_due_jobs(state.clone(), nsfw_client.clone(), config.clone()).await
        {
            log::error!("Video processing worker tick failed: {err:?}");
            sentry_anyhow::capture_anyhow(&err);
        }
        tokio::time::sleep(Duration::from_secs(config.tick_seconds)).await;
    }
}

async fn process_due_jobs(
    state: Arc<AppState>,
    nsfw_client: NsfwApiClient,
    config: WorkerConfig,
) -> Result<()> {
    let due_ids = fetch_due_video_ids(
        &state.yral_redis_store_dragonfly,
        chrono::Utc::now().timestamp(),
        config.batch_size,
    )
    .await?;

    futures::stream::iter(due_ids)
        .for_each_concurrent(config.parallelism, |video_id| {
            let state = state.clone();
            let nsfw_client = nsfw_client.clone();
            let config = config.clone();

            async move {
                if let Err(err) =
                    process_one_job(state, nsfw_client, config, video_id.clone()).await
                {
                    log::error!("Video processing job failed for {video_id}: {err:?}");
                    sentry_anyhow::capture_anyhow(&err);
                }
            }
        })
        .await;

    Ok(())
}

async fn process_one_job(
    state: Arc<AppState>,
    nsfw_client: NsfwApiClient,
    config: WorkerConfig,
    video_id: String,
) -> Result<()> {
    let lock_owner = Uuid::new_v4().to_string();
    let lock_acquired = try_acquire_lock(
        &state.yral_redis_store_dragonfly,
        &video_id,
        &lock_owner,
        config.lock_ttl_ms,
    )
    .await?;

    if !lock_acquired {
        return Ok(());
    }

    let result = process_locked_job(state.clone(), nsfw_client, config, &video_id).await;
    if let Err(err) = release_lock(&state.yral_redis_store_dragonfly, &video_id, &lock_owner).await
    {
        log::warn!("Failed to release video processing lock for {video_id}: {err:?}");
    }

    result
}

async fn process_locked_job(
    state: Arc<AppState>,
    nsfw_client: NsfwApiClient,
    config: WorkerConfig,
    video_id: &str,
) -> Result<()> {
    let Some(job) = load_job(&state.yral_redis_store_dragonfly, video_id).await? else {
        remove_from_schedule(&state.yral_redis_store_dragonfly, video_id).await?;
        return Ok(());
    };

    if job.next_run_at > chrono::Utc::now().timestamp() {
        return Ok(());
    }

    match job.phase {
        VideoProcessingPhase::DedupPending => {
            process_dedup_pending(state, job, config).await?;
        }
        VideoProcessingPhase::NsfwEnqueuePending => {
            process_nsfw_enqueue_pending(state, nsfw_client, job, config).await?;
        }
        VideoProcessingPhase::NsfwPollPending => {
            process_nsfw_poll_pending(state, nsfw_client, job, config).await?;
        }
        VideoProcessingPhase::Completed | VideoProcessingPhase::TerminalFailed => {
            remove_from_schedule(&state.yral_redis_store_dragonfly, video_id).await?;
        }
    }

    Ok(())
}

async fn process_dedup_pending(
    state: Arc<AppState>,
    mut job: VideoProcessingJob,
    config: WorkerConfig,
) -> Result<()> {
    setup_context!(&job.video_id, Step::Deduplication, {
        "source": "video_processing_worker",
        "job": &job,
    });

    // Dedup may return Ok without calling the continuation when AI approval blocks the video.
    let callback_called = Arc::new(AtomicBool::new(false));
    let callback_called_for_dedup = callback_called.clone();
    let callback_pool = state.yral_redis_store_dragonfly.clone();

    let publisher_data = VideoPublisherDataV2 {
        publisher_principal: job.publisher_user_id.clone(),
        post_id: job.post_id.clone(),
    };

    let dedup_result = qstash::duplicate::VideoHashDuplication
        .process_video_deduplication_v2(
            &state.bigquery_client,
            &state.milvus_client,
            &state.rewards_module.dragonfly_pool,
            &state.kvrocks_client,
            &job.video_id,
            &job.source_video_uri,
            publisher_data,
            30,
            move |video_id, _post_id, _timestamp, _publisher_user_id| {
                let video_id = video_id.to_string();
                let callback_pool = callback_pool.clone();
                let callback_called_for_dedup = callback_called_for_dedup.clone();

                Box::pin(async move {
                    // Replace the old upload_video_gcs callback with a durable phase transition.
                    callback_called_for_dedup.store(true, Ordering::SeqCst);
                    queue::mark_nsfw_enqueue_pending(&callback_pool, &video_id).await
                })
            },
        )
        .await;

    match dedup_result {
        Ok(()) if callback_called.load(Ordering::SeqCst) => {
            log::info!(
                "Dedup completed for {}; NSFW enqueue phase scheduled",
                job.video_id
            );
        }
        Ok(()) => {
            job.phase = VideoProcessingPhase::Completed;
            job.last_nsfw_status = Some("dedup_completed_without_handoff".to_string());
            job.last_error = None;
            save_and_unschedule(&state.yral_redis_store_dragonfly, &mut job).await?;
            log::info!(
                "Dedup completed for {} without NSFW handoff; marking job completed",
                job.video_id
            );
        }
        Err(err) => {
            let error_message = format!("{err:?}");
            log::error!("Dedup failed for {}: {error_message}", job.video_id);
            retry_or_terminal(
                &state,
                &mut job,
                config.max_dedup_attempts,
                RetryCounter::Dedup,
                "dedup failed",
                error_message,
            )
            .await?;
        }
    }

    Ok(())
}

async fn process_nsfw_enqueue_pending(
    state: Arc<AppState>,
    nsfw_client: NsfwApiClient,
    mut job: VideoProcessingJob,
    config: WorkerConfig,
) -> Result<()> {
    setup_context!(&job.video_id, Step::NsfwApiHandoff, {
        "source": "video_processing_worker",
        "job": &job,
    });

    let request = VideoDetectRequest {
        job_id: job.nsfw_job_id.clone(),
        video_id: job.video_id.clone(),
        publisher_user_id: job.publisher_user_id.clone(),
        source_video_uri: job.source_video_uri.clone(),
        post_id: Some(job.post_id.clone()),
        canister_id: job.canister_id.clone(),
        source_object_version: job.source_object_version.clone(),
        upload_event_id: job.upload_event_id.clone(),
        upload_created_at: job.upload_created_at.clone(),
        policy_version: job.policy_version.clone(),
        trace_id: Some(job.trace_id.clone()),
    };

    match nsfw_client.detect_video(&request).await {
        Ok(response) => {
            log::info!(
                "NSFW detect accepted for {}: job_id={}, status={}, trace_id={:?}, response_video_id={}",
                job.video_id,
                response.job_id,
                response.status,
                response.trace_id,
                response.video_id
            );
            job.nsfw_job_id = response.job_id;
            job.last_nsfw_status = Some(response.status.clone());
            job.last_error = None;
            apply_nsfw_status_after_enqueue(&state, &mut job, &response.status, config).await?;
        }
        Err(err) if err.is_retryable() => {
            retry_or_terminal(
                &state,
                &mut job,
                config.max_nsfw_enqueue_attempts,
                RetryCounter::NsfwEnqueue,
                "NSFW detect enqueue failed",
                err.to_string(),
            )
            .await?;
        }
        Err(err) => {
            mark_terminal_failed(
                &state,
                &mut job,
                format!("NSFW detect terminal error: {err}"),
            )
            .await?;
        }
    }

    Ok(())
}

async fn process_nsfw_poll_pending(
    state: Arc<AppState>,
    nsfw_client: NsfwApiClient,
    mut job: VideoProcessingJob,
    config: WorkerConfig,
) -> Result<()> {
    setup_context!(&job.video_id, Step::NsfwApiStatusPoll, {
        "source": "video_processing_worker",
        "job": &job,
    });

    match nsfw_client.video_status(&job.video_id).await {
        Ok(response) => {
            log::info!(
                "NSFW status for {}: job_id={}, status={}, attempts={}, trace_id={:?}, last_error_code={:?}, last_error_message={:?}, final_result_present={}",
                job.video_id,
                response.job_id,
                response.status,
                response.attempts,
                response.trace_id,
                response.last_error_code,
                response.last_error_message,
                response.final_result.is_some()
            );

            // Status lookup is by video_id, so after a retry it can briefly return the previous NSFW job.
            if response.job_id != job.nsfw_job_id {
                let expected_job_id = job.nsfw_job_id.clone();
                retry_or_terminal(
                    &state,
                    &mut job,
                    config.max_nsfw_poll_attempts,
                    RetryCounter::NsfwPoll,
                    "NSFW status returned stale job_id",
                    format!(
                        "expected={expected_job_id}; got={}; status={}",
                        response.job_id, response.status
                    ),
                )
                .await?;
                return Ok(());
            }

            job.nsfw_job_id = response.job_id;
            job.last_nsfw_status = Some(response.status.clone());
            job.last_error = response
                .last_error_message
                .or(response.last_error_code)
                .or_else(|| Some(format!("NSFW status: {}", response.status)));
            apply_nsfw_status_after_poll(&state, &mut job, &response.status, config).await?;
        }
        Err(err) if matches!(err, NsfwApiError::Retryable(_)) => {
            retry_or_terminal(
                &state,
                &mut job,
                config.max_nsfw_poll_attempts,
                RetryCounter::NsfwPoll,
                "NSFW status poll failed",
                err.to_string(),
            )
            .await?;
        }
        Err(err) => {
            mark_terminal_failed(
                &state,
                &mut job,
                format!("NSFW status terminal error: {err}"),
            )
            .await?;
        }
    }

    Ok(())
}

async fn apply_nsfw_status_after_enqueue(
    state: &AppState,
    job: &mut VideoProcessingJob,
    status: &str,
    config: WorkerConfig,
) -> Result<()> {
    match status {
        "classified" => mark_completed(state, job).await,
        "queued" | "processing" | "failed_retryable" => {
            job.phase = VideoProcessingPhase::NsfwPollPending;
            job.next_run_at = chrono::Utc::now().timestamp() + config.nsfw_poll_delay_seconds;
            save_and_schedule(&state.yral_redis_store_dragonfly, job).await
        }
        "failed_terminal" | "superseded" => {
            mark_terminal_failed(
                state,
                job,
                format!("NSFW detect returned terminal status {status}"),
            )
            .await
        }
        _ => {
            job.phase = VideoProcessingPhase::NsfwPollPending;
            job.next_run_at = chrono::Utc::now().timestamp() + config.nsfw_poll_delay_seconds;
            job.last_error = Some(format!("unknown NSFW detect status {status}"));
            save_and_schedule(&state.yral_redis_store_dragonfly, job).await
        }
    }
}

async fn apply_nsfw_status_after_poll(
    state: &AppState,
    job: &mut VideoProcessingJob,
    status: &str,
    config: WorkerConfig,
) -> Result<()> {
    match status {
        "classified" => mark_completed(state, job).await,
        "queued" | "processing" => {
            job.nsfw_poll_attempts += 1;
            if job.nsfw_poll_attempts >= config.max_nsfw_poll_attempts {
                mark_terminal_failed(
                    state,
                    job,
                    format!("NSFW status stayed {status} after max poll attempts"),
                )
                .await
            } else {
                job.phase = VideoProcessingPhase::NsfwPollPending;
                job.next_run_at = chrono::Utc::now().timestamp() + config.nsfw_poll_delay_seconds;
                save_and_schedule(&state.yral_redis_store_dragonfly, job).await
            }
        }
        "failed_retryable" => reenqueue_failed_retryable(state, job, config).await,
        "failed_terminal" | "superseded" => {
            mark_terminal_failed(state, job, format!("NSFW status is terminal: {status}")).await
        }
        _ => {
            job.nsfw_poll_attempts += 1;
            if job.nsfw_poll_attempts >= config.max_nsfw_poll_attempts {
                mark_terminal_failed(
                    state,
                    job,
                    format!("unknown NSFW status {status} after max poll attempts"),
                )
                .await
            } else {
                job.phase = VideoProcessingPhase::NsfwPollPending;
                job.next_run_at = chrono::Utc::now().timestamp() + config.nsfw_poll_delay_seconds;
                job.last_error = Some(format!("unknown NSFW status {status}"));
                save_and_schedule(&state.yral_redis_store_dragonfly, job).await
            }
        }
    }
}

async fn reenqueue_failed_retryable(
    state: &AppState,
    job: &mut VideoProcessingJob,
    config: WorkerConfig,
) -> Result<()> {
    job.nsfw_job_retry_attempts += 1;
    if job.nsfw_job_retry_attempts > config.max_nsfw_job_retry_attempts {
        return mark_terminal_failed(
            state,
            job,
            format!(
                "NSFW job stayed failed_retryable after {} job retry attempts",
                job.nsfw_job_retry_attempts
            ),
        )
        .await;
    }

    job.phase = VideoProcessingPhase::NsfwEnqueuePending;
    // NSFW queue idempotency includes source_object_version; bump it to request a bounded retry job.
    job.source_object_version = format!("retry-{}", job.nsfw_job_retry_attempts);
    job.nsfw_job_id = queue::nsfw_job_id(
        &job.video_id,
        &job.policy_version,
        &job.source_object_version,
    );
    job.nsfw_enqueue_attempts = 0;
    job.nsfw_poll_attempts = 0;
    job.last_error = Some(format!(
        "NSFW status failed_retryable; scheduling job retry {}",
        job.nsfw_job_retry_attempts
    ));
    job.next_run_at =
        chrono::Utc::now().timestamp() + retry_delay_seconds(job.nsfw_job_retry_attempts);
    save_and_schedule(&state.yral_redis_store_dragonfly, job).await
}

async fn retry_or_terminal(
    state: &AppState,
    job: &mut VideoProcessingJob,
    max_attempts: u32,
    counter: RetryCounter,
    context: &str,
    error_message: String,
) -> Result<()> {
    let attempts = match counter {
        RetryCounter::Dedup => {
            job.dedup_attempts += 1;
            job.dedup_attempts
        }
        RetryCounter::NsfwEnqueue => {
            job.nsfw_enqueue_attempts += 1;
            job.nsfw_enqueue_attempts
        }
        RetryCounter::NsfwPoll => {
            job.nsfw_poll_attempts += 1;
            job.nsfw_poll_attempts
        }
    };

    if attempts >= max_attempts {
        mark_terminal_failed(
            state,
            job,
            format!("{context}; attempts={attempts}; error={error_message}"),
        )
        .await
    } else {
        job.last_error = Some(format!(
            "{context}; attempts={attempts}; error={error_message}"
        ));
        job.next_run_at = chrono::Utc::now().timestamp() + retry_delay_seconds(attempts);
        save_and_schedule(&state.yral_redis_store_dragonfly, job).await
    }
}

async fn mark_completed(state: &AppState, job: &mut VideoProcessingJob) -> Result<()> {
    job.phase = VideoProcessingPhase::Completed;
    job.last_error = None;
    save_and_unschedule(&state.yral_redis_store_dragonfly, job).await?;
    log::info!("Video processing completed for {}", job.video_id);
    Ok(())
}

async fn mark_terminal_failed(
    state: &AppState,
    job: &mut VideoProcessingJob,
    error_message: String,
) -> Result<()> {
    job.phase = VideoProcessingPhase::TerminalFailed;
    job.last_error = Some(error_message.clone());
    save_and_unschedule(&state.yral_redis_store_dragonfly, job).await?;
    log::error!(
        "Video processing reached terminal failure for {}: {}",
        job.video_id,
        error_message
    );
    sentry_anyhow::capture_anyhow(&anyhow::anyhow!(error_message));
    Ok(())
}

fn retry_delay_seconds(attempts: u32) -> i64 {
    let exponent = attempts.saturating_sub(1).min(6);
    let delay = 30_i64.saturating_mul(2_i64.saturating_pow(exponent));
    delay.min(30 * 60)
}

enum RetryCounter {
    Dedup,
    NsfwEnqueue,
    NsfwPoll,
}

fn env_parse<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<T>().ok())
        .unwrap_or(default)
}

pub fn new_upload_job(
    video_id: String,
    publisher_user_id: String,
    post_id: String,
    canister_id: Option<String>,
) -> VideoProcessingJob {
    let source_video_uri = get_storj_video_url(&publisher_user_id, &video_id, false);
    VideoProcessingJob::new(
        video_id,
        publisher_user_id,
        post_id,
        canister_id,
        source_video_uri,
        dedup_delay_seconds_from_env(),
    )
}
