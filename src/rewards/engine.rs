use crate::{
    app_state::AppState,
    events::types::{EventPayload, RewardEarnedPayload, VideoDurationWatchedPayloadV2},
    rewards::{
        analytics,
        btc_conversion::BtcConverter,
        config::{get_config, update_config as update_config_fn, RewardConfig},
        fraud_detection::{FraudCheck, FraudDetector},
        history::{HistoryTracker, RewardRecord, ViewRecord},
        user_verification::UserVerification,
        view_tracking::ViewTracker,
        wallet::WalletIntegration,
    },
    types::RedisPool,
};
use anyhow::{Context, Result};
use candid::Principal;
use chrono::Utc;
use std::sync::Arc;

#[derive(Clone)]
pub struct RewardEngine {
    redis_pool: RedisPool,
    view_tracker: ViewTracker,
    user_verification: UserVerification,
    history_tracker: HistoryTracker,
    fraud_detector: FraudDetector,
    btc_converter: BtcConverter,
    wallet: WalletIntegration,
}

impl RewardEngine {
    pub fn new(redis_pool: RedisPool, admin_agent: ic_agent::Agent) -> Self {
        let view_tracker = ViewTracker::new(redis_pool.clone());
        let user_verification = UserVerification::new(redis_pool.clone());
        let history_tracker = HistoryTracker::new(redis_pool.clone());
        let fraud_detector = FraudDetector::new(redis_pool.clone());
        let btc_converter = BtcConverter::new();
        let wallet = WalletIntegration::new(admin_agent);
        Self {
            redis_pool,
            view_tracker,
            user_verification,
            history_tracker,
            fraud_detector,
            btc_converter,
            wallet,
        }
    }

    pub fn with_config(
        redis_pool: RedisPool,
        admin_agent: ic_agent::Agent,
        config: RewardConfig,
    ) -> Self {
        let view_tracker = ViewTracker::new(redis_pool.clone());
        let user_verification = UserVerification::new(redis_pool.clone());
        let history_tracker = HistoryTracker::new(redis_pool.clone());
        let fraud_detector = FraudDetector::with_config(
            redis_pool.clone(),
            config.fraud_threshold,
            config.shadow_ban_duration,
        );
        let btc_converter = BtcConverter::new();
        let wallet = WalletIntegration::new(admin_agent);
        // Initialize config in Redis if provided
        tokio::spawn({
            let redis_pool = redis_pool.clone();
            async move {
                if let Err(e) = update_config_fn(&redis_pool, config).await {
                    log::error!("Failed to initialize config in Redis: {}", e);
                }
            }
        });

        Self {
            redis_pool,
            view_tracker,
            user_verification,
            history_tracker,
            fraud_detector,
            btc_converter,
            wallet,
        }
    }

    /// Initialize the reward engine (load Lua scripts, etc.)
    pub async fn initialize(&mut self) -> Result<()> {
        self.view_tracker.load_lua_scripts().await?;
        log::info!("Reward engine initialized");
        Ok(())
    }

    /// Process a video duration watched event
    pub async fn process_video_view(
        &self,
        event: VideoDurationWatchedPayloadV2,
        app_state: &Arc<AppState>,
    ) -> Result<()> {
        // if event.source.is_some() {
        log::info!(
            "Processing video view event: {:?} ; publisher: {:?} ; user_id: {:?}",
            event,
            event
                .publisher_user_id
                .unwrap_or_else(Principal::anonymous)
                .to_text(),
            event.user_id.to_text()
        );
        // }

        let config = get_config(&self.redis_pool)
            .await
            .map_err(|e| {
                log::error!("Failed to get config: {}", e);
                e
            })
            .unwrap_or_default();
        if event.absolute_watched < config.min_watch_duration {
            return Ok(());
        }

        let video_id = event.video_id.as_ref().context("Missing video_id")?;
        let publisher_user_id = event
            .publisher_user_id
            .as_ref()
            .context("Missing publisher_user_id")?;
        let is_logged_in = event.is_logged_in.unwrap_or(true);

        // For non-logged-in users, only track total_count_all and exit
        if !is_logged_in {
            // Track the view (only increments total_count_all)
            let _ = self
                .view_tracker
                .track_view(video_id, &event.user_id, false)
                .await?;
            return Ok(());
        }

        // 2. Verify user registration (with caching)
        if !self
            .user_verification
            .is_registered_user(event.user_id, app_state)
            .await?
        {
            let _ = self
                .view_tracker
                .track_view(video_id, &event.user_id, false)
                .await?;
            return Ok(());
        }

        // 2. Verify publisher registration (with caching)
        if !self
            .user_verification
            .is_registered_user(*publisher_user_id, app_state)
            .await?
        {
            let _ = self
                .view_tracker
                .track_view(video_id, &event.user_id, false)
                .await?;
            return Ok(());
        }

        // 3. Check if creator is shadow banned
        if self
            .fraud_detector
            .is_shadow_banned(publisher_user_id)
            .await?
        {
            log::debug!(
                "Creator {} is shadow banned, skipping reward",
                publisher_user_id
            );
            return Ok(());
        }

        // 4. ATOMIC: Count the view (config version is now checked in Lua script)
        let view_count = self
            .view_tracker
            .track_view(video_id, &event.user_id, true)
            .await?;

        if let Some(count) = view_count {
            log::info!(
                "New view recorded for video {} by user {}: total count = {}",
                video_id,
                event.user_id,
                count
            );

            // 5. NON-ATOMIC: Store history (fire and forget)
            self.history_tracker
                .record_view(ViewRecord {
                    user_id: event.user_id.to_string(),
                    video_id: video_id.clone(),
                    timestamp: Utc::now().timestamp(),
                    duration_watched: event.absolute_watched,
                    percentage_watched: event.percentage_watched,
                })
                .await;

            // 6. Check for milestone and track reward allocation
            let (view_count_reward_allocated, reward_amount_inr) =
                if count != 0 && count % config.view_milestone == 0 {
                    let milestone_number = count / config.view_milestone;
                    log::info!(
                        "Milestone {} reached for video {} (view count: {})",
                        milestone_number,
                        video_id,
                        count
                    );

                    // Process the reward
                    let milestone_result = self
                        .process_milestone(
                            video_id,
                            publisher_user_id,
                            count,
                            milestone_number,
                            &config,
                            app_state,
                        )
                        .await;

                    if milestone_result.is_ok() {
                        // 7. Fraud detection (async, non-blocking)
                        let fraud_check = self
                            .fraud_detector
                            .check_fraud_patterns(*publisher_user_id)
                            .await;
                        if fraud_check == FraudCheck::Suspicious {
                            log::warn!(
                                "Suspicious activity detected for creator {}",
                                publisher_user_id
                            );
                        }
                        (true, Some(config.reward_amount_inr))
                    } else {
                        log::error!(
                            "Failed to process milestone reward for video {} by user {}",
                            video_id,
                            publisher_user_id
                        );
                        (false, None)
                    }
                } else {
                    (false, None)
                };

            // 7. Send analytics event for unique view (after reward decision)
            let btc_video_view_tier = count / config.view_milestone;
            analytics::send_btc_video_viewed_event(
                analytics::BtcVideoViewedEventParams {
                    video_id,
                    publisher_user_id,
                    is_unique_view: true,
                    source: event.source.clone(),
                    client_type: event.client_type.clone(),
                    btc_video_view_count: count,
                    btc_video_view_tier,
                    share_count: event.share_count,
                    like_count: event.like_count,
                    view_count_reward_allocated,
                    reward_amount_inr,
                    user_id: &event.user_id,
                    is_logged_in: event.is_logged_in.unwrap_or(false),
                },
                app_state,
            )
            .await;
        } else {
            log::debug!(
                "Duplicate view for video {} by user {}",
                video_id,
                event.user_id
            );

            // Send analytics event for duplicate view
            // Need to fetch current view count for duplicate views
            if let Ok(current_count) = self.get_view_count(video_id).await {
                let btc_video_view_tier = current_count / config.view_milestone;
                analytics::send_btc_video_viewed_event(
                    analytics::BtcVideoViewedEventParams {
                        video_id,
                        publisher_user_id,
                        is_unique_view: false,
                        source: event.source.clone(),
                        client_type: event.client_type.clone(),
                        btc_video_view_count: current_count,
                        btc_video_view_tier,
                        share_count: event.share_count,
                        like_count: event.like_count,
                        view_count_reward_allocated: false,
                        reward_amount_inr: None,
                        user_id: &event.user_id,
                        is_logged_in: event.is_logged_in.unwrap_or(false),
                    },
                    app_state,
                )
                .await;
            }
        }

        Ok(())
    }

    /// Process a milestone reward
    async fn process_milestone(
        &self,
        video_id: &str,
        creator_id: &Principal,
        view_count: u64,
        milestone_number: u64,
        config: &RewardConfig,
        app_state: &Arc<AppState>,
    ) -> Result<()> {
        // Convert INR to BTC
        let btc_amount = self
            .btc_converter
            .convert_inr_to_btc(config.reward_amount_inr)
            .await?;

        log::info!(
            "Processing reward for creator {}: {} BTC (₹{}) for video {} milestone {}",
            creator_id,
            btc_amount,
            config.reward_amount_inr,
            video_id,
            milestone_number
        );

        // Create reward record
        let reward_record = RewardRecord {
            video_id: video_id.to_string(),
            milestone: milestone_number,
            reward_btc: btc_amount,
            reward_inr: config.reward_amount_inr,
            timestamp: Utc::now().timestamp(),
            tx_id: None,
            view_count,
        };

        // Store reward history (non-atomic)
        self.history_tracker
            .record_reward(creator_id, reward_record.clone())
            .await;

        // Queue BTC transaction
        match self
            .wallet
            .queue_btc_reward(
                *creator_id,
                btc_amount,
                config.reward_amount_inr,
                video_id,
                milestone_number,
            )
            .await
        {
            Ok(tx_id) => {
                log::info!(
                    "BTC reward queued successfully for creator {} with tx_id: {}",
                    creator_id,
                    tx_id
                );

                // Update milestone tracker
                self.view_tracker
                    .set_last_milestone(video_id, milestone_number)
                    .await?;

                // Send notification to creator
                self.send_reward_notification(
                    creator_id,
                    video_id,
                    milestone_number,
                    btc_amount,
                    config.reward_amount_inr,
                    view_count,
                    app_state,
                )
                .await;
            }
            Err(e) => {
                log::error!(
                    "Failed to queue BTC reward for creator {}: {}",
                    creator_id,
                    e
                );
                return Err(e);
            }
        }

        Ok(())
    }

    /// Send notification to creator about reward
    #[allow(clippy::too_many_arguments)]
    async fn send_reward_notification(
        &self,
        creator_id: &Principal,
        video_id: &str,
        milestone: u64,
        reward_btc: f64,
        reward_inr: f64,
        view_count: u64,
        app_state: &Arc<AppState>,
    ) {
        // Create the payload for the notification
        let payload = RewardEarnedPayload {
            creator_id: *creator_id,
            video_id: video_id.to_string(),
            milestone,
            reward_btc,
            reward_inr,
            view_count,
            timestamp: chrono::Utc::now().timestamp(),
            rewards_received_bs: true,
        };

        // Create the event and send notification
        let event = EventPayload::RewardEarned(payload);
        event.send_notification(app_state).await;

        log::info!(
            "Sent reward notification to creator {} for video {} (₹{:.2}, {:.8} BTC, milestone {})",
            creator_id,
            video_id,
            reward_inr,
            reward_btc,
            milestone
        );
    }

    /// Get current configuration
    pub async fn get_config(&self) -> RewardConfig {
        get_config(&self.redis_pool).await.unwrap_or_else(|e| {
            log::error!("Failed to get config: {}", e);
            RewardConfig::default()
        })
    }

    /// Update configuration
    pub async fn update_config(&self, new_config: RewardConfig) -> Result<()> {
        update_config_fn(&self.redis_pool, new_config).await?;
        log::info!("Reward configuration updated");
        Ok(())
    }

    /// Get view count for a video
    pub async fn get_view_count(&self, video_id: &str) -> Result<u64> {
        self.view_tracker.get_view_count(video_id).await
    }

    /// Get last milestone for a video
    pub async fn get_last_milestone(&self, video_id: &str) -> Result<u64> {
        self.view_tracker.get_last_milestone(video_id).await
    }

    /// Get comprehensive stats for a video (single Redis call)
    pub async fn get_video_stats(&self, video_id: &str) -> Result<crate::rewards::api::VideoStats> {
        let (count, total_count_loggedin, total_count_all, last_milestone) =
            self.view_tracker.get_all_video_stats(video_id).await?;

        Ok(crate::rewards::api::VideoStats {
            count,
            total_count_loggedin,
            total_count_all,
            last_milestone,
        })
    }

    /// Get stats for multiple videos using Redis pipelining
    pub async fn get_bulk_video_stats(
        &self,
        video_ids: &[String],
    ) -> Result<std::collections::HashMap<String, crate::rewards::api::VideoStats>> {
        let stats_map = self.view_tracker.get_bulk_video_stats(video_ids).await?;

        Ok(stats_map
            .into_iter()
            .map(
                |(video_id, (count, total_count_loggedin, total_count_all, last_milestone))| {
                    (
                        video_id,
                        crate::rewards::api::VideoStats {
                            count,
                            total_count_loggedin,
                            total_count_all,
                            last_milestone,
                        },
                    )
                },
            )
            .collect())
    }
}
