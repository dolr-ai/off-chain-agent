use crate::{
    app_state::AppState,
    events::types::{
        EventPayload, RewardEarnedPayload, VideoDurationWatchedPayloadV2, VideoStartedPayload,
    },
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
    yral_auth::dragonfly::DragonflyPool,
};
use anyhow::{Context, Result};
use candid::Principal;
use chrono::Utc;
use std::sync::Arc;

#[derive(Clone)]
pub struct RewardEngine {
    dragonfly_pool: Arc<DragonflyPool>,
    view_tracker: ViewTracker,
    user_verification: UserVerification,
    history_tracker: HistoryTracker,
    fraud_detector: FraudDetector,
    btc_converter: BtcConverter,
    wallet: WalletIntegration,
}

impl RewardEngine {
    pub fn new(dragonfly_pool: Arc<DragonflyPool>, admin_agent: ic_agent::Agent) -> Self {
        let view_tracker = ViewTracker::new(dragonfly_pool.clone());
        let user_verification = UserVerification::new(dragonfly_pool.clone());
        let history_tracker = HistoryTracker::new(dragonfly_pool.clone());
        let fraud_detector = FraudDetector::new(dragonfly_pool.clone());
        let btc_converter = BtcConverter::new();
        let wallet = WalletIntegration::new(admin_agent);
        Self {
            dragonfly_pool,
            view_tracker,
            user_verification,
            history_tracker,
            fraud_detector,
            btc_converter,
            wallet,
        }
    }

    pub fn with_config(
        dragonfly_pool: Arc<DragonflyPool>,
        admin_agent: ic_agent::Agent,
        config: RewardConfig,
    ) -> Self {
        let view_tracker = ViewTracker::new(dragonfly_pool.clone());
        let user_verification = UserVerification::new(dragonfly_pool.clone());
        let history_tracker = HistoryTracker::new(dragonfly_pool.clone());
        let fraud_detector = FraudDetector::with_config(
            dragonfly_pool.clone(),
            config.fraud_threshold,
            config.shadow_ban_duration,
        );
        let btc_converter = BtcConverter::new();
        let wallet = WalletIntegration::new(admin_agent);
        // Initialize config in Dragonfly if provided
        tokio::spawn({
            let dragonfly_pool = dragonfly_pool.clone();
            async move {
                if let Err(e) = update_config_fn(&dragonfly_pool, config).await {
                    log::error!("Failed to initialize config in Dragonfly: {}", e);
                }
            }
        });

        Self {
            dragonfly_pool,
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

        let config = get_config(&self.dragonfly_pool)
            .await
            .map_err(|e| {
                log::error!("Failed to get config: {}", e);
                e
            })
            .unwrap_or_default();
        let video_id = event.video_id.as_ref().context("Missing video_id")?;
        if event.absolute_watched < config.min_watch_duration {
            if event.client_type == Some("web".to_string()) {
                let _ = self
                    .view_tracker
                    .track_view(video_id, &event.user_id, false)
                    .await?;
            }

            return Ok(());
        }

        let publisher_user_id = event
            .publisher_user_id
            .as_ref()
            .context("Missing publisher_user_id")?;

        // Skip self-views (creator viewing their own video)
        if event.user_id == *publisher_user_id {
            log::debug!(
                "Self-view detected for video {} by user {}, skipping reward",
                video_id,
                event.user_id
            );
            return Ok(());
        }

        let is_logged_in = event.is_logged_in.unwrap_or(true);

        // Determine if we should track views based on client_type and absolute_watched
        // Only track if client_type is "web" OR (not "web" AND absolute_watched is 3.0-4.5)
        let should_track = match event.client_type.as_deref() {
            Some("web") => true,
            _ => event.absolute_watched >= 3.0 && event.absolute_watched <= 4.5,
        };

        // For non-logged-in users, only track total_count_all and exit
        if !is_logged_in {
            // Track the view (only increments total_count_all)
            if should_track {
                let _ = self
                    .view_tracker
                    .track_view(video_id, &event.user_id, false)
                    .await?;
            }
            return Ok(());
        }

        // 2. Verify user registration (with caching)
        if !self
            .user_verification
            .is_registered_user(event.user_id, app_state)
            .await?
        {
            if should_track {
                let _ = self
                    .view_tracker
                    .track_view(video_id, &event.user_id, false)
                    .await?;
            }
            return Ok(());
        }

        // 2. Verify publisher registration (with caching)
        if !self
            .user_verification
            .is_registered_user(*publisher_user_id, app_state)
            .await?
        {
            if should_track {
                let _ = self
                    .view_tracker
                    .track_view(video_id, &event.user_id, false)
                    .await?;
            }
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

                    if let Ok(inr_for_analytics) = milestone_result {
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
                        (true, Some(inr_for_analytics))
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

    /// Process a video started event (for profile normal view tracking only)
    pub async fn process_video_started(&self, event: VideoStartedPayload) -> Result<()> {
        if let Some(user_id_str) = &event.user_id {
            let user_id = Principal::from_text(user_id_str).context("Invalid user_id format")?;

            // Only track if feature_name is "profile"
            if event.feature_name == "profile" {
                log::info!(
                    "Recording profile normal view for video {} by user {}",
                    event.video_id,
                    user_id
                );

                // Track as normal view (increments total_count_all only, no deduplication)
                let _ = self
                    .view_tracker
                    .track_view(&event.video_id, &user_id, false)
                    .await?;
            }
        } else {
            log::debug!(
                "Video started event for video {} has no user_id (not from authenticated request)",
                event.video_id
            );
        }

        Ok(())
    }

    /// Process a milestone reward
    /// Returns the INR amount (for analytics tracking)
    async fn process_milestone(
        &self,
        video_id: &str,
        creator_id: &Principal,
        view_count: u64,
        milestone_number: u64,
        config: &RewardConfig,
        app_state: &Arc<AppState>,
    ) -> Result<f64> {
        use crate::rewards::config::{RewardMode, RewardTokenType};

        // Calculate reward based on mode
        let (token_amount, total_inr) = match &config.reward_mode {
            RewardMode::InrAmount {
                amount_per_view_inr,
            } => {
                // Mode 1: INR amount → convert to tokens
                let total_inr = amount_per_view_inr * config.view_milestone as f64;

                // Convert INR to token amount based on reward_token type
                let token_amount = match config.reward_token {
                    RewardTokenType::Btc => {
                        // Use live BTC/INR rate from blockchain.info
                        self.btc_converter.convert_inr_to_btc(total_inr).await?
                    }
                    RewardTokenType::Dolr => {
                        // Use ICPSwap for DOLR price conversion
                        let icpswap_client = app_state
                            .rewards_module
                            .icpswap_client
                            .as_ref()
                            .context("ICPSwap client not available")?;

                        let amount = self
                            .btc_converter
                            .convert_inr_to_dolr_with_icpswap(total_inr, icpswap_client)
                            .await?;

                        log::info!(
                            "Used ICPSwap for DOLR conversion: ₹{} INR = {} DOLR",
                            total_inr,
                            amount
                        );
                        amount
                    }
                };

                (token_amount, total_inr)
            }
            RewardMode::DirectTokenE8s {
                amount_per_milestone_e8s,
            } => {
                // Mode 2: Direct e8s → convert to token amount, calculate INR for analytics
                let token_amount = *amount_per_milestone_e8s as f64 / 100_000_000.0;

                // Convert token amount to INR for analytics only
                let total_inr = match config.reward_token {
                    RewardTokenType::Btc => {
                        // Get BTC/INR rate and calculate INR equivalent
                        let btc_inr_rate = self.btc_converter.get_btc_inr_rate().await?;
                        token_amount * btc_inr_rate
                    }
                    RewardTokenType::Dolr => {
                        // Get DOLR/USD rate from ICPSwap and convert to INR
                        let icpswap_client = app_state
                            .rewards_module
                            .icpswap_client
                            .as_ref()
                            .context("ICPSwap client not available")?;

                        let inr_usd_rate = self.btc_converter.get_inr_usd_rate().await?;
                        let dolr_usd_rate = icpswap_client.get_dolr_usd_rate().await?;

                        // Convert DOLR → USD → INR
                        let usd_amount = token_amount * dolr_usd_rate;
                        let inr_amount = usd_amount * inr_usd_rate;

                        log::info!(
                            "Converted {} DOLR to ₹{} INR for analytics (DOLR/USD: {}, INR/USD: {})",
                            token_amount,
                            inr_amount,
                            dolr_usd_rate,
                            inr_usd_rate
                        );

                        inr_amount
                    }
                };

                log::info!(
                    "Using direct e8s mode: {} e8s = {} tokens ≈ ₹{} INR (for analytics)",
                    amount_per_milestone_e8s,
                    token_amount,
                    total_inr
                );

                (token_amount, total_inr)
            }
        };

        let token_name = match config.reward_token {
            RewardTokenType::Btc => "BTC",
            RewardTokenType::Dolr => "DOLR",
        };

        log::info!(
            "Processing reward for creator {}: {} {} (₹{}) for video {} milestone {}",
            creator_id,
            token_amount,
            token_name,
            total_inr,
            video_id,
            milestone_number
        );

        // Create reward record
        let reward_record = RewardRecord {
            video_id: video_id.to_string(),
            milestone: milestone_number,
            reward_btc: token_amount,
            reward_inr: total_inr,
            timestamp: Utc::now().timestamp(),
            tx_id: None,
            view_count,
        };

        // Store reward history (non-atomic)
        self.history_tracker
            .record_reward(creator_id, reward_record.clone())
            .await;

        // Queue token transaction
        match self
            .wallet
            .queue_reward(
                *creator_id,
                token_amount,
                total_inr,
                video_id,
                milestone_number,
                config.reward_token,
            )
            .await
        {
            Ok(tx_id) => {
                log::info!(
                    "{} reward queued successfully for creator {} with tx_id: {}",
                    token_name,
                    creator_id,
                    tx_id
                );

                // Update milestone tracker
                self.view_tracker
                    .set_last_milestone(video_id, milestone_number)
                    .await?;

                // Send analytics event
                analytics::send_btc_rewarded_event(
                    analytics::BtcRewardedEventParams {
                        creator_id,
                        video_id,
                        milestone: milestone_number,
                        reward_btc: token_amount,
                        reward_inr: total_inr,
                        view_count,
                        tx_id: Some(tx_id.clone()),
                    },
                    app_state,
                )
                .await;

                // Send notification to creator
                self.send_reward_notification(
                    creator_id,
                    video_id,
                    milestone_number,
                    token_amount,
                    total_inr,
                    view_count,
                    config.reward_token,
                    app_state,
                )
                .await;
            }
            Err(e) => {
                log::error!(
                    "Failed to queue {} reward for creator {}: {}",
                    token_name,
                    creator_id,
                    e
                );
                return Err(e);
            }
        }

        Ok(total_inr)
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
        reward_token: crate::rewards::config::RewardTokenType,
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
            reward_token,
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
        get_config(&self.dragonfly_pool).await.unwrap_or_else(|e| {
            log::error!("Failed to get config: {}", e);
            RewardConfig::default()
        })
    }

    /// Update configuration
    pub async fn update_config(&self, new_config: RewardConfig) -> Result<()> {
        update_config_fn(&self.dragonfly_pool, new_config).await?;
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
