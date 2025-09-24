use crate::{
    app_state::AppState,
    events::types::VideoDurationWatchedPayloadV2,
    rewards::{
        btc_conversion::BtcConverter,
        config::{ConfigManager, RewardConfig},
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

pub struct RewardEngine {
    redis_pool: RedisPool,
    view_tracker: ViewTracker,
    user_verification: UserVerification,
    history_tracker: HistoryTracker,
    fraud_detector: FraudDetector,
    btc_converter: BtcConverter,
    wallet: WalletIntegration,
    config_manager: ConfigManager,
}

impl RewardEngine {
    pub fn new(redis_pool: RedisPool) -> Self {
        let mut view_tracker = ViewTracker::new(redis_pool.clone());
        let user_verification = UserVerification::new(redis_pool.clone());
        let history_tracker = HistoryTracker::new(redis_pool.clone());
        let fraud_detector = FraudDetector::new(redis_pool.clone());
        let btc_converter = BtcConverter::new();
        let wallet = WalletIntegration::new();
        let config_manager = ConfigManager::new(None);

        Self {
            redis_pool,
            view_tracker,
            user_verification,
            history_tracker,
            fraud_detector,
            btc_converter,
            wallet,
            config_manager,
        }
    }

    pub fn with_config(redis_pool: RedisPool, config: RewardConfig) -> Self {
        let mut view_tracker = ViewTracker::new(redis_pool.clone());
        let user_verification = UserVerification::new(redis_pool.clone());
        let history_tracker = HistoryTracker::new(redis_pool.clone());
        let fraud_detector = FraudDetector::with_config(
            redis_pool.clone(),
            config.fraud_threshold,
            config.shadow_ban_duration,
        );
        let btc_converter = BtcConverter::new();
        let wallet = WalletIntegration::new();
        let config_manager = ConfigManager::new(Some(config));

        Self {
            redis_pool,
            view_tracker,
            user_verification,
            history_tracker,
            fraud_detector,
            btc_converter,
            wallet,
            config_manager,
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
        // 1. Basic validation
        if !event.is_logged_in.unwrap_or(false) {
            log::debug!("Skipping non-logged-in view for video {:?}", event.video_id);
            return Ok(());
        }

        let config = self.config_manager.get_config().await;
        if event.absolute_watched < config.min_watch_duration {
            log::debug!(
                "Skipping view with insufficient watch duration: {} < {}",
                event.absolute_watched,
                config.min_watch_duration
            );
            return Ok(());
        }

        let video_id = event.video_id.as_ref().context("Missing video_id")?;
        let publisher_user_id = event
            .publisher_user_id
            .as_ref()
            .context("Missing publisher_user_id")?;

        // 2. Verify user registration (with caching)
        if !self
            .user_verification
            .is_registered_user(event.user_id, app_state)
            .await?
        {
            log::debug!("Skipping view from unregistered user {}", event.user_id);
            return Ok(());
        }

        // 3. Check if creator is shadow banned
        if self.fraud_detector.is_shadow_banned(publisher_user_id).await? {
            log::debug!("Creator {} is shadow banned, skipping reward", publisher_user_id);
            return Ok(());
        }

        // 4. ATOMIC: Count the view
        let config_version = self.config_manager.get_config_version().await;
        let view_count = self
            .view_tracker
            .track_view(video_id, &event.user_id, config_version)
            .await?;

        if let Some(count) = view_count {
            log::info!(
                "New view recorded for video {} by user {}: total count = {}",
                video_id,
                event.user_id,
                count
            );

            // 5. NON-ATOMIC: Store history (fire and forget)
            self.history_tracker.record_view(ViewRecord {
                user_id: event.user_id.to_string(),
                video_id: video_id.clone(),
                timestamp: Utc::now().timestamp(),
                duration_watched: event.absolute_watched,
                percentage_watched: event.percentage_watched,
            }).await;

            // 6. Check for milestone
            if count % config.view_milestone == 0 {
                let milestone_number = count / config.view_milestone;
                log::info!(
                    "Milestone {} reached for video {} (view count: {})",
                    milestone_number,
                    video_id,
                    count
                );

                // Process the reward
                self.process_milestone(
                    video_id,
                    publisher_user_id,
                    count,
                    milestone_number,
                    &config,
                )
                .await?;
            }

            // 7. Fraud detection (async, non-blocking)
            let fraud_check = self.fraud_detector.check_fraud_patterns(*publisher_user_id).await;
            if fraud_check == FraudCheck::Suspicious {
                log::warn!("Suspicious activity detected for creator {}", publisher_user_id);
            }
        } else {
            log::debug!(
                "Duplicate view for video {} by user {}",
                video_id,
                event.user_id
            );
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

                // TODO: Send notification to creator
                self.send_reward_notification(creator_id, config.reward_amount_inr, milestone_number)
                    .await;
            }
            Err(e) => {
                log::error!("Failed to queue BTC reward for creator {}: {}", creator_id, e);
                return Err(e);
            }
        }

        Ok(())
    }

    /// Send notification to creator about reward
    async fn send_reward_notification(
        &self,
        creator_id: &Principal,
        reward_inr: f64,
        milestone: u64,
    ) {
        // TODO: Implement actual notification sending
        log::info!(
            "Sending reward notification to creator {}: ₹{} for milestone {}",
            creator_id,
            reward_inr,
            milestone
        );

        // This would integrate with the existing push notification system
        // Example:
        // let notification = NotificationPayload {
        //     title: "Reward Earned!".to_string(),
        //     body: format!("You earned ₹{} worth of BTC for reaching {} views!", reward_inr, milestone * 100),
        //     ...
        // };
        // notification_client.send(creator_id, notification).await;
    }

    /// Get current configuration
    pub async fn get_config(&self) -> RewardConfig {
        self.config_manager.get_config().await
    }

    /// Update configuration
    pub async fn update_config(&self, new_config: RewardConfig) -> Result<()> {
        self.config_manager.update_config(new_config).await?;
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
}