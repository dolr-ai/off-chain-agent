pub mod analytics;
pub mod api;
pub mod btc_conversion;
pub mod config;
pub mod engine;
pub mod fraud_detection;
pub mod history;
pub mod icpswap;
pub mod user_verification;
pub mod view_tracking;
pub mod wallet;

pub use btc_conversion::BtcConverter;
pub use engine::RewardEngine;
pub use icpswap::IcpSwapClient;
pub use view_tracking::ViewTracker;

use crate::yral_auth::dragonfly::DragonflyPool;
use anyhow::Result;
use std::sync::Arc;

#[derive(Clone)]
pub struct RewardsModule {
    pub view_tracker: ViewTracker,
    pub reward_engine: RewardEngine,
    pub btc_converter: BtcConverter,
    pub icpswap_client: Option<IcpSwapClient>,
    pub dragonfly_pool: Arc<DragonflyPool>,
}

impl RewardsModule {
    pub async fn new(dragonfly_pool: Arc<DragonflyPool>, admin_agent: ic_agent::Agent) -> Self {
        let view_tracker = ViewTracker::new(dragonfly_pool.clone());

        // Fetch config from Dragonfly (or use defaults if not found)
        let config = config::get_config(&dragonfly_pool)
            .await
            .unwrap_or_else(|e| {
                log::warn!("Failed to get config from Dragonfly, using defaults: {}", e);
                config::RewardConfig::default()
            });

        let reward_engine = RewardEngine::with_config(dragonfly_pool.clone(), admin_agent, config);
        let btc_converter = BtcConverter::new();

        let icpswap_client = match IcpSwapClient::new().await {
            Ok(client) => {
                log::info!("ICPSwap client initialized successfully");
                Some(client)
            }
            Err(e) => {
                log::error!("Failed to initialize ICPSwap client: {}", e);
                None
            }
        };

        Self {
            view_tracker,
            reward_engine,
            btc_converter,
            icpswap_client,
            dragonfly_pool,
        }
    }

    pub async fn initialize(&mut self) -> Result<()> {
        // Load Lua scripts into Dragonfly
        self.view_tracker.load_lua_scripts().await?;
        // Initialize reward engine (loads Lua scripts)
        self.reward_engine.initialize().await?;
        Ok(())
    }
}
