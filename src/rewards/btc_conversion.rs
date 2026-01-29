use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

const BTC_CACHE_DURATION_SECS: i64 = 300; // 5 minutes for BTC
const BLOCKCHAIN_API_URL: &str = "https://blockchain.info/ticker";
const DEFAULT_BTC_INR_RATE: f64 = 5000000.0; // Fallback rate: 1 BTC = 50 lakh INR

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CurrencyInfo {
    #[serde(rename = "15m")]
    fifteen_min: f64,
    last: f64,
    buy: f64,
    sell: f64,
    symbol: String,
}

type BlockchainTickerResponse = HashMap<String, CurrencyInfo>;

#[derive(Debug, Clone)]
struct CachedRate {
    rate: f64,
    timestamp: i64,
}

static BTC_RATE_CACHE: Lazy<Arc<RwLock<Option<CachedRate>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

#[derive(Clone)]
pub struct BtcConverter {
    client: reqwest::Client,
}

impl Default for BtcConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl BtcConverter {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self { client }
    }

    /// Convert INR amount to BTC using live exchange rate
    pub async fn convert_inr_to_btc(&self, inr_amount: f64) -> Result<f64> {
        let rate = self.get_btc_inr_rate().await?;
        let btc_amount = inr_amount / rate;
        Ok(btc_amount)
    }

    /// Convert INR to DOLR using ICPSwap client
    pub async fn convert_inr_to_dolr_with_icpswap(
        &self,
        inr_amount: f64,
        icpswap_client: &crate::rewards::icpswap::IcpSwapClient,
    ) -> Result<f64> {
        // Get INR/USD rate
        let inr_usd_rate = self.get_inr_usd_rate().await?;

        // Use ICPSwap for DOLR/USD conversion
        let dolr_amount = icpswap_client
            .convert_inr_to_dolr(inr_amount, inr_usd_rate)
            .await?;

        Ok(dolr_amount)
    }

    /// Get current BTC/INR exchange rate
    pub async fn get_btc_inr_rate(&self) -> Result<f64> {
        let now = chrono::Utc::now().timestamp();

        // Check cache first
        {
            let cache = BTC_RATE_CACHE.read().await;
            if let Some(cached) = &*cache {
                if now - cached.timestamp < BTC_CACHE_DURATION_SECS {
                    log::debug!("Using cached BTC/INR rate: {}", cached.rate);
                    return Ok(cached.rate);
                }
            }
        }

        // Fetch fresh rate
        match self.fetch_btc_rate_from_api().await {
            Ok(rate) => {
                let mut cache = BTC_RATE_CACHE.write().await;
                *cache = Some(CachedRate {
                    rate,
                    timestamp: now,
                });
                log::info!("Updated BTC/INR rate: {} INR per BTC", rate);
                Ok(rate)
            }
            Err(e) => {
                log::error!("Failed to fetch BTC/INR rate: {}", e);

                // Try to use stale cache if available
                let cache = BTC_RATE_CACHE.read().await;
                if let Some(cached) = &*cache {
                    log::warn!("Using stale cached BTC/INR rate: {}", cached.rate);
                    return Ok(cached.rate);
                }

                // Last resort: use default rate
                log::warn!("Using default BTC/INR rate: {}", DEFAULT_BTC_INR_RATE);
                Ok(DEFAULT_BTC_INR_RATE)
            }
        }
    }

    /// Get current INR/USD exchange rate
    pub async fn get_inr_usd_rate(&self) -> Result<f64> {
        let response = self
            .client
            .get(BLOCKCHAIN_API_URL)
            .send()
            .await
            .context("Failed to send request to Blockchain.info")?;

        if !response.status().is_success() {
            anyhow::bail!("Blockchain.info API returned status: {}", response.status());
        }

        let data: BlockchainTickerResponse = response
            .json()
            .await
            .context("Failed to parse Blockchain.info response")?;

        let inr_data = data
            .get("INR")
            .context("INR currency not found in Blockchain.info response")?;

        let usd_data = data
            .get("USD")
            .context("USD currency not found in Blockchain.info response")?;

        // Calculate INR/USD rate: how many INR per 1 USD
        let inr_usd_rate = inr_data.last / usd_data.last;

        log::debug!("INR/USD exchange rate: {} INR per USD", inr_usd_rate);
        Ok(inr_usd_rate)
    }

    /// Fetch BTC/INR rate from Blockchain.info API
    async fn fetch_btc_rate_from_api(&self) -> Result<f64> {
        let response = self
            .client
            .get(BLOCKCHAIN_API_URL)
            .send()
            .await
            .context("Failed to send request to Blockchain.info")?;

        if !response.status().is_success() {
            anyhow::bail!("Blockchain.info API returned status: {}", response.status());
        }

        let data: BlockchainTickerResponse = response
            .json()
            .await
            .context("Failed to parse Blockchain.info response")?;

        let inr_data = data
            .get("INR")
            .context("INR currency not found in Blockchain.info response")?;

        Ok(inr_data.last)
    }
}
