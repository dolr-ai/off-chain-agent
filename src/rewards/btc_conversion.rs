use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

const CACHE_DURATION_SECS: i64 = 300; // 5 minutes
const BLOCKCHAIN_API_URL: &str = "https://blockchain.info/ticker";
const DEFAULT_BTC_INR_RATE: f64 = 5000000.0; // Fallback rate: 1 BTC = 50 lakh INR
const DEFAULT_DOLR_INR_RATE: f64 = 1.0; // 1 DOLR = 1 INR

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

static RATE_CACHE: Lazy<Arc<RwLock<Option<CachedRate>>>> =
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

    /// Convert INR amount to DOLR using hardcoded rate
    /// 1 DOLR ≈ 1 USD ≈ 84 INR
    pub fn convert_inr_to_dolr(&self, inr_amount: f64, dolr_inr_rate: Option<f64>) -> f64 {
        let rate = dolr_inr_rate.unwrap_or(DEFAULT_DOLR_INR_RATE);
        let dolr_amount = inr_amount / rate;
        log::debug!(
            "Converting ₹{} INR to {} DOLR (rate: {} INR/DOLR)",
            inr_amount,
            dolr_amount,
            rate
        );
        dolr_amount
    }

    /// Get default DOLR/INR rate
    pub fn get_dolr_inr_rate(&self) -> f64 {
        DEFAULT_DOLR_INR_RATE
    }

    /// Get current BTC/INR exchange rate
    pub async fn get_btc_inr_rate(&self) -> Result<f64> {
        // Check cache first
        let now = chrono::Utc::now().timestamp();

        {
            let cache = RATE_CACHE.read().await;
            if let Some(cached) = &*cache {
                if now - cached.timestamp < CACHE_DURATION_SECS {
                    log::debug!("Using cached BTC/INR rate: {}", cached.rate);
                    return Ok(cached.rate);
                }
            }
        }

        // Fetch fresh rate
        match self.fetch_rate_from_api().await {
            Ok(rate) => {
                // Update cache
                let mut cache = RATE_CACHE.write().await;
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
                let cache = RATE_CACHE.read().await;
                if let Some(cached) = &*cache {
                    log::warn!("Using stale cached rate: {}", cached.rate);
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
        // Fetch ticker data to get both INR and USD rates for BTC
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
        // If 1 BTC = 5,000,000 INR and 1 BTC = 60,000 USD
        // Then 1 USD = 5,000,000 / 60,000 = 83.33 INR
        let inr_usd_rate = inr_data.last / usd_data.last;

        log::debug!("INR/USD exchange rate: {} INR per USD", inr_usd_rate);
        Ok(inr_usd_rate)
    }

    /// Fetch rate from Blockchain.info API
    async fn fetch_rate_from_api(&self) -> Result<f64> {
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

        // Use the 'last' price for INR
        Ok(inr_data.last)
    }
}
