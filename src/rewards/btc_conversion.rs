use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

const CACHE_DURATION_SECS: i64 = 300; // 5 minutes
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

static RATE_CACHE: Lazy<Arc<RwLock<Option<CachedRate>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

#[derive(Clone)]
pub struct BtcConverter {
    client: reqwest::Client,
}

impl BtcConverter {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self { client }
    }

    /// Convert INR amount to BTC
    pub async fn convert_inr_to_btc(&self, inr_amount: f64) -> Result<f64> {
        let rate = self.get_btc_inr_rate().await?;
        let btc_amount = inr_amount / rate;
        Ok(btc_amount)
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

    /// Store rate in Redis for distributed caching (optional)
    pub async fn _cache_rate_in_redis(
        &self,
        redis_pool: &crate::types::RedisPool,
        rate: f64,
    ) -> Result<()> {
        let mut conn = redis_pool.get().await?;
        let key = "rewards:btc_inr_rate";
        conn.set_ex::<_, _, ()>(key, rate.to_string(), CACHE_DURATION_SECS as u64)
            .await?;
        Ok(())
    }

    /// Get cached rate from Redis (optional)
    pub async fn _get_cached_rate_from_redis(
        &self,
        redis_pool: &crate::types::RedisPool,
    ) -> Result<Option<f64>> {
        let mut conn = redis_pool.get().await?;
        let key = "rewards:btc_inr_rate";
        let rate: Option<String> = conn.get(key).await?;
        Ok(rate.and_then(|r| r.parse().ok()))
    }
}

/// Convenience function to convert INR to BTC
pub async fn _convert_inr_to_btc(inr_amount: f64) -> Result<f64> {
    let converter = BtcConverter::new();
    converter.convert_inr_to_btc(inr_amount).await
}

/// Format BTC amount for display (8 decimal places)
pub fn _format_btc(btc_amount: f64) -> String {
    format!("{:.8}", btc_amount)
}

/// Format INR amount for display (2 decimal places)
pub fn _format_inr(inr_amount: f64) -> String {
    format!("{:.2}", inr_amount)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btc_formatting() {
        assert_eq!(format_btc(0.00001234), "0.00001234");
        assert_eq!(format_btc(1.0), "1.00000000");
    }

    #[test]
    fn test_inr_formatting() {
        assert_eq!(format_inr(10.0), "10.00");
        assert_eq!(format_inr(99.999), "100.00");
    }
}
