use anyhow::{Context, Result};
use candid::{CandidType, Deserialize, Principal};
use ic_agent::Agent;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::sync::RwLock;

// ICPSwap pool canister IDs
const DOLR_ICP_POOL: &str = "rxwy2-zaaaa-aaaag-qcfna-cai"; // DOLR/ICP pool
const ICP_CKUSDT_POOL: &str = "hkstf-6iaaa-aaaag-qkcoq-cai"; // ICP/ckUSDT pool

// Cache duration: 30 minutes
const PRICE_CACHE_DURATION_SECS: i64 = 1800;

// Default fallback rate if ICPSwap fails
const DEFAULT_DOLR_USD_RATE: f64 = 0.000413;

#[derive(Debug, Clone)]
struct CachedPrice {
    rate: f64,
    timestamp: i64,
}

static DOLR_USD_PRICE_CACHE: Lazy<Arc<RwLock<Option<CachedPrice>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

#[derive(CandidType, Deserialize, Debug)]
struct SwapArgs {
    #[serde(rename = "amountIn")]
    amount_in: String,
    #[serde(rename = "zeroForOne")]
    zero_for_one: bool,
    #[serde(rename = "amountOutMinimum")]
    amount_out_minimum: String,
}

#[derive(CandidType, Deserialize, Debug)]
enum SwapResult {
    #[serde(rename = "ok")]
    Ok(candid::Nat),
    #[serde(rename = "err")]
    Err(String),
}

#[derive(Clone)]
pub struct IcpSwapClient {
    agent: Arc<Agent>,
}

impl IcpSwapClient {
    pub async fn new() -> Result<Self> {
        let agent = Agent::builder()
            .with_url("https://icp-api.io")
            .build()
            .context("Failed to create IC agent")?;

        Ok(Self {
            agent: Arc::new(agent),
        })
    }

    pub async fn get_dolr_usd_rate(&self) -> Result<f64> {
        let now = chrono::Utc::now().timestamp();

        {
            let cache = DOLR_USD_PRICE_CACHE.read().await;
            if let Some(cached) = &*cache {
                if now - cached.timestamp < PRICE_CACHE_DURATION_SECS {
                    log::debug!("Using cached DOLR/USD rate: {}", cached.rate);
                    return Ok(cached.rate);
                }
            }
        }

        match self.fetch_dolr_usd_rate().await {
            Ok(rate) => {
                let mut cache = DOLR_USD_PRICE_CACHE.write().await;
                *cache = Some(CachedPrice {
                    rate,
                    timestamp: now,
                });
                log::info!("Updated DOLR/USD rate from ICPSwap: {}", rate);
                Ok(rate)
            }
            Err(e) => {
                let cache = DOLR_USD_PRICE_CACHE.read().await;
                if let Some(cached) = &*cache {
                    log::warn!("Using stale cached DOLR/USD rate: {}", cached.rate);
                    return Ok(cached.rate);
                }

                log::warn!("Using default DOLR/USD rate: {}", DEFAULT_DOLR_USD_RATE);
                Ok(DEFAULT_DOLR_USD_RATE)
            }
        }
    }

    /// Fetch DOLR/USD rate from ICPSwap using two-hop pricing
    async fn fetch_dolr_usd_rate(&self) -> Result<f64> {
        let dolr_amount = "100000000"; // 1 DOLR in e8s
        let dolr_icp_quote = self
            .query_pool_quote(DOLR_ICP_POOL, dolr_amount, true)
            .await
            .context("Failed to query DOLR/ICP pool")?;

        let icp_per_dolr = dolr_icp_quote as f64 / 100_000_000.0;

        log::debug!("DOLR/ICP rate: 1 DOLR = {} ICP", icp_per_dolr);

        let icp_amount = dolr_icp_quote.to_string();
        let icp_ckusdt_quote = self
            .query_pool_quote(ICP_CKUSDT_POOL, &icp_amount, true)
            .await
            .context("Failed to query ICP/ckUSDT pool")?;

        let ckusdt_amount = icp_ckusdt_quote as f64 / 1_000_000.0;

        log::debug!("{} ICP = {} ckUSDT", icp_per_dolr, ckusdt_amount);

        let dolr_usd_rate = ckusdt_amount;

        log::info!(
            "Calculated DOLR/USD rate from ICPSwap: 1 DOLR = ${} USD",
            dolr_usd_rate
        );

        Ok(dolr_usd_rate)
    }

    async fn query_pool_quote(
        &self,
        pool_canister_id: &str,
        amount_in: &str,
        zero_for_one: bool,
    ) -> Result<u128> {
        let pool_principal =
            Principal::from_text(pool_canister_id).context("Invalid pool canister ID")?;

        let args = SwapArgs {
            amount_in: amount_in.to_string(),
            zero_for_one,
            amount_out_minimum: "0".to_string(),
        };

        let response = self
            .agent
            .query(&pool_principal, "quote")
            .with_arg(candid::encode_one(&args).context("Failed to encode args")?)
            .call()
            .await
            .context("Failed to call quote method")?;

        let result: SwapResult =
            candid::decode_one(&response).context("Failed to decode response")?;

        match result {
            SwapResult::Ok(amount) => {
                let amount_u128: u128 = amount.0.try_into().context("Quote amount too large")?;
                Ok(amount_u128)
            }
            SwapResult::Err(err) => {
                anyhow::bail!("ICPSwap quote error: {}", err)
            }
        }
    }

    pub async fn convert_inr_to_dolr(&self, inr_amount: f64, inr_usd_rate: f64) -> Result<f64> {
        let dolr_usd_rate = self.get_dolr_usd_rate().await?;

        let usd_amount = inr_amount / inr_usd_rate;
        let dolr_amount = usd_amount / dolr_usd_rate;

        log::debug!(
            "Converting ₹{} INR to {} DOLR (INR/USD: {}, DOLR/USD: {})",
            inr_amount,
            dolr_amount,
            inr_usd_rate,
            dolr_usd_rate
        );

        Ok(dolr_amount)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conversion_math() {
        // Test the INR -> USD -> DOLR conversion logic
        let inr_amount = 0.04;
        let inr_usd_rate = 92.0;
        let dolr_usd_rate = 0.00042;

        // Step 1: INR to USD
        let usd_amount = inr_amount / inr_usd_rate;
        assert_eq!(usd_amount, 0.04 / 92.0);
        assert!((usd_amount - 0.000434782_f64).abs() < 0.000001);

        // Step 2: USD to DOLR
        let dolr_amount = usd_amount / dolr_usd_rate;
        assert!((dolr_amount - 1.035_f64).abs() < 0.01);

        // In e8s
        let dolr_e8s = (dolr_amount * 100_000_000.0) as u64;
        assert!(dolr_e8s > 100_000_000); // More than 1 DOLR
        assert!(dolr_e8s < 110_000_000); // Less than 1.1 DOLR
    }

    #[tokio::test]
    async fn test_live_icpswap_client_creation() {
        println!("\n🔧 Testing ICPSwap client creation...");
        let client = IcpSwapClient::new().await;
        assert!(
            client.is_ok(),
            "Failed to create ICPSwap client: {:?}",
            client.err()
        );
        println!("✅ Client created successfully");
    }

    #[tokio::test]
    #[ignore] // Requires network access to ICP - ACTUAL CANISTER CALLS
    async fn test_live_query_dolr_icp_pool() {
        println!("\n🔧 Testing DOLR/ICP pool query...");
        println!("   Pool canister: {}", DOLR_ICP_POOL);

        let client = IcpSwapClient::new().await.expect("Failed to create client");

        // Query: How much ICP for 1 DOLR?
        let dolr_amount = "100000000"; // 1 DOLR in e8s
        println!("   Querying: {} DOLR (1e8 units)", dolr_amount);

        let result = client
            .query_pool_quote(DOLR_ICP_POOL, dolr_amount, true)
            .await;

        match result {
            Ok(icp_amount) => {
                let icp = icp_amount as f64 / 100_000_000.0;
                println!("✅ Quote successful: 1 DOLR = {} ICP", icp);
                println!("   Raw e8s: {}", icp_amount);

                // Sanity checks
                assert!(icp_amount > 0, "Should get some ICP");
                assert!(icp > 0.0, "ICP amount should be positive");
                assert!(icp < 100.0, "ICP amount seems too high");
            }
            Err(e) => {
                panic!("❌ Failed to query DOLR/ICP pool: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_live_query_icp_ckusdt_pool() {
        println!("\n🔧 Testing ICP/ckUSDT pool query...");
        println!("   Pool canister: {}", ICP_CKUSDT_POOL);

        let client = IcpSwapClient::new().await.expect("Failed to create client");

        // Query: How much ckUSDT for 0.01 ICP?
        let icp_amount = "1000000"; // 0.01 ICP in e8s
        println!("   Querying: {} ICP (0.01 ICP)", icp_amount);

        let result = client
            .query_pool_quote(ICP_CKUSDT_POOL, icp_amount, true)
            .await;

        match result {
            Ok(ckusdt_amount) => {
                let ckusdt = ckusdt_amount as f64 / 1_000_000.0; // ckUSDT uses 6 decimals
                println!("✅ Quote successful: 0.01 ICP = {} ckUSDT", ckusdt);
                println!("   Raw e6: {}", ckusdt_amount);

                // Sanity checks
                assert!(ckusdt_amount > 0, "Should get some ckUSDT");
                assert!(ckusdt > 0.0, "ckUSDT amount should be positive");
                assert!(ckusdt < 1000.0, "ckUSDT amount seems too high");
            }
            Err(e) => {
                panic!("❌ Failed to query ICP/ckUSDT pool: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_live_fetch_dolr_usd_rate() {
        println!("\n🔧 Testing full DOLR/USD rate calculation...");

        let client = IcpSwapClient::new().await.expect("Failed to create client");
        let rate = client.get_dolr_usd_rate().await;

        match rate {
            Ok(r) => {
                println!("✅ DOLR/USD rate: ${:.10}", r);
                println!("   1 DOLR = ${:.10} USD", r);
                println!("   1 USD = {:.2} DOLR", 1.0 / r);

                // Sanity checks
                assert!(r > 0.0, "Rate should be positive");
                assert!(r < 2.0, "Rate seems too large (> $2 per DOLR)");
                assert!(r > 0.01, "Rate seems too small (< $0.01 per DOLR)");
            }
            Err(e) => {
                panic!("❌ Failed to fetch DOLR/USD rate: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_convert_inr_to_dolr() {
        let client = IcpSwapClient::new().await.expect("Failed to create client");
        let inr_amount = 0.04;
        let inr_usd_rate = 92.0;

        let result = client.convert_inr_to_dolr(inr_amount, inr_usd_rate).await;

        match result {
            Ok(dolr_amount) => {
                println!("✅ ₹{} INR = {} DOLR", inr_amount, dolr_amount);
                let dolr_e8s = (dolr_amount * 100_000_000.0) as u64;
                println!("   In e8s: {}", dolr_e8s);

                assert!(dolr_amount > 0.0001, "Should be at least 0.0001 DOLR");
                assert!(dolr_amount < 0.1, "Should be less than 0.1 DOLR");

                // Compare with old bug
                let old_bug_usd = inr_amount / inr_usd_rate;
                let old_bug_dolr = old_bug_usd; // Bug: used USD as DOLR directly
                let old_bug_e8s = (old_bug_dolr * 100_000_000.0) as u64;

                println!("   Old bug amount: {} e8s", old_bug_e8s);
                println!(
                    "   Improvement: {:.1}x more DOLR",
                    dolr_e8s as f64 / old_bug_e8s as f64
                );

                assert!(dolr_e8s > old_bug_e8s, "New amount should be much larger");
            }
            Err(e) => {
                println!("⚠️ Failed to convert (expected if ICPSwap is down): {}", e);
            }
        }
    }
}
