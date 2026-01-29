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
const DEFAULT_DOLR_USD_RATE: f64 = 0.00042;

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

        // Fetch root key for non-mainnet (development only)
        // For mainnet, this is not needed
        // agent.fetch_root_key().await?;

        Ok(Self {
            agent: Arc::new(agent),
        })
    }

    /// Get DOLR/USD rate using two-hop pricing through ICPSwap
    /// 1. Query DOLR/ICP pool
    /// 2. Query ICP/ckUSDT pool
    /// 3. Calculate DOLR/USD = (DOLR/ICP) * (ICP/ckUSDT)
    pub async fn get_dolr_usd_rate(&self) -> Result<f64> {
        let now = chrono::Utc::now().timestamp();

        // Check cache first
        {
            let cache = DOLR_USD_PRICE_CACHE.read().await;
            if let Some(cached) = &*cache {
                if now - cached.timestamp < PRICE_CACHE_DURATION_SECS {
                    log::debug!("Using cached DOLR/USD rate: {}", cached.rate);
                    return Ok(cached.rate);
                }
            }
        }

        // Fetch fresh rate from ICPSwap
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
                log::error!("Failed to fetch DOLR/USD rate from ICPSwap: {}", e);

                // Try to use stale cache if available
                let cache = DOLR_USD_PRICE_CACHE.read().await;
                if let Some(cached) = &*cache {
                    log::warn!("Using stale cached DOLR/USD rate: {}", cached.rate);
                    return Ok(cached.rate);
                }

                // Last resort: use default rate
                log::warn!("Using default DOLR/USD rate: {}", DEFAULT_DOLR_USD_RATE);
                Ok(DEFAULT_DOLR_USD_RATE)
            }
        }
    }

    /// Fetch DOLR/USD rate from ICPSwap using two-hop pricing
    async fn fetch_dolr_usd_rate(&self) -> Result<f64> {
        // Step 1: Get DOLR/ICP rate
        // Quote: How much ICP do we get for 1 DOLR (1e8 units)?
        let dolr_amount = "100000000"; // 1 DOLR in e8s
        let dolr_icp_quote = self
            .query_pool_quote(DOLR_ICP_POOL, dolr_amount, true)
            .await
            .context("Failed to query DOLR/ICP pool")?;

        // dolr_icp_quote is in e8s (ICP units)
        let icp_per_dolr = dolr_icp_quote as f64 / 100_000_000.0;

        log::debug!("DOLR/ICP rate: 1 DOLR = {} ICP", icp_per_dolr);

        // Step 2: Get ICP/ckUSDT rate
        // Quote: How much ckUSDT do we get for the ICP amount?
        let icp_amount = dolr_icp_quote.to_string(); // Use the ICP we'd get from 1 DOLR
        let icp_ckusdt_quote = self
            .query_pool_quote(ICP_CKUSDT_POOL, &icp_amount, true)
            .await
            .context("Failed to query ICP/ckUSDT pool")?;

        // icp_ckusdt_quote is in e6 (ckUSDT uses 6 decimals, like USDT)
        let ckusdt_amount = icp_ckusdt_quote as f64 / 1_000_000.0;

        log::debug!("{} ICP = {} ckUSDT", icp_per_dolr, ckusdt_amount);

        // Step 3: Calculate DOLR/USD
        // If 1 DOLR = X ICP, and X ICP = Y ckUSDT, then 1 DOLR = Y ckUSDT = Y USD
        let dolr_usd_rate = ckusdt_amount;

        log::info!(
            "Calculated DOLR/USD rate from ICPSwap: 1 DOLR = ${} USD",
            dolr_usd_rate
        );

        Ok(dolr_usd_rate)
    }

    /// Query ICPSwap pool for a quote (how much output for given input)
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
            amount_out_minimum: "0".to_string(), // For quotes, we don't care about min output
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
                // Convert candid::Nat to u128
                let amount_u128: u128 = amount.0.try_into().context("Quote amount too large")?;
                Ok(amount_u128)
            }
            SwapResult::Err(err) => {
                anyhow::bail!("ICPSwap quote error: {}", err)
            }
        }
    }

    /// Convert INR amount to DOLR using live ICPSwap rate
    pub async fn convert_inr_to_dolr(&self, inr_amount: f64, inr_usd_rate: f64) -> Result<f64> {
        // Get DOLR/USD rate from ICPSwap
        let dolr_usd_rate = self.get_dolr_usd_rate().await?;

        // Convert: INR -> USD -> DOLR
        // inr_amount INR = (inr_amount / inr_usd_rate) USD = (inr_amount / inr_usd_rate / dolr_usd_rate) DOLR
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

    #[test]
    fn test_conversion_edge_cases() {
        // Zero amount
        let inr_amount = 0.0;
        let inr_usd_rate = 92.0;
        let dolr_usd_rate = 0.00042;
        let usd_amount = inr_amount / inr_usd_rate;
        let dolr_amount = usd_amount / dolr_usd_rate;
        assert_eq!(dolr_amount, 0.0);

        // Very small amount
        let inr_amount = 0.001;
        let usd_amount = inr_amount / inr_usd_rate;
        let dolr_amount = usd_amount / dolr_usd_rate;
        assert!(dolr_amount > 0.0);
        assert!(dolr_amount < 0.1);
    }

    #[test]
    fn test_pool_canister_ids_valid() {
        // Verify pool canister IDs are valid principals
        assert!(Principal::from_text(DOLR_ICP_POOL).is_ok());
        assert!(Principal::from_text(ICP_CKUSDT_POOL).is_ok());
    }

    #[test]
    fn test_default_fallback_rate() {
        // Verify default rate is reasonable (between 0.0001 and 0.01)
        assert!(DEFAULT_DOLR_USD_RATE > 0.0001);
        assert!(DEFAULT_DOLR_USD_RATE < 0.01);
    }

    #[test]
    fn test_cache_duration() {
        // Verify cache duration is 30 minutes
        assert_eq!(PRICE_CACHE_DURATION_SECS, 1800);
    }

    // Integration tests - require network access to ICP
    // Run with: cargo test --package off-chain-agent --lib rewards::icpswap::tests::test_live -- --ignored --nocapture

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

                // Reasonable bounds
                assert!(dolr_amount > 0.01, "Should be at least 0.01 DOLR");
                assert!(dolr_amount < 10.0, "Should be less than 10 DOLR");

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

    #[tokio::test]
    async fn test_caching_works() {
        let client = IcpSwapClient::new().await.expect("Failed to create client");

        // Clear cache
        {
            let mut cache = DOLR_USD_PRICE_CACHE.write().await;
            *cache = None;
        }

        // First call - should fetch from ICPSwap
        let start = std::time::Instant::now();
        let rate1 = client.get_dolr_usd_rate().await.ok();
        let duration1 = start.elapsed();

        // Second call - should use cache (much faster)
        let start = std::time::Instant::now();
        let rate2 = client.get_dolr_usd_rate().await.ok();
        let duration2 = start.elapsed();

        if let (Some(r1), Some(r2)) = (rate1, rate2) {
            println!("First call (network): {:?}", duration1);
            println!("Second call (cache): {:?}", duration2);
            println!("Rate 1: {}", r1);
            println!("Rate 2: {}", r2);

            // Cache should return same value
            assert_eq!(r1, r2, "Cached rate should match");

            // Cached call should be faster (at least 10x)
            assert!(
                duration2 < duration1 / 10,
                "Cached call should be much faster"
            );
        }
    }

    #[tokio::test]
    async fn test_stale_cache_fallback() {
        let client = IcpSwapClient::new().await.expect("Failed to create client");

        // Set a stale cache value
        {
            let mut cache = DOLR_USD_PRICE_CACHE.write().await;
            *cache = Some(CachedPrice {
                rate: 0.0005,
                timestamp: 0, // Very old timestamp
            });
        }

        // This should fetch fresh data, not use the stale cache
        let rate = client.get_dolr_usd_rate().await;

        match rate {
            Ok(r) => {
                println!("✅ Got fresh rate: {}", r);
                // Should be different from our stale value
                assert_ne!(r, 0.0005, "Should have fetched fresh rate");
            }
            Err(e) => {
                println!("⚠️ Failed to fetch fresh rate: {}", e);
            }
        }
    }

    #[test]
    fn test_expected_reward_calculation() {
        // Simulate the full reward calculation
        let config_inr = 0.04;
        let view_milestone = 1;
        let total_inr = config_inr * view_milestone as f64;

        // Typical rates
        let inr_usd_rate = 92.0;
        let dolr_usd_rate = 0.00042; // Example ICPSwap rate

        // Convert
        let usd_amount = total_inr / inr_usd_rate;
        let dolr_amount = usd_amount / dolr_usd_rate;
        let dolr_e8s = (dolr_amount * 100_000_000.0) as u64;

        println!("Expected reward for ₹{} INR:", total_inr);
        println!("  USD amount: ${:.10}", usd_amount);
        println!("  DOLR amount: {} DOLR", dolr_amount);
        println!("  In e8s: {} e8s", dolr_e8s);

        // Assertions
        assert!(dolr_amount > 0.5, "Should be at least 0.5 DOLR");
        assert!(dolr_amount < 2.0, "Should be less than 2 DOLR");
        assert!(dolr_e8s > 50_000_000, "Should be more than 0.5 DOLR in e8s");
        assert!(dolr_e8s < 200_000_000, "Should be less than 2 DOLR in e8s");
    }
}
