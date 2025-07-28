use candid::Principal;
use num_bigint::{BigInt, BigUint, Sign};
use serde::{Deserialize, Serialize};
use videogen_common::VideoGenError;

const VIDEOGEN_COST_SATS: u64 = 1000; // Cost for video generation in sats
const WORKER_URL: &str = "https://yral-hot-or-not.go-bazzinga.workers.dev";

#[derive(Serialize)]
struct SatsBalanceUpdateRequestV2 {
    previous_balance: BigUint,
    delta: BigInt,
    is_airdropped: bool,
}

/// Load current balance for user from worker
pub async fn load_sats_balance(user_principal: Principal) -> Result<BigUint, VideoGenError> {
    let req_url = format!("{}/balance/{}", WORKER_URL, user_principal);

    let client = reqwest::Client::new();
    let res = client
        .get(&req_url)
        .send()
        .await
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to fetch balance: {}", e)))?;

    if !res.status().is_success() {
        return Err(VideoGenError::NetworkError(format!(
            "Balance fetch failed with status: {}",
            res.status()
        )));
    }

    let balance_str = res.text().await.map_err(|e| {
        VideoGenError::NetworkError(format!("Failed to read balance response: {}", e))
    })?;

    // Parse the balance from string to BigUint
    balance_str
        .trim()
        .parse::<BigUint>()
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to parse balance: {}", e)))
}

/// Deduct balance for video generation
pub async fn deduct_videogen_balance(user_principal: Principal) -> Result<BigUint, VideoGenError> {
    // Load current balance
    let balance = load_sats_balance(user_principal).await?;

    // Check if user has sufficient balance
    let cost_biguint = BigUint::from(VIDEOGEN_COST_SATS);
    if balance < cost_biguint {
        return Err(VideoGenError::InsufficientBalance);
    }

    // Create balance update request with negative delta
    let delta = BigInt::from_biguint(Sign::Minus, cost_biguint);

    let worker_req = SatsBalanceUpdateRequestV2 {
        previous_balance: balance.clone(),
        delta,
        is_airdropped: false,
    };

    // Send balance update to worker
    let req_url = format!("{}/v2/update_balance/{}", WORKER_URL, user_principal);
    let client = reqwest::Client::new();
    let res = client
        .post(&req_url)
        .json(&worker_req)
        .send()
        .await
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to update balance: {}", e)))?;

    if !res.status().is_success() {
        return Err(VideoGenError::NetworkError(format!(
            "Balance update failed with status: {}",
            res.status()
        )));
    }

    // Return the original balance for potential rollback
    Ok(balance)
}

/// Rollback balance deduction by adding the cost back
pub async fn rollback_videogen_balance(
    user_principal: Principal,
    original_balance: BigUint,
) -> Result<(), VideoGenError> {
    // Create balance update request with positive delta to add the cost back
    let cost_biguint = BigUint::from(VIDEOGEN_COST_SATS);
    let delta = BigInt::from_biguint(Sign::Plus, cost_biguint.clone());

    // Calculate what the balance should be after deduction for the previous_balance field
    let deducted_balance = &original_balance - &cost_biguint;

    let worker_req = SatsBalanceUpdateRequestV2 {
        previous_balance: deducted_balance,
        delta,
        is_airdropped: false,
    };

    // Send balance update to worker
    let req_url = format!("{}/v2/update_balance/{}", WORKER_URL, user_principal);
    let client = reqwest::Client::new();
    let res = client
        .post(&req_url)
        .json(&worker_req) // TODO: need to add Auth jwt , but when being used
        .send()
        .await
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to rollback balance: {}", e)))?;

    if !res.status().is_success() {
        return Err(VideoGenError::NetworkError(format!(
            "Balance rollback failed with status: {}",
            res.status()
        )));
    }

    Ok(())
}
