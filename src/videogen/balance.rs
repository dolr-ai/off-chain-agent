use candid::Principal;
use num_bigint::{BigInt, BigUint, Sign};
use serde::Serialize;
use videogen_common::VideoGenError;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};

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

/// Deduct balance for video generation and return the deducted amount
pub async fn deduct_videogen_balance(user_principal: Principal, jwt_token: &str) -> Result<u64, VideoGenError> {
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
    
    // Add JWT authorization header
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", jwt_token))
            .map_err(|_| VideoGenError::AuthError)?,
    );
    
    let res = client
        .post(&req_url)
        .headers(headers)
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

    // Return the deducted amount
    Ok(VIDEOGEN_COST_SATS)
}

/// Rollback balance deduction by adding the specified amount back
pub async fn rollback_videogen_balance(
    user_principal: Principal,
    deducted_amount: u64,
    jwt_token: &str,
) -> Result<(), VideoGenError> {
    // Load current balance
    let current_balance = load_sats_balance(user_principal).await?;

    // Create balance update request with positive delta to add the amount back
    let amount_biguint = BigUint::from(deducted_amount);
    let delta = BigInt::from_biguint(Sign::Plus, amount_biguint);

    let worker_req = SatsBalanceUpdateRequestV2 {
        previous_balance: current_balance,
        delta,
        is_airdropped: false,
    };

    // Send balance update to worker
    let req_url = format!("{}/v2/update_balance/{}", WORKER_URL, user_principal);
    let client = reqwest::Client::new();
    
    // Add JWT authorization header
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", jwt_token))
            .map_err(|_| VideoGenError::AuthError)?,
    );
    
    let res = client
        .post(&req_url)
        .headers(headers)
        .json(&worker_req)
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
