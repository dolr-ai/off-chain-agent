use axum::{http::StatusCode, Json};
use candid::Principal;
use num_bigint::BigUint;
use std::str::FromStr;
use videogen_common::{TokenType, VideoGenError, TOKEN_COST_CONFIG};
use yral_canisters_client::rate_limits::{RateLimits, VideoGenRequestKey};
use yral_canisters_common::utils::token::{
    DolrOperations, SatsOperations, TokenOperations, TokenOperationsProvider,
};

/// Get the appropriate token operations based on token type
pub fn get_token_operations(
    token_type: &TokenType,
    jwt_token: Option<String>,
    agent: Option<ic_agent::Agent>,
) -> Result<TokenOperationsProvider, VideoGenError> {
    match token_type {
        TokenType::Sats => Ok(TokenOperationsProvider::Sats(SatsOperations::new(
            jwt_token,
        ))),
        TokenType::Dolr => {
            let agent = agent.ok_or_else(|| {
                VideoGenError::InvalidInput("Agent required for DOLR operations".to_string())
            })?;
            Ok(TokenOperationsProvider::Dolr(DolrOperations::new(agent)))
        }
        TokenType::Free => Err(VideoGenError::InvalidInput(
            "No token operations for Free token type".to_string(),
        )),
    }
}

/// Load balance for any token type
pub async fn load_token_balance(
    user_principal: Principal,
    token_type: &TokenType,
    jwt_token: Option<String>,
    agent: Option<ic_agent::Agent>,
) -> Result<BigUint, VideoGenError> {
    let ops = get_token_operations(token_type, jwt_token, agent)?;
    let balance = ops
        .load_balance(user_principal)
        .await
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to load balance: {e}")))?;

    // Convert TokenBalance to BigUint
    BigUint::from_str(&balance.e8s.0.to_string())
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to parse balance: {e}")))
}

/// Deduct balance for any token type
pub async fn deduct_token_balance(
    user_principal: Principal,
    amount: u64,
    token_type: &TokenType,
    jwt_token: Option<String>,
    agent: Option<ic_agent::Agent>,
) -> Result<u64, VideoGenError> {
    // For Free token type, no deduction needed
    if matches!(token_type, TokenType::Free) {
        return Ok(0);
    }

    let ops = get_token_operations(token_type, jwt_token.clone(), agent.clone())?;

    // Check balance first
    let balance = load_token_balance(user_principal, token_type, jwt_token, agent).await?;
    let cost_biguint = BigUint::from(amount);
    if balance < cost_biguint {
        return Err(VideoGenError::InsufficientBalance);
    }

    // Deduct the balance
    ops.deduct_balance(user_principal, amount)
        .await
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to deduct balance: {e}")))
}

/// Add balance back for any token type (used for rollback)
pub async fn add_token_balance(
    user_principal: Principal,
    amount: u64,
    token_type: &TokenType,
    jwt_token: Option<String>,
    agent: Option<ic_agent::Agent>,
) -> Result<(), VideoGenError> {
    // For Free token type, no operation needed
    if matches!(token_type, TokenType::Free) {
        return Ok(());
    }

    let ops = get_token_operations(token_type, jwt_token, agent)?;

    ops.add_balance(user_principal, amount)
        .await
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to add balance: {e}")))
}

/// Get the cost for a specific model and token type
pub fn get_model_cost(model_name: &str, token_type: &TokenType) -> u64 {
    TOKEN_COST_CONFIG.get_model_cost(model_name, token_type)
}

/// Handle token balance deduction with automatic rate limit cleanup on failure
pub async fn deduct_balance_with_cleanup(
    user_principal: Principal,
    cost: u64,
    token_type: &TokenType,
    jwt_token: String,
    agent: &ic_agent::Agent,
    request_key: &crate::videogen::VideoGenRequestKey,
    property: &str,
) -> Result<Option<u64>, (StatusCode, Json<VideoGenError>)> {
    // For free requests, return None immediately
    if matches!(token_type, TokenType::Free) {
        return Ok(None);
    }

    // Prepare auth based on token type
    let jwt_opt = match token_type {
        TokenType::Sats => Some(jwt_token),
        TokenType::Dolr => None,
        TokenType::Free => None, // Should not reach here
    };

    // Get agent for DOLR operations
    let agent_opt = if matches!(token_type, TokenType::Dolr) {
        Some(agent.clone())
    } else {
        None
    };

    // Attempt to deduct balance
    match deduct_token_balance(user_principal, cost, token_type, jwt_opt, agent_opt).await {
        Ok(amount) => {
            log::info!("Successfully deducted {amount} {token_type:?} from user {user_principal}");
            Ok(Some(amount))
        }
        Err(e) => {
            log::error!(
                "Balance deduction failed for user {user_principal}: {e:?}. Decrementing rate limit counter."
            );

            // Cleanup: decrement rate limit counter
            let rate_limits_client = RateLimits(*crate::consts::RATE_LIMITS_CANISTER_ID, agent);

            // Convert our key type to the canister's key type
            let canister_key = VideoGenRequestKey {
                principal: request_key.principal,
                counter: request_key.counter,
            };

            if let Err(dec_err) = rate_limits_client
                .decrement_video_generation_counter_v_1(canister_key, property.to_string())
                .await
            {
                log::error!("Failed to decrement rate limit counter after balance deduction failure: {dec_err}");
            }

            // Return appropriate error
            Err(match e {
                VideoGenError::InsufficientBalance => (StatusCode::PAYMENT_REQUIRED, Json(e)),
                _ => (StatusCode::SERVICE_UNAVAILABLE, Json(e)),
            })
        }
    }
}
