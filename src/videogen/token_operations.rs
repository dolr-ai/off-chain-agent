use candid::Principal;
use videogen_common::{TokenType, VideoGenError, TOKEN_COST_CONFIG};
use yral_canisters_common::utils::token::{TokenOperations, TokenOperationsProvider, SatsOperations, DolrOperations};
use yral_canisters_common::utils::token::balance::TokenBalance;
use num_bigint::BigUint;
use std::str::FromStr;

/// Get the appropriate token operations based on token type
pub fn get_token_operations(
    token_type: &TokenType,
    jwt_token: Option<String>,
    agent: Option<ic_agent::Agent>,
) -> Result<TokenOperationsProvider, VideoGenError> {
    match token_type {
        TokenType::Sats => Ok(TokenOperationsProvider::Sats(SatsOperations::new(jwt_token))),
        TokenType::Dolr => {
            let agent = agent.ok_or_else(|| {
                VideoGenError::InvalidInput("Agent required for DOLR operations".to_string())
            })?;
            Ok(TokenOperationsProvider::Dolr(DolrOperations::new(agent)))
        }
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
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to load balance: {}", e)))?;
    
    // Convert TokenBalance to BigUint
    BigUint::from_str(&balance.e8s.0.to_string())
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to parse balance: {}", e)))
}

/// Deduct balance for any token type
pub async fn deduct_token_balance(
    user_principal: Principal,
    amount: u64,
    token_type: &TokenType,
    jwt_token: Option<String>,
    agent: Option<ic_agent::Agent>,
) -> Result<u64, VideoGenError> {
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
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to deduct balance: {}", e)))
}

/// Add balance back for any token type (used for rollback)
pub async fn add_token_balance(
    user_principal: Principal,
    amount: u64,
    token_type: &TokenType,
    jwt_token: Option<String>,
    agent: Option<ic_agent::Agent>,
) -> Result<(), VideoGenError> {
    let ops = get_token_operations(token_type, jwt_token, agent)?;
    
    ops.add_balance(user_principal, amount)
        .await
        .map_err(|e| VideoGenError::NetworkError(format!("Failed to add balance: {}", e)))
}

/// Get the cost for a specific model and token type
pub fn get_model_cost(model_name: &str, token_type: &TokenType) -> u64 {
    TOKEN_COST_CONFIG.get_model_cost(model_name, token_type)
}