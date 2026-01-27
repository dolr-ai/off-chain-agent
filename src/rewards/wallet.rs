use crate::rewards::config::RewardTokenType;
use anyhow::Result;
use candid::Principal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use yral_canisters_common::utils::token::{CkBtcOperations, DolrOperations, TokenOperations};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TransactionStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

#[derive(Clone)]
pub struct WalletIntegration {
    dolr_ops: DolrOperations,
    btc_ops: CkBtcOperations,
}

const MAX_AMOUNT_E8S_VIEWS: u64 = 10_000_000_000; // 10 tokens max

impl WalletIntegration {
    pub fn new(admin_agent: ic_agent::Agent) -> Self {
        Self {
            dolr_ops: DolrOperations::new(admin_agent.clone()),
            btc_ops: CkBtcOperations::new(admin_agent),
        }
    }

    /// Queue a reward transaction for processing (supports both BTC and DOLR)
    pub async fn queue_reward(
        &self,
        creator_id: Principal,
        token_amount: f64,
        amount_inr: f64,
        video_id: &str,
        milestone: u64,
        token_type: RewardTokenType,
    ) -> Result<String> {
        // Convert to e8s (1 token = 100,000,000 e8s)
        let amount_e8s = (token_amount * 100_000_000.0) as u64;

        if amount_e8s > MAX_AMOUNT_E8S_VIEWS {
            return Err(anyhow::anyhow!("Amount exceeds maximum allowed"));
        }

        let (token_name, memo_tag) = match token_type {
            RewardTokenType::Btc => ("BTC", "BTC_VIEWS"),
            RewardTokenType::Dolr => ("DOLR", "DOLR_VIEWS"),
        };

        // Create transaction memo as JSON string
        let _memo_json = json!({
            "type": "video_view_reward",
            "token": token_name,
            "video_id": video_id,
            "milestone": milestone,
            "amount_inr": amount_inr,
            "timestamp": chrono::Utc::now().timestamp(),
        });

        // Convert memo to bytes for the transaction
        let memo_bytes = memo_tag.to_string().into_bytes();

        log::info!(
            "Transferring {} e8s ({} {}, ₹{}) to creator {} for video {} milestone {}",
            amount_e8s,
            token_amount,
            token_name,
            amount_inr,
            creator_id,
            video_id,
            milestone
        );

        // Transfer using appropriate token operations
        let transfer_result = match token_type {
            RewardTokenType::Btc => {
                self.btc_ops
                    .add_balance_with_memo(creator_id, amount_e8s, Some(memo_bytes))
                    .await
            }
            RewardTokenType::Dolr => {
                self.dolr_ops
                    .add_balance_with_memo(creator_id, amount_e8s, Some(memo_bytes))
                    .await
            }
        };

        match transfer_result {
            Ok(_) => {
                // Generate transaction ID based on current timestamp
                let tx_prefix = match token_type {
                    RewardTokenType::Btc => "btc_tx",
                    RewardTokenType::Dolr => "dolr_tx",
                };
                let tx_id = format!(
                    "{}_{}_{}",
                    tx_prefix,
                    creator_id,
                    chrono::Utc::now().timestamp()
                );

                log::info!(
                    "Successfully transferred {} e8s {} to creator {} (tx_id: {})",
                    amount_e8s,
                    token_name,
                    creator_id,
                    tx_id
                );

                Ok(tx_id)
            }
            Err(e) => {
                log::error!(
                    "Failed to transfer {} e8s {} to creator {}: {}",
                    amount_e8s,
                    token_name,
                    creator_id,
                    e
                );
                Err(anyhow::anyhow!("Token transfer failed: {}", e))
            }
        }
    }
}
