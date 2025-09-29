use anyhow::Result;
use candid::Principal;
use serde::{Deserialize, Serialize};
use serde_json::json;
use yral_canisters_common::utils::token::{CkBtcOperations, TokenOperations};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BtcTransaction {
    pub creator_id: Principal,
    pub video_id: String,
    pub milestone: u64,
    pub amount_btc: f64,
    pub amount_inr: f64,
    pub timestamp: i64,
    pub tx_id: Option<String>,
    pub status: TransactionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TransactionStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

#[derive(Clone)]
pub struct WalletIntegration {
    ckbtc_ops: CkBtcOperations,
}

const MAX_AMOUNT_SATS_BTC_VIEWS: u64 = 4000;

impl WalletIntegration {
    pub fn new(admin_agent: ic_agent::Agent) -> Self {
        Self {
            ckbtc_ops: CkBtcOperations::new(admin_agent),
        }
    }

    /// Queue a BTC reward transaction for processing
    pub async fn queue_btc_reward(
        &self,
        creator_id: Principal,
        amount_btc: f64,
        amount_inr: f64,
        video_id: &str,
        milestone: u64,
    ) -> Result<String> {
        // Convert BTC to satoshis (1 BTC = 100,000,000 sats)
        let amount_sats = (amount_btc * 100_000_000.0) as u64;

        if amount_sats > MAX_AMOUNT_SATS_BTC_VIEWS {
            return Err(anyhow::anyhow!("Amount exceeds maximum allowed"));
        }

        // Create transaction memo as JSON string
        let memo_json = json!({
            "type": "video_view_reward",
            "video_id": video_id,
            "milestone": milestone,
            "amount_inr": amount_inr,
            "timestamp": chrono::Utc::now().timestamp(),
        });

        // Convert memo to bytes for the transaction
        let memo_bytes = memo_json.to_string().into_bytes();

        log::info!(
            "Transferring {} sats ({} BTC, ₹{}) to creator {} for video {} milestone {}",
            amount_sats,
            amount_btc,
            amount_inr,
            creator_id,
            video_id,
            milestone
        );

        // Transfer ckBTC using add_balance_with_memo (which transfers from admin to user)
        match self
            .ckbtc_ops
            .add_balance_with_memo(creator_id, amount_sats, Some(memo_bytes))
            .await
        {
            Ok(_) => {
                // Generate transaction ID based on current timestamp
                let tx_id = format!("ckbtc_tx_{}_{}", creator_id, chrono::Utc::now().timestamp());

                log::info!(
                    "Successfully transferred {} sats to creator {} (tx_id: {})",
                    amount_sats,
                    creator_id,
                    tx_id
                );

                Ok(tx_id)
            }
            Err(e) => {
                log::error!(
                    "Failed to transfer {} sats to creator {}: {}",
                    amount_sats,
                    creator_id,
                    e
                );
                Err(anyhow::anyhow!("ckBTC transfer failed: {}", e))
            }
        }
    }
}
