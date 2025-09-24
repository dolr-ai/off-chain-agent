use anyhow::Result;
use candid::Principal;
use num_traits::ToPrimitive;
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

        // Create transaction memo as JSON string
        let _memo_json = json!({
            "type": "video_view_reward",
            "video_id": video_id,
            "milestone": milestone,
            "amount_inr": amount_inr,
            "timestamp": chrono::Utc::now().timestamp(),
        });

        log::info!(
            "Transferring {} sats ({} BTC, ₹{}) to creator {} for video {} milestone {}",
            amount_sats,
            amount_btc,
            amount_inr,
            creator_id,
            video_id,
            milestone
        );

        // Transfer ckBTC using add_balance (which transfers from admin to user)
        match self.ckbtc_ops.add_balance(creator_id, amount_sats).await {
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

    /// Process pending transactions
    pub async fn _process_pending_transactions(&self) -> Result<Vec<String>> {
        // TODO: Implement batch processing of pending transactions
        // This would:
        // 1. Query pending transactions from storage
        // 2. Process them in batches
        // 3. Update their status

        log::debug!("Processing pending BTC transactions");
        Ok(vec![])
    }

    /// Get transaction status
    pub async fn _get_transaction_status(&self, tx_id: &str) -> Result<TransactionStatus> {
        // TODO: Query actual transaction status from wallet/blockchain
        log::debug!("Getting status for transaction {}", tx_id);

        // Mock implementation
        Ok(TransactionStatus::Completed)
    }

    /// Credit BTC to creator's wallet (same as queue_btc_reward but with custom memo)
    pub async fn _credit_btc_to_wallet(
        &self,
        creator_id: Principal,
        amount_btc: f64,
        memo: String,
    ) -> Result<String> {
        // Convert BTC to satoshis
        let amount_sats = (amount_btc * 100_000_000.0) as u64;

        log::info!(
            "Crediting {} sats ({} BTC) to creator {} with memo: {}",
            amount_sats,
            amount_btc,
            creator_id,
            memo
        );

        // Transfer ckBTC
        self.ckbtc_ops
            .add_balance(creator_id, amount_sats)
            .await
            .map_err(|e| anyhow::anyhow!("ckBTC transfer failed: {}", e))?;

        let tx_id = format!("ckbtc_tx_{}", chrono::Utc::now().timestamp());
        Ok(tx_id)
    }

    /// Verify wallet address exists (all principals can receive ckBTC)
    pub async fn _verify_wallet_exists(&self, creator_id: Principal) -> Result<bool> {
        // All valid principals can receive ckBTC on the IC
        log::debug!("Verifying wallet existence for creator {}", creator_id);
        Ok(true)
    }

    /// Get wallet balance in BTC
    pub async fn _get_wallet_balance(&self, creator_id: Principal) -> Result<f64> {
        log::debug!("Getting wallet balance for creator {}", creator_id);

        // Get balance in smallest units (e8s)
        let balance = self
            .ckbtc_ops
            .load_balance(creator_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to load balance: {}", e))?;

        // Convert from e8s (satoshis) to BTC
        // balance.e8s is the amount in satoshis
        let btc_amount = balance.e8s.0.to_f64().unwrap_or(0.0) / 100_000_000.0;

        Ok(btc_amount)
    }
}

/// Helper function to format transaction memo
pub fn _create_transaction_memo(video_id: &str, milestone: u64, timestamp: i64) -> String {
    json!({
        "type": "video_view_reward",
        "video_id": video_id,
        "milestone": milestone,
        "timestamp": timestamp,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_memo() {
        let memo = _create_transaction_memo("video_123", 100, 1234567890);
        assert!(memo.contains("video_123"));
        assert!(memo.contains("100"));
        assert!(memo.contains("1234567890"));
    }
}
