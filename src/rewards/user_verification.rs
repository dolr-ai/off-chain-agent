use crate::{
    app_state::AppState, consts::USER_INFO_SERVICE_CANISTER_ID, yral_auth::dragonfly::DragonflyPool,
};
use anyhow::Result;
use candid::Principal;
use redis::AsyncCommands;
use std::sync::Arc;
use yral_canisters_client::user_info_service::{SessionType, UserInfoService};

#[derive(Clone)]
pub struct UserVerification {
    dragonfly_redis_store: Arc<DragonflyPool>,
}

impl UserVerification {
    pub fn new(dragonfly_redis_store: Arc<DragonflyPool>) -> Self {
        Self {
            dragonfly_redis_store,
        }
    }

    pub async fn is_registered_user(
        &self,
        principal: Principal,
        app_state: &Arc<AppState>,
    ) -> Result<bool> {
        let cache_key = format!("impressions:user:registered:{}", principal);

        // Check Dragonfly cache first
        let cached: Option<String> = self
            .dragonfly_redis_store
            .execute_with_retry(|mut conn| {
                let key = cache_key.clone();
                async move { conn.get(&key).await }
            })
            .await?;

        if let Some(cached_value) = cached {
            return Ok(cached_value == "true");
        }

        // Cache miss, check with canister
        let is_registered = check_user_registration(principal, app_state).await;

        // Cache the result (fire and forget)
        let cache_key_clone = cache_key.clone();
        let dragonfly_redis_store = self.dragonfly_redis_store.clone();
        let principal_text = principal.to_text();
        tokio::spawn(async move {
            let value = if is_registered { "true" } else { "false" };
            let value_str = value.to_string();

            // Set with 1 minute TTL (60 seconds)
            log::info!(
                "Caching user registration status for {}: {}",
                principal_text,
                value
            );

            if let Err(e) = dragonfly_redis_store
                .execute_with_retry(|mut conn| {
                    let key = cache_key_clone.clone();
                    let val = value_str.clone();
                    async move { conn.set_ex::<_, _, ()>(&key, val, 60).await }
                })
                .await
            {
                log::error!(
                    "Failed to cache user registration status for {}: {}",
                    principal_text,
                    e
                );
            }
        });

        Ok(is_registered)
    }
}

/// Checks if user is registered via user info service (reusing existing logic)
async fn check_user_registration(user_principal: Principal, app_state: &Arc<AppState>) -> bool {
    let user_info_service = UserInfoService(*USER_INFO_SERVICE_CANISTER_ID, &app_state.agent);

    let result = match user_info_service
        .get_user_session_type(user_principal)
        .await
    {
        Ok(result) => result,
        Err(e) => {
            log::debug!("Failed to get session type for principal {user_principal}: {e}");
            return false;
        }
    };

    match result {
        yral_canisters_client::user_info_service::Result8::Ok(session_type) => {
            matches!(session_type, SessionType::RegisteredSession)
        }
        yral_canisters_client::user_info_service::Result8::Err(e) => {
            if e.contains("User not found") {
                // TODO: migrate to user_post_service/user_info_service
                // Previously fell back to IndividualUserTemplate.get_session_type().
                // Individual user canisters have been decommissioned — if the user
                // is not found in user_info_service, they are not registered.
                log::debug!(
                    "User {user_principal} not found in user info service — treating as not registered"
                );
                false
            } else {
                log::debug!("Failed to get session type for principal {user_principal}: {e}");
                false
            }
        }
    }
}
