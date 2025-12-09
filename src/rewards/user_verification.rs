use crate::{app_state::AppState, consts::USER_INFO_SERVICE_CANISTER_ID, types::RedisPool};
use anyhow::Result;
use candid::Principal;
use redis::AsyncCommands;
use std::sync::Arc;
use yral_canisters_client::{
    individual_user_template::IndividualUserTemplate,
    user_info_service::{SessionType, UserInfoService},
};

#[derive(Clone)]
pub struct UserVerification {
    redis_pool: RedisPool,
}

impl UserVerification {
    pub fn new(redis_pool: RedisPool) -> Self {
        Self { redis_pool }
    }

    pub async fn is_registered_user(
        &self,
        principal: Principal,
        app_state: &Arc<AppState>,
    ) -> Result<bool> {
        let cache_key = format!("user:registered:{}", principal);

        // Check Redis cache first
        let mut conn = self.redis_pool.get().await?;
        let cached: Option<String> = conn.get(&cache_key).await?;

        if let Some(cached_value) = cached {
            return Ok(cached_value == "true");
        }

        // Cache miss, check with canister
        let is_registered = check_user_registration(principal, app_state).await;

        // Cache the result (fire and forget)
        let cache_key_clone = cache_key.clone();
        let redis_pool = self.redis_pool.clone();
        tokio::spawn(async move {
            if let Ok(mut conn) = redis_pool.get().await {
                let value = if is_registered { "true" } else { "false" };
                // Set with 1 hour TTL (60 seconds)
                log::info!(
                    "Caching user registration status for {}: {}",
                    principal.to_text(),
                    value
                );
                if let Err(e) = conn.set_ex::<_, _, ()>(&cache_key_clone, value, 60).await {
                    log::error!(
                        "Failed to cache user registration status for {}: {}",
                        principal,
                        e
                    );
                }
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
        yral_canisters_client::user_info_service::Result6::Ok(session_type) => {
            matches!(session_type, SessionType::RegisteredSession)
        }
        yral_canisters_client::user_info_service::Result6::Err(e) => {
            if e.contains("User not found") {
                log::debug!(
                    "User {user_principal} not found in user info service, checking individual canister"
                );
                check_individual_canister_registration(user_principal, app_state).await
            } else {
                log::debug!("Failed to get session type for principal {user_principal}: {e}");
                false
            }
        }
    }
}

/// Check individual canister for registration status
async fn check_individual_canister_registration(
    user_principal: Principal,
    app_state: &Arc<AppState>,
) -> bool {
    let canister_id = match app_state
        .get_individual_canister_by_user_principal(user_principal)
        .await
    {
        Ok(id) => id,
        Err(e) => {
            log::debug!("Failed to get individual canister for principal {user_principal}: {e}");
            return false;
        }
    };

    let individual_template = IndividualUserTemplate(canister_id, &app_state.agent);

    match individual_template.get_session_type().await {
        Ok(result) => match result {
            yral_canisters_client::individual_user_template::Result7::Ok(session_type) => {
                matches!(
                    session_type,
                    yral_canisters_client::individual_user_template::SessionType::RegisteredSession
                )
            }
            yral_canisters_client::individual_user_template::Result7::Err(_) => false,
        },
        Err(e) => {
            log::debug!(
                "Failed to get session type from individual canister for {}: {}",
                user_principal,
                e
            );
            false
        }
    }
}
