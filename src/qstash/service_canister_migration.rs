use std::{error::Error, sync::Arc};

use axum::{extract::State, response::IntoResponse, Json};
use candid::Principal;
use http::StatusCode;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use yral_canisters_client::ic::USER_INFO_SERVICE_ID;
use yral_metadata_types::SetUserMetadataReqMetadata;

use crate::{app_state::AppState, types::RedisPool};

#[derive(Serialize, Deserialize, Copy, Clone, Eq, PartialEq, PartialOrd, Debug)]
pub struct MigrateIndividualUserRequest {
    pub user_canister: Principal,
    pub user_principal: Principal,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct MigrationStatus {
    pub individual_user_canister: Principal,
    pub migrated: bool,
}

#[derive(Clone, Debug)]
pub struct ServiceCanisterMigrationRedis {
    pool: RedisPool,
}

impl ServiceCanisterMigrationRedis {
    fn new(pool: RedisPool) -> Self {
        Self { pool }
    }

    pub async fn set_migrated_info_for_user(
        &self,
        user_principal: Principal,
        migration_info: MigrationStatus,
    ) -> Result<(), Box<dyn Error>> {
        let mut conn = self.pool.get().await?;
        conn.set::<_, _, ()>(
            user_principal.to_text(),
            serde_json::to_string(&migration_info)?,
        )
        .await?;

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn get_migration_info_for_user(
        &self,
        user_principal: Principal,
    ) -> Result<Option<MigrationStatus>, Box<dyn Error>> {
        let mut conn = self.pool.get().await?;
        let migration_info_str_opt: Option<String> = conn.get(user_principal.to_text()).await?;

        match migration_info_str_opt {
            Some(data_str) => {
                let migration_status: MigrationStatus = serde_json::from_str(&data_str)?;
                Ok(Some(migration_status))
            }
            None => Ok(None),
        }
    }
}

pub async fn update_the_metadata_mapping(
    State(state): State<Arc<AppState>>,
    Json(request): Json<MigrateIndividualUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let admin_identity = &state.admin_identity;

    let _user_metadata = state
        .yral_metadata_client
        .get_user_metadata_v2(request.user_principal.to_text())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((
            StatusCode::INTERNAL_SERVER_ERROR,
            "User metadata not found".to_string(),
        ))?;

    state
        .yral_metadata_client
        .admin_set_user_metadata(
            admin_identity,
            request.user_principal,
            SetUserMetadataReqMetadata {
                user_canister_id: USER_INFO_SERVICE_ID,
                user_name: String::new(),
            },
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let service_canister_migration_redis =
        ServiceCanisterMigrationRedis::new(state.service_cansister_migration_redis_pool.clone());

    service_canister_migration_redis
        .set_migrated_info_for_user(
            request.user_principal,
            MigrationStatus {
                migrated: true,
                individual_user_canister: request.user_canister,
            },
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(())
}

pub async fn migrate_individual_user_to_service_canister(
    State(_state): State<Arc<AppState>>,
    Json(_request): Json<MigrateIndividualUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // TODO: migrate to user_post_service/user_info_service
    // Previously used IndividualUserTemplate to get session_type and register
    // the user in UserInfoService. Individual user canisters have been
    // decommissioned — this migration endpoint is no longer needed.
    log::warn!("migrate_individual_user_to_service_canister is deprecated — \
                individual user canisters have been decommissioned");
    Ok((StatusCode::OK, "Migration no longer needed".to_string()))
}

pub async fn transfer_all_posts_for_the_individual_user(
    State(_state): State<Arc<AppState>>,
    Json(_request): Json<MigrateIndividualUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // TODO: migrate to user_post_service/user_info_service
    // Previously used IndividualUserTemplate to fetch posts from individual user
    // canisters and transfer them to UserPostService. Individual user canisters
    // have been decommissioned — this transfer endpoint is no longer needed.
    log::warn!("transfer_all_posts_for_the_individual_user is deprecated — \
                individual user canisters have been decommissioned");
    Ok((StatusCode::OK, "Transfer no longer needed".to_string()))
}
