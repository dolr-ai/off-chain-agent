use std::{error::Error, sync::Arc};

use axum::{extract::State, response::IntoResponse, Json};
use candid::Principal;
use http::StatusCode;
use ic_agent::Identity;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use yral_canisters_client::{
    ic::USER_INFO_SERVICE_ID,
    individual_user_template::{IndividualUserTemplate, Result7, SessionType},
    user_info_service::{Result_, UserInfoService},
};
use yral_metadata_client::MetadataClient;
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

impl MigrationStatus {
    fn new(individual_user_canister: Principal) -> Self {
        MigrationStatus {
            individual_user_canister,
            migrated: false,
        }
    }
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

pub async fn update_the_metadata_mapping_impl(
    yral_metadata_client: &MetadataClient<true>,
    admin_identity: &impl Identity,
    migrate_request: MigrateIndividualUserRequest,
) -> Result<(), Box<dyn Error>> {
    let user_metadata = yral_metadata_client
        .get_user_metadata_v2(migrate_request.user_principal.to_text())
        .await
        .map_err(|e| e.to_string())?
        .ok_or("User metadata not found")?;

    yral_metadata_client
        .admin_set_user_metadata(
            admin_identity,
            migrate_request.user_principal,
            SetUserMetadataReqMetadata {
                user_canister_id: USER_INFO_SERVICE_ID,
                user_name: String::new(),
            },
        )
        .await?;

    Ok(())
}

pub async fn migrate_individual_user_to_service_canister(
    State(state): State<Arc<AppState>>,
    Json(request): Json<MigrateIndividualUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let user_info_canister = UserInfoService(USER_INFO_SERVICE_ID, &state.agent);

    let individual_user_service = IndividualUserTemplate(request.user_canister, &state.agent);

    let session_type_res = individual_user_service
        .get_session_type()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let session_type = match session_type_res {
        Result7::Ok(session_type) => Ok(session_type),
        Result7::Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e)),
    }?;

    let registered_session = matches!(session_type, SessionType::RegisteredSession);

    let accept_new_registration_res = user_info_canister
        .accept_new_user_registration(request.user_principal, registered_session)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    if let Result_::Err(e) = accept_new_registration_res {
        return Err((StatusCode::INTERNAL_SERVER_ERROR, e));
    }

    let service_canister_migration_redis =
        ServiceCanisterMigrationRedis::new(state.service_cansister_migration_redis_pool.clone());

    let mut migration_info = MigrationStatus::new(request.user_canister);

    service_canister_migration_redis
        .set_migrated_info_for_user(request.user_principal, migration_info)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    update_the_metadata_mapping_impl(&state.yral_metadata_client, &state.admin_identity, request)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    migration_info.migrated = true;

    service_canister_migration_redis
        .set_migrated_info_for_user(request.user_principal, migration_info)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SyncPostToPostServiceRequest {
    user_principal: Principal,
    canister_id: Principal,
    post_id: u64,
}
