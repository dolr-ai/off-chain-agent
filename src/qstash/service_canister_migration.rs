use std::env;
use std::{error::Error, sync::Arc};

use axum::{extract::State, response::IntoResponse, Json};
use candid::Principal;
use futures::future::join_all;
use http::{header::AUTHORIZATION, StatusCode};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use yral_canisters_client::{
    ic::USER_INFO_SERVICE_ID,
    individual_user_template::{GetPostsOfUserProfileError, IndividualUserTemplate, Result6},
    individual_user_template::{Result7, SessionType},
    user_info_service::{Result_, UserInfoService},
};
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
    State(state): State<Arc<AppState>>,
    Json(request): Json<MigrateIndividualUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let user_metadata = state
        .yral_metadata_client
        .get_user_metadata_v2(request.user_principal.to_text())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((
            StatusCode::INTERNAL_SERVER_ERROR,
            "User metadata not found".to_string(),
        ))?;

    if user_metadata.user_canister_id != request.user_canister {
        return Err((
            StatusCode::BAD_REQUEST,
            "User canister does not match metadata".to_string(),
        ));
    }

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

    service_canister_migration_redis
        .set_migrated_info_for_user(
            request.user_principal,
            MigrationStatus {
                migrated: false,
                individual_user_canister: request.user_canister,
            },
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    state
        .qstash_client
        .transfer_all_posts_to_service_canister(&request)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(())
}

pub async fn transfer_all_posts_for_the_individual_user(
    State(state): State<Arc<AppState>>,
    Json(request): Json<MigrateIndividualUserRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let individual_user_template = IndividualUserTemplate(request.user_canister, &state.agent);

    let mut posts_left = true;

    let mut start_index = 0;

    while posts_left {
        let posts_res = individual_user_template
            .get_posts_of_this_user_profile_with_pagination_cursor(start_index, 100)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        match posts_res {
            Result6::Ok(posts) => {
                let transfer_posts_requests = posts.iter().map(|post_details| async {
                    process_post_for_transfer(
                        post_details.id,
                        request.user_canister,
                        request.user_principal,
                    )
                    .await
                    .map_err(|e| e.to_string())
                });

                let result = join_all(transfer_posts_requests).await;

                if let Some(Err(e)) = result.first() {
                    log::error!("failed to transfer post {e}")
                }
            }
            Result6::Err(e) => {
                if matches!(e, GetPostsOfUserProfileError::ReachedEndOfItemsList) {
                    posts_left = false;
                } else {
                    log::error!("failed to transfer post error from canister {:?}", e);

                    break;
                }
            }
        }

        start_index += 100;
    }

    state
        .qstash_client
        .update_yral_metadata_mapping(&request)
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

#[derive(Serialize, Deserialize, Clone, Copy, Eq, PartialEq)]
pub struct TransferPostRequest {
    post_id: u64,
    canister_id: Principal,
    user_principal: Principal,
}

pub async fn process_post_for_transfer(
    post_id: u64,
    canister_id: Principal,
    user_principal: Principal,
) -> Result<(), Box<dyn Error>> {
    let request_payload = SyncPostToPostServiceRequest {
        user_principal,
        canister_id,
        post_id,
    };

    let yral_cloudflare_worker_token = env::var("YRAL_CLOUDFLARE_WORKER_GRPC_AUTH_TOKEN")?;

    let response_result = reqwest::Client::new()
        .post("https://yral-upload-video.go-bazzinga.workers.dev/sync_post_to_post_canister")
        .header(
            AUTHORIZATION,
            format!("Bearer {}", yral_cloudflare_worker_token),
        )
        .json(&request_payload)
        .send()
        .await;

    match response_result {
        Err(e) => Err(e.into()),
        Ok(response) => {
            if response.status().is_success() {
                Ok(())
            } else {
                let response_error = response.text().await?;

                Err(response_error.into())
            }
        }
    }
}
