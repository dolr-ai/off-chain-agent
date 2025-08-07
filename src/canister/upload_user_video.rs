use std::{error::Error, sync::Arc};

use axum::{extract::State, Json};
use ic_agent::{identity::DelegatedIdentity, Agent, Identity};
use serde::{Deserialize, Serialize};

use crate::{
    app_state::AppState, events::VideoUploadSuccessful, types::DelegatedIdentityWire,
    utils::api_response::ApiResponse,
};

use yral_canisters_client::individual_user_template::{
    IndividualUserTemplate, PostDetailsFromFrontend, Result1,
};

#[derive(Serialize, Deserialize, Clone)]
pub struct UploadUserVideoRequestBody {
    delegated_identity_wire: DelegatedIdentityWire,
    post_details: PostDetails,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PostDetails {
    pub is_nsfw: bool,
    pub hashtags: Vec<String>,
    pub description: String,
    pub video_uid: String,
    pub creator_consent_for_inclusion_in_hot_or_not: bool,
}

impl From<PostDetails> for PostDetailsFromFrontend {
    fn from(value: PostDetails) -> Self {
        Self {
            is_nsfw: value.is_nsfw,
            hashtags: value.hashtags,
            description: value.description,
            video_uid: value.video_uid,
            creator_consent_for_inclusion_in_hot_or_not: value
                .creator_consent_for_inclusion_in_hot_or_not,
        }
    }
}

pub struct UploadUserVideoResData;

pub async fn upload_user_video_handler(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<UploadUserVideoRequestBody>,
) -> Json<ApiResponse<u64>> {
    let upload_video_result = upload_user_video_impl(app_state.clone(), payload).await;

    Json(ApiResponse::from(upload_video_result))
}

pub async fn upload_user_video_impl(
    app_state: Arc<AppState>,
    payload: UploadUserVideoRequestBody,
) -> Result<u64, Box<dyn Error + Send + Sync>> {
    let yral_metadata_client = &app_state.yral_metadata_client;
    let identity: DelegatedIdentity = payload
        .delegated_identity_wire
        .try_into()
        .map_err(|e: k256::elliptic_curve::Error| e.to_string())?;
    let user_principal = identity.sender()?;

    let agent = Agent::builder()
        .with_identity(identity)
        .with_url("https://ic0.app")
        .build()?;
    let user_meta_data = yral_metadata_client
        .get_user_metadata(user_principal)
        .await?
        .ok_or("metadata for principal not found")?;
    let individual_user_template = IndividualUserTemplate(user_meta_data.user_canister_id, &agent);

    let upload_video_res = individual_user_template
        .add_post_v_2(PostDetailsFromFrontend::from(payload.post_details.clone()))
        .await?;

    match upload_video_res {
        Result1::Ok(post_id) => {
            let upload_video_event = VideoUploadSuccessful {
                shared_state: app_state.clone(),
            };

            let upload_event_result = upload_video_event
                .send_event(
                    user_principal,
                    user_meta_data.user_name,
                    payload.post_details.video_uid,
                    payload.post_details.hashtags.len(),
                    payload.post_details.is_nsfw,
                    payload
                        .post_details
                        .creator_consent_for_inclusion_in_hot_or_not,
                    post_id,
                )
                .await;

            if let Err(e) = upload_event_result {
                println!("Error in sending event upload_video_successful {e}");
            }

            Ok(post_id)
        }
        Result1::Err(e) => Err(e.into()),
    }
}
