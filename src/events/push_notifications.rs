use candid::Principal;
use serde_json::Value;
use yral_canisters_client::notification_store::{
    LikedPayload, NotificationStore, NotificationType, VideoApprovalPayload, VideoUploadPayload,
};
use yral_metadata_types::SendNotificationReq;

use crate::{
    app_state::AppState,
    events::types::{deserialize_event_payload, EventPayload},
};

const METADATA_SERVER_URL: &str = "https://metadata.yral.com";

#[derive(Clone)]
pub struct NotificationClient {
    api_key: String,
}

impl NotificationClient {
    pub fn new(api_key: String) -> Self {
        Self { api_key }
    }

    pub async fn send_notification(&self, data: SendNotificationReq, user_id: Principal) {
        let client = reqwest::Client::new();
        let url = format!(
            "{}/notifications/{}/send",
            METADATA_SERVER_URL,
            user_id.to_text()
        );

        let res = client
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(&data)
            .send()
            .await;

        if let Err(e) = res {
            log::error!("Error sending notification: {e:?}");
        }
    }
}

const NOTIFICATION_EVENTS: &[&str] = &[
    "video_upload_successful",
    "like_video",
    "video_approved",
    "video_disapproved",
    "tournament_started",
    "tournament_ended_winner",
    "reward_earned",
    "follow_user",
];

pub async fn dispatch_notif(
    event_type: &str,
    params: Value,
    app_state: &AppState,
) -> Result<(), Box<dyn std::error::Error>> {
    if !NOTIFICATION_EVENTS.contains(&event_type) {
        return Ok(());
    }

    let event = deserialize_event_payload(event_type, params)?;

    event.send_notification(app_state).await;
    let notification_store = NotificationStore(
        Principal::from_text("mlj75-eyaaa-aaaaa-qbn5q-cai").unwrap(),
        &app_state.agent,
    );

    match event {
        EventPayload::VideoUploadSuccessful(payload) => {
            notification_store
                .add_notification(
                    payload.publisher_user_id,
                    NotificationType::VideoUpload(VideoUploadPayload {
                        video_uid: payload.post_id,
                    }),
                )
                .await?;
        }
        EventPayload::LikeVideo(payload) => {
            notification_store
                .add_notification(
                    payload.publisher_user_id,
                    NotificationType::Liked(LikedPayload {
                        post_id: payload.post_id,
                        by_user_principal: payload.user_id,
                    }),
                )
                .await?;
        }
        EventPayload::VideoApproved(payload) => {
            notification_store
                .add_notification(
                    payload.user_id,
                    NotificationType::VideoApproved(VideoApprovalPayload {
                        video_id: payload.video_id,
                        post_id: payload.post_id,
                    }),
                )
                .await?;
        }
        EventPayload::VideoDisapproved(payload) => {
            notification_store
                .add_notification(
                    payload.user_id,
                    NotificationType::VideoDisapproved(VideoApprovalPayload {
                        video_id: payload.video_id,
                        post_id: payload.post_id,
                    }),
                )
                .await?;
        }
        _ => {}
    }
    Ok(())
}
