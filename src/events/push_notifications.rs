use candid::Principal;
use yral_metadata_types::SendNotificationReq;

const METADATA_SERVER_URL: &str = "https://yral-metadata.fly.dev";

#[derive(Clone)]
pub struct NotificationClient {
    api_key: String,
}

impl NotificationClient {
    pub fn new(api_key: String) -> Self {
        Self { api_key }
    }

    pub async fn send_notification(&self, ref data: SendNotificationReq, user_id: Principal) {
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
            log::error!("Error sending notification: {:?}", e);
        }
    }
}