use crate::{
    consts::NAITIK_YRAL_MULTI_SERVICES,
    events::{EventRequest, VerifiedEventBulkRequest, VerifiedEventBulkRequestV2},
};


#[derive(Clone)]
pub struct NaitikMultiServiceClient {
    client: reqwest::Client,
    base_url: reqwest::Url,
    jwt_token: String,
}

impl NaitikMultiServiceClient {
    pub fn new() -> Self {
        let client = reqwest::Client::new();
        let base_url = reqwest::Url::parse(&NAITIK_YRAL_MULTI_SERVICES.to_string()).expect("Invalid recsys endpoint URL");

        let jwt_token = std::env::var("NAITIK_MULTI_SERVICE_API_JWT_TOKEN").unwrap_or_default();
        if jwt_token.is_empty() {
            log::error!("NAITIK_MULTI_SERVICE_API_JWT_TOKEN is not set");
        } else {
            log::info!("NAITIK_MULTI_SERVICE_API_JWT_TOKEN is set");
        }
        Self {
            client,
            base_url,
            jwt_token,
        }
    }

    pub async fn send_event_v1_to_naitik_multi_services(
        &self,
        event: EventRequest,
    ) -> Result<(), anyhow::Error> {
        let url = self.base_url.join("/api/v1/events")?;

        let response = self
            .client
            .post(url)
            .bearer_auth(&self.jwt_token)
            .json(&event)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            log::error!(
                "Failed to send event to naitik multi services: {} - {}",
                status,
                text
            );
            return Err(anyhow::anyhow!("Failed to send event: {}", status));
        }

        Ok(())
    }

    pub async fn send_event_v2_to_naitik_multi_services(
        &self,
        event: EventRequest,
    ) -> Result<(), anyhow::Error> {
        let url = self.base_url.join("/api/v2/events")?;

        let response = self
            .client
            .post(url)
            .bearer_auth(&self.jwt_token)
            .json(&event)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            log::error!(
                "Failed to send event to naitik multi services: {} - {}",
                status,
                text
            );
            return Err(anyhow::anyhow!("Failed to send event: {}", status));
        }

        Ok(())
    }

    pub async fn send_bulk_events_v1_to_naitik_multi_services(
        &self,
        events: VerifiedEventBulkRequest,
    ) -> Result<(), anyhow::Error> {
        let url = self.base_url.join("/api/v1/events/bulk")?;

        let response = self
            .client
            .post(url)
            .bearer_auth(&self.jwt_token)
            .json(&events)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            log::error!(
                "Failed to send bulk events to naitik multi services: {} - {}",
                status,
                text
            );
            return Err(anyhow::anyhow!("Failed to send bulk events: {}", status));
        }

        Ok(())
    }

    pub async fn send_bulk_events_v2_to_naitik_multi_services(
        &self,
        events: VerifiedEventBulkRequestV2,
    ) -> Result<(), anyhow::Error> {
        let url = self.base_url.join("/api/v2/events/bulk")?;

        let response = self
            .client
            .post(url)
            .bearer_auth(&self.jwt_token)
            .json(&events)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            log::error!(
                "Failed to send bulk events v2 to naitik multi services: {} - {}",
                status,
                text
            );
            return Err(anyhow::anyhow!("Failed to send bulk events v2: {}", status));
        }

        Ok(())
    }
}