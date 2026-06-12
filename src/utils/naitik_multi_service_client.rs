use crate::{consts::NAITIK_YRAL_MULTI_SERVICES, events::EventRequest};


#[derive(Clone)]
pub struct NaitikMultiServiceClient {
    client: reqwest::Client,
    base_url: reqwest::Url,
}

impl NaitikMultiServiceClient {
    pub fn new() -> Self {
        let client = reqwest::Client::new();
        let base_url = reqwest::Url::parse(&NAITIK_YRAL_MULTI_SERVICES.to_string()).expect("Invalid recsys endpoint URL");

        let secret = std::env::var("NAITIK_MULTI_SERVICE_API_JWT_TOKEN").unwrap_or_default();
        if secret.is_empty() {
            log::error!("NAITIK_MULTI_SERVICE_API_JWT_TOKEN is not set");
        } else {
            log::info!("NAITIK_MULTI_SERVICE_API_JWT_TOKEN is set");
        }
        Self { client, base_url }
    }

    pub async fn send_event_to_naitik_multi_services(&self, event: EventRequest ) -> Result<(), anyhow::Error> {


        Ok(())
    }

    pub async fn send_bulk_events_to_naitik_multi_services(&self, events: VerifiedEventBulkRequest ) -> Result<(), anyhow::Error> {


        Ok(())
    }

    pub async fn send_bulk_events_v2_to_naitik_multi_services(&self, events: VerifiedEventBulkRequestV2 ) -> Result<(), anyhow::Error> {


        Ok(())
    }
}