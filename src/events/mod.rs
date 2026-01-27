use axum::extract::State;
use axum::response::IntoResponse;
use axum::{middleware, Json};
use event::Event;
use http::{header, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use types::AnalyticsEvent;
use utoipa::ToSchema;
use utoipa_axum::router::{OpenApiRouter, UtoipaMethodRouterExt};
use utoipa_axum::routes;
use verify::verify_event_bulk_request;
use yral_metrics::metrics::sealed_metric::SealedMetric;

use warehouse_events::warehouse_events_server::WarehouseEvents;

use crate::auth::check_auth_events;
use crate::events::verify::verify_event_bulk_request_v3;
use crate::events::warehouse_events::{Empty, WarehouseEvent};
use crate::types::DelegatedIdentityWire;
use crate::AppState;

pub mod warehouse_events {
    tonic::include_proto!("warehouse_events");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("warehouse_events_descriptor");
}

pub mod event;
pub mod nsfw;
pub mod push_notifications;
pub mod queries;
pub mod types;
pub mod utils;
pub mod verify;

/// Convert PascalCase to snake_case (e.g., "VideoDurationWatched" -> "video_duration_watched")
fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 5);
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }
    result
}

pub struct WarehouseEventsService {
    pub shared_state: Arc<AppState>,
}

#[tonic::async_trait]
impl WarehouseEvents for WarehouseEventsService {
    async fn send_event(
        &self,
        request: tonic::Request<WarehouseEvent>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        let shared_state = self.shared_state.clone();

        let request = request.into_inner();
        let event = event::Event::new(request);

        process_event_impl(event, shared_state).await.map_err(|e| {
            log::error!("Failed to process event grpc: {e}");
            tonic::Status::internal("Failed to process event")
        })?;

        Ok(tonic::Response::new(Empty {}))
    }
}

pub fn events_router(state: Arc<AppState>) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(post_event))
        .routes(
            routes!(handle_bulk_events).layer(middleware::from_fn_with_state(
                state.clone(),
                verify_event_bulk_request,
            )),
        )
        .with_state(state)
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
pub struct EventRequest {
    event: String,
    params: String,
}

#[utoipa::path(
    post,
    path = "",
    request_body = EventRequest,
    tag = "events",
    responses(
        (status = 200, description = "Event sent successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    )
)]
async fn post_event(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<EventRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let auth_token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim_start_matches("Bearer ").to_string());

    check_auth_events(auth_token).map_err(|e| (StatusCode::UNAUTHORIZED, e.to_string()))?;

    let warehouse_event = WarehouseEvent {
        event: payload.event,
        params: payload.params,
    };

    let event = Event::new(warehouse_event);

    process_event_impl(event, state.clone())
        .await
        .map_err(|e| {
            log::error!("Failed to process event rest: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to process event".to_string(),
            )
        })?;

    Ok((StatusCode::OK, "Event processed".to_string()))
}

async fn process_event_impl(
    event: Event,
    shared_state: Arc<AppState>,
) -> Result<(), anyhow::Error> {
    #[cfg(not(feature = "local-bin"))]
    event.stream_to_bigquery(&shared_state.clone());

    event.forward_to_mixpanel(&shared_state);

    event.check_video_deduplication(&shared_state.clone());

    event.update_view_count_canister(&shared_state.clone());

    #[cfg(not(feature = "local-bin"))]
    {
        use crate::events::push_notifications::dispatch_notif;

        let params: Value = serde_json::from_str(&event.event.params).map_err(|e| {
            log::error!("Failed to parse params: {e}");
            anyhow::anyhow!("Failed to parse params: {}", e)
        })?;

        let res = dispatch_notif(&event.event.event, params, &shared_state.clone()).await;
        if let Err(e) = res {
            log::error!("Failed to dispatch notification: {e:?}");
        }
    }

    // Process BTC rewards for video views
    if event.event.event == "video_duration_watched" {
        event.process_btc_rewards(&shared_state.clone()).await;
    }

    Ok(())
}

async fn process_event_impl_v2(
    event: Event,
    shared_state: Arc<AppState>,
) -> Result<(), anyhow::Error> {
    #[cfg(not(feature = "local-bin"))]
    event.stream_to_bigquery(&shared_state.clone());

    event.forward_to_mixpanel(&shared_state);

    event.check_video_deduplication(&shared_state.clone());

    event.update_view_count_canister(&shared_state.clone());

    #[cfg(not(feature = "local-bin"))]
    {
        use crate::events::push_notifications::dispatch_notif;

        let params: Value = serde_json::from_str(&event.event.params).map_err(|e| {
            log::error!("Failed to parse params: {e}");
            anyhow::anyhow!("Failed to parse params: {}", e)
        })?;

        let res = dispatch_notif(&event.event.event, params, &shared_state.clone()).await;
        if let Err(e) = res {
            log::error!("Failed to dispatch notification: {e:?}");
        }
    }

    // Process BTC rewards for video views
    if event.event.event == "video_duration_watched" {
        event.process_btc_rewards(&shared_state.clone()).await;
    }

    Ok(())
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct EventBulkRequest {
    pub delegated_identity_wire: DelegatedIdentityWire,
    pub events: Vec<AnalyticsEvent>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct VerifiedEventBulkRequest {
    pub events: Vec<AnalyticsEvent>,
}

#[utoipa::path(
    post,
    path = "/bulk",
    request_body = EventBulkRequest,
    tag = "events",
    responses(
        (status = 200, description = "Bulk event success"),
        (status = 400, description = "Bulk event failed"),
        (status = 500, description = "Internal server error"),
        (status = 403, description = "Forbidden"),
    )
)]
async fn handle_bulk_events(
    State(state): State<Arc<AppState>>,
    Json(request): Json<VerifiedEventBulkRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    for req_event in request.events {
        let event = Event::new(WarehouseEvent {
            event: req_event.tag(),
            params: req_event.params().to_string(),
        });

        if let Err(e) = process_event_impl(event, state.clone()).await {
            log::error!("Failed to process event rest: {e}"); // not sending any error to the client as it is a bulk request
        }
    }

    Ok((StatusCode::OK, "Events processed".to_string()))
}

/// V2 bulk event request with delegated identity auth and arbitrary payloads
#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct EventBulkRequestV2 {
    pub delegated_identity_wire: DelegatedIdentityWire,
    /// Array of event payloads - each must contain "event" field for the event name
    #[schema(value_type = Vec<Object>)]
    pub events: Vec<Value>,
}

/// V2 verified bulk events (after middleware validation)
#[derive(Clone, Serialize, Deserialize)]
pub struct VerifiedEventBulkRequestV2 {
    pub events: Vec<Value>,
}

#[utoipa::path(
    post,
    path = "/bulk",
    request_body = EventBulkRequestV2,
    tag = "events",
    responses(
        (status = 200, description = "Bulk event success"),
        (status = 400, description = "Bulk event failed"),
        (status = 500, description = "Internal server error"),
        (status = 403, description = "Forbidden"),
    )
)]
async fn handle_bulk_events_v2(
    State(state): State<Arc<AppState>>,
    Json(request): Json<VerifiedEventBulkRequestV2>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    for mut payload in request.events {
        // Extract event name and convert PascalCase to snake_case for backwards compat
        let event_name = payload
            .get("event")
            .and_then(|v| v.as_str())
            .map(to_snake_case)
            .unwrap_or_else(|| "unknown".to_string());

        // Remove "event" field from params (old AnalyticsEventV3.params() didn't include it)
        if let Value::Object(ref mut map) = payload {
            map.remove("event");
        }

        let event = Event::new(WarehouseEvent {
            event: event_name,
            params: payload.to_string(),
        });

        if let Err(e) = process_event_impl_v2(event, state.clone()).await {
            log::error!("Failed to process event rest: {e}"); // not sending any error to the client as it is a bulk request
        }
    }

    Ok((StatusCode::OK, "Events processed".to_string()))
}

#[utoipa::path(
    post,
    path = "",
    request_body = EventRequest,
    tag = "events",
    responses(
        (status = 200, description = "Event sent successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    )
)]
async fn post_event_v2(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<EventRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    // Convert event name to snake_case for backwards compat with mobile sending PascalCase
    let event_name = to_snake_case(&payload.event);

    let warehouse_event = WarehouseEvent {
        event: event_name,
        params: payload.params,
    };

    let event = Event::new(warehouse_event);

    process_event_impl_v2(event, state.clone())
        .await
        .map_err(|e| {
            log::error!("Failed to process event rest: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to process event".to_string(),
            )
        })?;

    Ok((StatusCode::OK, "Event processed".to_string()))
}

pub fn events_router_v2(state: Arc<AppState>) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(post_event_v2))
        .routes(
            routes!(handle_bulk_events_v2).layer(middleware::from_fn_with_state(
                state.clone(),
                verify_event_bulk_request_v3,
            )),
        )
        .with_state(state)
}
