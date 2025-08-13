use axum::extract::State;
use axum::response::IntoResponse;
use axum::{middleware, Json};
use candid::Principal;
use event::Event;
use http::{header, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::error::Error;
use std::sync::Arc;
use types::AnalyticsEvent;
use utoipa::ToSchema;
use utoipa_axum::router::{OpenApiRouter, UtoipaMethodRouterExt};
use utoipa_axum::routes;
use verify::verify_event_bulk_request;
use yral_metrics::metrics::sealed_metric::SealedMetric;

use warehouse_events::warehouse_events_server::WarehouseEvents;

use crate::auth::check_auth_events;
use crate::events::types::AnalyticsEventV3;
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

    event.check_video_deduplication(&shared_state.clone());

    event.update_watch_history(&shared_state.clone());
    event.update_success_history(&shared_state.clone());

    event.update_watch_history_v2(&shared_state.clone());
    event.update_success_history_v2(&shared_state.clone());

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

    Ok(())
}

async fn process_event_impl_v2(
    event: Event,
    shared_state: Arc<AppState>,
) -> Result<(), anyhow::Error> {
    #[cfg(not(feature = "local-bin"))]
    event.stream_to_bigquery(&shared_state.clone());

    event.check_video_deduplication(&shared_state.clone());

    event.update_watch_history_v3(&shared_state.clone());
    event.update_success_history_v3(&shared_state.clone());

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
    let mut metric_events = Vec::new();
    for req_event in request.events {
        let event = Event::new(WarehouseEvent {
            event: req_event.tag(),
            params: req_event.params().to_string(),
        });

        metric_events.push(req_event);

        if let Err(e) = process_event_impl(event, state.clone()).await {
            log::error!("Failed to process event rest: {e}"); // not sending any error to the client as it is a bulk request
        }
    }

    if let Err(e) = state
        .metrics
        .push_list("metrics_list".into(), metric_events)
        .await
    {
        log::error!("Failed to push metrics to vector: {e}");
    }

    Ok((StatusCode::OK, "Events processed".to_string()))
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct EventBulkRequestV3 {
    pub delegated_identity_wire: DelegatedIdentityWire,
    pub events: Vec<AnalyticsEventV3>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct VerifiedEventBulkRequestV3 {
    pub events: Vec<AnalyticsEventV3>,
}

#[utoipa::path(
    post,
    path = "/bulk",
    request_body = EventBulkRequestV3,
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
    Json(request): Json<VerifiedEventBulkRequestV3>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    for req_event in request.events {
        let event = Event::new(WarehouseEvent {
            event: req_event.tag(),
            params: req_event.params().to_string(),
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
                verify_event_bulk_request,
            )),
        )
        .with_state(state)
}
