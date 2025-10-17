use std::sync::Arc;

use axum::middleware;
use candid::Principal;
use delete_post::{handle_delete_post, handle_delete_post_v2};
use report_post::{handle_report_post_v2, ReportPostRequestV2};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use types::PostRequest;
use utoipa::ToSchema;
use utoipa_axum::{
    router::{OpenApiRouter, UtoipaMethodRouterExt},
    routes,
};
use verify::verify_post_request;

use crate::posts::report_post::{__path_handle_report_post_v2, __path_handle_report_post_v3};
use crate::posts::{
    delete_post::{__path_handle_delete_post, __path_handle_delete_post_v2},
    report_post::handle_report_post_v3,
};
use crate::{app_state::AppState, posts::report_post::ReportPostRequestV3};

pub mod delete_post;
pub mod nsfw_query;
mod queries;
pub mod report_post;
pub mod types;
mod utils;
mod verify;
pub mod videohash;

/// Macro to create a route with verification middleware
macro_rules! verified_route {
    ($router:expr, $handler:path, $request_type:ty, $state:expr) => {
        $router.routes(routes!($handler).layer(middleware::from_fn_with_state(
            $state.clone(),
            verify_post_request::<$request_type>,
        )))
    };
}

#[instrument(skip(state))]
pub fn posts_router(state: Arc<AppState>) -> OpenApiRouter {
    let mut router = OpenApiRouter::new();

    router = verified_route!(router, handle_delete_post, DeletePostRequest, state);
    router = verified_route!(router, handle_report_post_v2, ReportPostRequestV2, state);

    router.with_state(state)
}

#[instrument(skip(state))]
pub fn posts_router_v2(state: Arc<AppState>) -> OpenApiRouter {
    let mut router = OpenApiRouter::new();

    router = verified_route!(router, handle_delete_post_v2, DeletePostRequestV2, state);
    router = verified_route!(router, handle_report_post_v3, ReportPostRequestV3, state);

    router
        .routes(routes!(nsfw_query::get_nsfw_data))
        .routes(routes!(videohash::get_videohash))
        .with_state(state)
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
pub struct DeletePostRequest {
    #[schema(value_type = String)]
    canister_id: Principal,
    post_id: u64,
    video_id: String,
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
pub struct DeletePostRequestV2 {
    #[schema(value_type = String)]
    publisher_user_id: Principal,
    post_id: String, // Changed from u64 to String
    video_id: String,
}
