use std::sync::Arc;

use axum::middleware;
use candid::Principal;
use delete_post::handle_delete_post;
use report_post::{
    handle_report_post, handle_report_post_v2, ReportPostRequest, ReportPostRequestV2,
};
use serde::{Deserialize, Serialize};
use tracing::instrument;
use types::PostRequest;
use utoipa::ToSchema;
use utoipa_axum::{
    router::{OpenApiRouter, UtoipaMethodRouterExt},
    routes,
};
use verify::verify_post_request;

use crate::app_state::AppState;
use crate::posts::delete_post::__path_handle_delete_post;
use crate::posts::report_post::{__path_handle_report_post, __path_handle_report_post_v2};

pub mod delete_post;
mod queries;
pub mod report_post;
pub mod types;
mod utils;
mod verify;

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
    router = verified_route!(router, handle_report_post, ReportPostRequest, state);
    router = verified_route!(router, handle_report_post_v2, ReportPostRequestV2, state);

    router.with_state(state)
}

#[derive(Serialize, Deserialize, Clone, ToSchema, Debug)]
pub struct DeletePostRequest {
    #[schema(value_type = String)]
    canister_id: Principal,
    post_id: u64,
    video_id: String,
}
