use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::extract::DefaultBodyLimit;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{routing::get, Router};
use canister::canister_health_handler;
use config::AppConfig;
use events::event::storj::enqueue_storj_backfill_item;
use http::header::CONTENT_TYPE;
use offchain_service::report_approved_handler;
use qstash::qstash_router;
use sentry_tower::{NewSentryLayer, SentryHttpLayer};
use tonic::service::Routes;
use tower::make::Shared;
use tower::steer::Steer;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::instrument;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_swagger_ui::SwaggerUi;

use crate::auth::check_auth_grpc;
use crate::events::warehouse_events::warehouse_events_server::WarehouseEventsServer;
use crate::events::{warehouse_events, WarehouseEventsService};
use crate::offchain_service::off_chain::off_chain_server::OffChainServer;
use crate::offchain_service::{off_chain, OffChainService};
use error::*;

mod app_state;
mod auth;
pub mod canister;
mod config;
mod consts;
mod duplicate_video;
mod error;
mod events;
pub mod kvrocks;
pub mod leaderboard;
pub mod metrics;
mod middleware;
#[cfg(not(feature = "local-bin"))]
mod milvus;
mod moderation;
mod offchain_service;
pub mod pipeline;
mod posts;
mod qstash;
mod rewards;
pub mod scratchpad;
mod types;
pub mod user;
pub mod utils;
pub mod videogen;
pub mod yral_auth;

use app_state::AppState;

async fn main_impl() -> Result<()> {
    #[derive(OpenApi)]
    #[openapi(
        tags(
            (name = "OFF_CHAIN", description = "Off Chain Agent API"),
        )
    )]
    struct ApiDoc;

    let conf = AppConfig::load()?;

    let shared_state = Arc::new(AppState::new(conf.clone()).await);

    let sentry_tower_layer = ServiceBuilder::new()
        .layer(NewSentryLayer::new_from_top())
        .layer(SentryHttpLayer::with_transaction());

    let router = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .nest("/api/v1/posts", posts::posts_router(shared_state.clone()))
        .nest(
            "/api/v1/events",
            events::events_router(shared_state.clone()),
        )
        .nest("/api/v1/user", user::user_router(shared_state.clone()))
        .nest(
            "/api/v1/videogen",
            videogen::videogen_router(shared_state.clone()),
        )
        .nest(
            "/api/v1/leaderboard",
            leaderboard::leaderboard_router(shared_state.clone()),
        )
        .nest(
            "/api/v1/rewards",
            rewards::api::rewards_router(shared_state.clone()),
        )
        .nest(
            "/api/v2/videogen",
            videogen::videogen_router_v2(shared_state.clone()),
        )
        .nest(
            "/api/v2/events",
            events::events_router_v2(shared_state.clone()),
        )
        .nest(
            "/api/v2/posts",
            posts::posts_router_v2(shared_state.clone()),
        )
        .nest(
            "/api/v1/videos",
            duplicate_video::router::video_router(shared_state.clone()),
        )
        .nest(
            "/api/v1/moderation",
            moderation::moderation_router(shared_state.clone()),
        );

    #[cfg(not(feature = "local-bin"))]
    let router = router.nest(
        "/api/v1/milvus",
        milvus::router::milvus_router(shared_state.clone()),
    );

    let (router, api) = router.split_for_parts();

    let router =
        router.merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", api.clone()));

    // build our application with a route
    let qstash_routes = qstash_router(shared_state.clone());
    let replicate_webhook_routes = videogen::router::replicate_webhook_router(shared_state.clone());

    let http = Router::new()
        .route("/healthz", get(health_handler))
        .route("/canister-health", get(canister_health_handler))
        .route("/report-approved", post(report_approved_handler))
        .route(
            "/enqueue_storj_backfill_item",
            post(enqueue_storj_backfill_item),
        )
        .nest("/qstash", qstash_routes)
        .nest("/replicate", replicate_webhook_routes)
        .fallback_service(router)
        .layer(DefaultBodyLimit::max(50 * 1024 * 1024)) // 50MB limit
        .layer(CorsLayer::permissive())
        .layer(axum::middleware::from_fn(
            crate::middleware::http_logging_middleware,
        )) // HTTP logging before Sentry
        .layer(sentry_tower_layer)
        .with_state(shared_state.clone());

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(warehouse_events::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(off_chain::FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    let grpc_axum = Routes::builder()
        .routes()
        .add_service(WarehouseEventsServer::with_interceptor(
            WarehouseEventsService {
                shared_state: shared_state.clone(),
            },
            check_auth_grpc,
        ))
        .add_service(OffChainServer::with_interceptor(
            OffChainService {
                shared_state: shared_state.clone(),
            },
            check_auth_grpc,
        ))
        .add_service(reflection_service)
        .into_axum_router()
        .layer(NewSentryLayer::new_from_top());

    let http_grpc = Steer::new(
        vec![http, grpc_axum],
        |req: &axum::extract::Request, _svcs: &[_]| {
            if req.headers().get(CONTENT_TYPE).map(|v| v.as_bytes()) != Some(b"application/grpc") {
                0
            } else {
                1
            }
        },
    );

    // run it
    let addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 50051));
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    log::info!("listening on {addr}");

    axum::serve(listener, Shared::new(http_grpc)).await.unwrap();

    Ok(())
}

fn main() {
    // Initialize ffmpeg
    ffmpeg_next::init().expect("Failed to initialize ffmpeg");

    // Initialize the rustls crypto provider
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let _guard = sentry::init((
        "https://e84a6dc4e0dfbaf7bd99b4a285a3e5b3@apm.yral.com/2",
        sentry::ClientOptions {
            release: sentry::release_name!(),
            // debug: true, // use when debugging sentry issues
            traces_sample_rate: std::env::var("SENTRY_TRACES_SAMPLE_RATE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.5),
            send_default_pii: true, // Keep false, manually add safe data
            attach_stacktrace: true,
            before_send: Some(crate::middleware::sentry_scrub::create_before_send()),
            ..Default::default()
        },
    ));

    // Configure sentry to only capture errors (not debug/info/warn)
    let sentry_layer = sentry_tracing::layer().event_filter(|metadata| match *metadata.level() {
        tracing::Level::ERROR => sentry_tracing::EventFilter::Event,
        tracing::Level::WARN => sentry_tracing::EventFilter::Breadcrumb,
        _ => sentry_tracing::EventFilter::Ignore,
    });

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                // Default to info level, with warn for noisy crates
                format!(
                    "{}=info,tower_http=warn,axum::rejection=warn,hyper=warn,reqwest=warn",
                    env!("CARGO_CRATE_NAME")
                )
                .into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .with(sentry_layer)
        .init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            main_impl().await.unwrap();
        });
}

#[instrument]
async fn health_handler() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK")
}
