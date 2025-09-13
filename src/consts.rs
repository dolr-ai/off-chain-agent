use candid::Principal;
use once_cell::sync::Lazy;
use reqwest::Url;

pub const STDB_URL: &str = "https://maincloud.spacetimedb.com";

pub const DELETED_CANISTERS_MODULE_IDENTITY: &str = "yral-canisters";

pub static STDB_ACCESS_TOKEN: Lazy<String> = Lazy::new(|| {
    std::env::var("STDB_ADMIN_ACCESS_TOKEN").expect("STDB_ADMIN_ACCESS_TOKEN to be set")
});

pub static DEDUP_INDEX_CANISTER_ID: Lazy<Principal> = Lazy::new(|| {
    "4drz6-pyaaa-aaaas-qbfoa-cai"
        .parse()
        .expect("canister id to be valid")
});

/// with nsfw detection v2, nsfw probablity greater or equal to this is considered nsfw
pub const NSFW_THRESHOLD: f32 = 0.4;

pub static BIGQUERY_INGESTION_URL: Lazy<Url> = Lazy::new(|| {
    Url::parse("https://bigquery.googleapis.com/bigquery/v2/projects/hot-or-not-feed-intelligence/datasets/analytics_335143420/tables/test_events_analytics/insertAll").unwrap()
});

#[allow(dead_code)]
pub const PLATFORM_ORCHESTRATOR_ID: &str = "74zq4-iqaaa-aaaam-ab53a-cai";

pub static YRAL_METADATA_URL: Lazy<Url> =
    Lazy::new(|| Url::parse("https://yral-metadata.fly.dev/").unwrap());

#[allow(dead_code)]
pub const RECYCLE_THRESHOLD_SECS: u64 = 15 * 24 * 60 * 60; // 15 days

pub const GOOGLE_CHAT_REPORT_SPACE_URL: &str =
    "https://chat.googleapis.com/v1/spaces/AAAA1yDLYO4/messages";

#[allow(dead_code)]
pub const CLOUDFLARE_ACCOUNT_ID: &str = "a209c523d2d9646cc56227dbe6ce3ede";

#[allow(dead_code)]
pub const ICP_LEDGER_CANISTER_ID: &str = "ryjl3-tyaaa-aaaaa-aaaba-cai";

pub static OFF_CHAIN_AGENT_URL: Lazy<Url> = Lazy::new(|| {
    let url = std::env::var("OFF_CHAIN_AGENT_URL")
        .unwrap_or_else(|_| "https://icp-off-chain-agent.fly.dev/".into());
    Url::parse(&url).unwrap()
});

pub const NSFW_SERVER_URL: &str = "https://prod-yral-nsfw-classification.fly.dev:443";

pub const ML_FEED_SERVER_GRPC_URL: &str = "https://yral-ml-feed-server.fly.dev:443";

pub static STORJ_INTERFACE_URL: Lazy<Url> = Lazy::new(|| {
    Url::parse("https://storjinterutdu7phc9s-368643dc6f41f83b.tec-s1.onthetaedgecloud.com/")
        .unwrap()
});

pub static STORJ_INTERFACE_TOKEN: Lazy<String> =
    Lazy::new(|| std::env::var("STORJ_INTERFACE_TOKEN").expect("STORJ_INTERFACE_TOKEN to be set"));

pub static STORJ_BACKUP_CANISTER_ACCESS_GRANT: Lazy<String> = Lazy::new(|| {
    std::env::var("STORJ_BACKUP_CANISTER_ACCESS_GRANT")
        .expect("STORJ_BACKUP_CANISTER_ACCESS_GRANT to be set")
});

pub const CANISTER_BACKUPS_BUCKET: &str = "canister-backups";

// Rate Limiting Constants
pub static RATE_LIMITS_CANISTER_ID: Lazy<Principal> = Lazy::new(|| {
    "h2jgv-ayaaa-aaaas-qbh4a-cai"
        .parse()
        .expect("Rate limits canister ID to be valid")
});

#[allow(dead_code)]
pub const VIDEOGEN_RATE_LIMIT_PROPERTY: &str = "VIDEOGEN";

// User Info Service Constants
pub static USER_INFO_SERVICE_CANISTER_ID: Lazy<Principal> = Lazy::new(|| {
    "ivkka-7qaaa-aaaas-qbg3q-cai"
        .parse()
        .expect("User info service canister ID to be valid")
});

// User Post Service Constants
pub static USER_POST_SERVICE_CANISTER_ID: Lazy<Principal> = Lazy::new(|| {
    "gxhc3-pqaaa-aaaas-qbh3q-cai"
        .parse()
        .expect("User post service canister ID to be valid")
});

// Video Generation Constants
pub const VEO3_PROJECT_ID: &str = "hot-or-not-feed-intelligence";
pub const VEO3_STORAGE_URI: &str = "gs://yral_ai_generated_videos/veo-output/";
pub const VEO3_LOCATION: &str = "us-central1";

// LumaLabs Constants
pub const LUMALABS_API_URL: &str = "https://api.lumalabs.ai/dream-machine/v1";
pub const LUMALABS_IMAGE_BUCKET: &str = "videogen_tmp_image_store";
