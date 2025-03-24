use once_cell::sync::Lazy;
use reqwest::Url;

pub static BIGQUERY_INGESTION_URL: Lazy<Url> = Lazy::new(|| {
    Url::parse("https://bigquery.googleapis.com/bigquery/v2/projects/hot-or-not-feed-intelligence/datasets/analytics_335143420/tables/test_events_analytics/insertAll").unwrap()
});

pub const PLATFORM_ORCHESTRATOR_ID: &str = "74zq4-iqaaa-aaaam-ab53a-cai";

pub static YRAL_METADATA_URL: Lazy<Url> =
    Lazy::new(|| Url::parse("https://yral-metadata.fly.dev").unwrap());

pub const RECYCLE_THRESHOLD_SECS: u64 = 15 * 24 * 60 * 60; // 15 days

pub const GOOGLE_CHAT_REPORT_SPACE_URL: &str =
    "https://chat.googleapis.com/v1/spaces/AAAA1yDLYO4/messages";

pub const CLOUDFLARE_ACCOUNT_ID: &str = "a209c523d2d9646cc56227dbe6ce3ede";

pub const ICP_LEDGER_CANISTER_ID: &str = "ryjl3-tyaaa-aaaaa-aaaba-cai";

// TODO: change this back to https://icp-off-chain-agent.fly.dev/ before merging
pub static OFF_CHAIN_AGENT_URL: Lazy<Url> =
    Lazy::new(|| Url::parse("https://pr-102-yral-dapp-off-chain-agent.fly.dev/").unwrap());

pub const NSFW_SERVER_URL: &str = "https://prod-yral-nsfw-classification.fly.dev:443";

pub const CLOUDFLARE_ML_FEED_CACHE_WORKER_URL: &str =
    "https://yral-ml-feed-cache.go-bazzinga.workers.dev";

pub const ML_FEED_SERVER_GRPC_URL: &str = "https://yral-ml-feed-server.fly.dev:443";

pub static STORJ_INTERFACE_URL: Lazy<Url> = Lazy::new(|| {
    Url::parse("https://storjinterzvpuuk9pt3-1a53e66524f5042a.tec-s1.onthetaedgecloud.com").unwrap()
});

pub static STORJ_INTERFACE_TOKEN: Lazy<String> =
    Lazy::new(|| std::env::var("STORJ_INTERFACE_TOKEN").unwrap_or_else(|_| "testtoken".into()));
