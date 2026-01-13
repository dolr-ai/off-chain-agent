use candid::Principal;
use once_cell::sync::Lazy;
use reqwest::Url;

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

pub static YRAL_UPLOAD_VIDEO_WORKER_URL: Lazy<Url> =
    Lazy::new(|| Url::parse("https://yral-upload-video.go-bazzinga.workers.dev").unwrap());

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
        .unwrap_or_else(|_| "https://offchain.yral.com/".into());
    Url::parse(&url).unwrap()
});

pub const NSFW_SERVER_URL: &str = "https://prod-yral-nsfw-classification.fly.dev:443";

pub const ML_FEED_SERVER_GRPC_URL: &str = "https://yral-ml-feed-server.fly.dev:443";

// Analytics Server
pub const ANALYTICS_SERVER_URL: &str = "https://analytics.yral.com";

pub static STORJ_INTERFACE_URL: Lazy<Url> =
    Lazy::new(|| Url::parse("https://storj-interface.yral.com/").unwrap());

pub static STORJ_INTERFACE_TOKEN: Lazy<String> =
    Lazy::new(|| std::env::var("STORJ_INTERFACE_TOKEN").expect("STORJ_INTERFACE_TOKEN to be set"));

pub static STORJ_BACKUP_CANISTER_ACCESS_GRANT: Lazy<String> = Lazy::new(|| {
    std::env::var("STORJ_BACKUP_CANISTER_ACCESS_GRANT")
        .expect("STORJ_BACKUP_CANISTER_ACCESS_GRANT to be set")
});

pub const CANISTER_BACKUPS_BUCKET: &str = "canister-backups";

// Storj Public Bucket URLs
pub const STORJ_SFW_BUCKET_URL: &str =
    "https://link.storjshare.io/raw/jx6vm3ebgb4gt3gfkmcrw62bl7rq/yral-videos";
pub const STORJ_NSFW_BUCKET_URL: &str =
    "https://link.storjshare.io/raw/jwait7tp3civp6cbaot4zzjbheqq/yral-nsfw-videos";

pub fn get_storj_video_url(publisher_user_id: &str, video_id: &str, is_nsfw: bool) -> String {
    let bucket_url = if is_nsfw {
        STORJ_NSFW_BUCKET_URL
    } else {
        STORJ_SFW_BUCKET_URL
    };
    format!("{}/{}/{}.mp4", bucket_url, publisher_user_id, video_id)
}

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

// LumaLabs Constants
pub const LUMALABS_API_URL: &str = "https://api.lumalabs.ai/dream-machine/v1";
pub const LUMALABS_IMAGE_BUCKET: &str = "videogen_tmp_image_store";
pub const SPEECH_TO_VIDEO_AUDIO_BUCKET: &str = "videogen_tmp_image_store";

// Replicate Constants
pub const REPLICATE_API_URL: &str = "https://api.replicate.com/v1";
pub const REPLICATE_WAN2_5_MODEL: &str = "wan-video/wan-2.5-t2v";
pub const REPLICATE_WAN2_5_FAST_MODEL: &str = "wan-video/wan-2.5-t2v-fast";

pub static MODERATOR_PRINCIPALS: Lazy<Vec<Principal>> = Lazy::new(|| {
    vec![
        "o7soq-c4ync-cfs3n-i5qbs-472zl-nbxlh-df7r4-2uqpz-svjpz-7ktda-dae"
            .parse()
            .unwrap(),
        "ai5pa-6geuw-2fe7h-oi5vs-fd3rd-unvxq-44jhj-ze2dx-gxl6k-us473-qqe"
            .parse()
            .unwrap(),
        "5tl42-cgswe-y7ewx-pcbbc-oi4mq-7doez-5kry4-d2pbn-qm6zr-bq7iw-vae"
            .parse()
            .unwrap(),
        "dui6m-6zkzw-jl2s6-vjief-5xwxa-aps5o-tnoa6-kwg72-wehle-jwwdm-hae"
            .parse()
            .unwrap(),
        "we7z3-kyy6a-i256f-mhnr5-evuc5-vfjyt-d2sos-nfvg4-app2q-j6z7t-uqe"
            .parse()
            .unwrap(),
        "bj5wc-6kmlu-om5sc-clarc-dfrya-pvccu-iw3xc-twpzw-tswqt-q3fou-wqe"
            .parse()
            .unwrap(),
        "uau2g-57gtt-6vcnr-zalkn-t76ad-tejac-cdt62-qdv25-wfl5l-fimkk-yae"
            .parse()
            .unwrap(),
        "7vovb-nk3ke-4cptr-p57qb-wtcrl-rlc2f-4kweo-tksld-pfq2p-ptkiw-pqe"
            .parse()
            .unwrap(),
    ]
});
