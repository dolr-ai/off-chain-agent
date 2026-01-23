use crate::config::AppConfig;
use crate::consts::{ANALYTICS_SERVER_URL, NSFW_SERVER_URL, YRAL_METADATA_URL};
#[cfg(not(feature = "local-bin"))]
use crate::events::push_notifications::NotificationClient;
use crate::kvrocks::KvrocksClient;
use crate::metrics::{init_metrics, CfMetricTx};
use crate::qstash::client::QStashClient;
use crate::qstash::QStashState;
use crate::rewards::RewardsModule;
use crate::scratchpad::ScratchpadClient;
use crate::types::RedisPool;
use crate::yral_auth::dragonfly::{
    get_ca_cert_pem, get_client_cert_pem, get_client_key_pem, init_dragonfly_redis,
    init_dragonfly_redis_2, DragonflyPool,
};
use crate::yral_auth::YralAuthRedis;
use anyhow::{anyhow, Context, Result};
use candid::Principal;
use google_cloud_alloydb_v1::client::AlloyDBAdmin;
use google_cloud_auth::credentials::service_account::Builder as CredBuilder;
use google_cloud_bigquery::client::{Client, ClientConfig};
use hyper_util::client::legacy::connect::HttpConnector;
use ic_agent::identity::Secp256k1Identity;
use ic_agent::Agent;
#[cfg(not(feature = "local-bin"))]
use milvus::client::Client as MilvusClient;
use reqwest::Client as ReqwestClient;
use std::env;
use std::sync::Arc;
use tonic::transport::{Channel, ClientTlsConfig};
use yral_alloydb_client::AlloyDbInstance;
use yral_canisters_client::individual_user_template::IndividualUserTemplate;
use yral_metadata_client::MetadataClient;
use yup_oauth2::hyper_rustls::HttpsConnector;
use yup_oauth2::{authenticator::Authenticator, ServiceAccountAuthenticator};

#[derive(Clone)]
pub struct MixpanelClient {
    pub client: ReqwestClient,
    pub token: String,
    pub url: String,
}

impl Default for MixpanelClient {
    fn default() -> Self {
        Self::new()
    }
}

impl MixpanelClient {
    pub fn new() -> Self {
        let token = env::var("ANALYTICS_SERVER_TOKEN").expect("ANALYTICS_SERVER_TOKEN is required");
        Self {
            client: ReqwestClient::new(),
            token,
            url: format!("{}/api/send_event", ANALYTICS_SERVER_URL),
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub admin_identity: Secp256k1Identity,
    pub agent: ic_agent::Agent,
    pub yral_metadata_client: MetadataClient<true>,
    #[cfg(not(feature = "local-bin"))]
    pub auth: Authenticator<HttpsConnector<HttpConnector>>,
    /// Google Chat App authenticator (for sending messages with interactive buttons)
    #[cfg(not(feature = "local-bin"))]
    pub gchat_auth: Authenticator<HttpsConnector<HttpConnector>>,
    pub qstash: QStashState,
    #[cfg(not(feature = "local-bin"))]
    pub bigquery_client: Client,
    pub nsfw_detect_channel: Channel,
    pub qstash_client: QStashClient,
    #[cfg(not(feature = "local-bin"))]
    pub gcs_client: Arc<cloud_storage::Client>,
    pub metrics: CfMetricTx,
    #[cfg(not(any(feature = "local-bin", feature = "use-local-agent")))]
    pub alloydb_client: AlloyDbInstance,
    #[cfg(not(feature = "local-bin"))]
    #[cfg(not(feature = "local-bin"))]
    pub canister_backup_redis_pool: RedisPool,
    #[cfg(not(feature = "local-bin"))]
    pub notification_client: NotificationClient,
    #[cfg(not(feature = "local-bin"))]
    pub yral_auth_redis: YralAuthRedis,
    #[cfg(not(feature = "local-bin"))]
    pub yral_auth_dragonfly: Arc<DragonflyPool>,
    pub leaderboard_redis_pool: RedisPool,
    #[cfg(not(feature = "local-bin"))]
    pub rewards_module: RewardsModule,
    pub service_cansister_migration_redis_pool: RedisPool,
    pub config: AppConfig,
    pub replicate_api_token: String,
    pub user_migration_api_key: String,
    #[cfg(not(feature = "local-bin"))]
    pub milvus_client: Option<MilvusClient>,
    #[cfg(not(feature = "local-bin"))]
    pub kvrocks_client: KvrocksClient,

    // This uses ds staging
    #[cfg(not(feature = "local-bin"))]
    pub scratchpad_client: ScratchpadClient,

    pub mixpanel_client: MixpanelClient,
}

impl AppState {
    pub async fn new(app_config: AppConfig) -> Self {
        let leaderboard_redis_pool = init_leaderboard_redis_pool().await;
        let agent = init_agent().await;

        // Initialize Dragonfly cluster 2 for rewards/impressions
        #[cfg(not(feature = "local-bin"))]
        let rewards_dragonfly_pool = init_dragonfly_redis_2()
            .await
            .expect("Failed to initialize Dragonfly cluster 2 for rewards");

        #[cfg(not(feature = "local-bin"))]
        let mut rewards_module = RewardsModule::new(rewards_dragonfly_pool, agent.clone()).await;

        // Initialize the rewards module (loads Lua scripts)
        #[cfg(not(feature = "local-bin"))]
        if let Err(e) = rewards_module.initialize().await {
            log::error!("Failed to initialize rewards module: {}", e);
        }

        #[cfg(not(feature = "local-bin"))]
        let milvus_client = init_milvus_client(&app_config).await;

        #[cfg(not(feature = "local-bin"))]
        let kvrocks_client = init_kvrocks_client().await;
        #[cfg(not(feature = "local-bin"))]
        let scratchpad_client = init_scratchpad_client().await;

        AppState {
            admin_identity: init_identity(),
            yral_metadata_client: init_yral_metadata_client(&app_config),
            agent,
            #[cfg(not(feature = "local-bin"))]
            auth: init_auth().await,
            #[cfg(not(feature = "local-bin"))]
            gchat_auth: init_gchat_auth().await,
            // ml_server_grpc_channel: init_ml_server_grpc_channel().await,
            qstash: init_qstash(),
            #[cfg(not(feature = "local-bin"))]
            bigquery_client: init_bigquery_client().await,
            nsfw_detect_channel: init_nsfw_detect_channel().await,
            qstash_client: init_qstash_client().await,
            #[cfg(not(feature = "local-bin"))]
            gcs_client: Arc::new(cloud_storage::Client::default()),
            metrics: init_metrics(),
            #[cfg(not(any(feature = "local-bin", feature = "use-local-agent")))]
            alloydb_client: init_alloydb_client().await,
            #[cfg(not(feature = "local-bin"))]
            #[cfg(not(feature = "local-bin"))]
            canister_backup_redis_pool: init_canister_backup_redis_pool().await,
            #[cfg(not(feature = "local-bin"))]
            notification_client: NotificationClient::new(
                env::var("YRAL_METADATA_NOTIFICATION_API_KEY").unwrap_or_default(),
            ),
            #[cfg(not(feature = "local-bin"))]
            yral_auth_redis: YralAuthRedis::init(&app_config).await,
            #[cfg(not(feature = "local-bin"))]
            yral_auth_dragonfly: init_dragonfly_redis_pool().await,
            leaderboard_redis_pool,
            #[cfg(not(feature = "local-bin"))]
            rewards_module,
            config: app_config,
            service_cansister_migration_redis_pool: init_service_canister_migration_redis_pool()
                .await,
            replicate_api_token: env::var("REPLICATE_API_TOKEN").unwrap_or_default(),
            user_migration_api_key: env::var("YRAL_OFF_CHAIN_USER_MIGRATION_API_KEY")
                .expect("YRAL_OFF_CHAIN_USER_MIGRATION_API_KEY is not set"),
            #[cfg(not(feature = "local-bin"))]
            milvus_client,
            #[cfg(not(feature = "local-bin"))]
            kvrocks_client,
            #[cfg(not(feature = "local-bin"))]
            scratchpad_client,
            mixpanel_client: MixpanelClient::new(),
        }
    }

    pub async fn get_access_token(&self, scopes: &[&str]) -> String {
        #[cfg(feature = "local-bin")]
        {
            "localtoken".into()
        }

        #[cfg(not(feature = "local-bin"))]
        {
            let auth = &self.auth;
            let token = auth.token(scopes).await.unwrap();

            match token.token() {
                Some(t) => t.to_string(),
                _ => panic!("No access token found"),
            }
        }
    }

    /// Get access token for Google Chat API using the yral-mobile service account
    pub async fn get_gchat_access_token(&self) -> String {
        #[cfg(feature = "local-bin")]
        {
            "localtoken".into()
        }

        #[cfg(not(feature = "local-bin"))]
        {
            let auth = &self.gchat_auth;
            let token = auth
                .token(&["https://www.googleapis.com/auth/chat.bot"])
                .await
                .expect("Failed to get Google Chat access token");

            match token.token() {
                Some(t) => t.to_string(),
                _ => panic!("No Google Chat access token found"),
            }
        }
    }

    pub async fn get_individual_canister_by_user_principal(
        &self,
        user_principal: Principal,
    ) -> Result<Principal> {
        let meta = self
            .yral_metadata_client
            .get_user_metadata_v2(user_principal.to_string())
            .await
            .context("Failed to get user_metadata from yral_metadata_client")?;

        match meta {
            Some(meta) => Ok(meta.user_canister_id),
            None => Err(anyhow!(
                "user metadata does not exist in yral_metadata_service"
            )),
        }
    }

    pub fn individual_user(&self, user_canister: Principal) -> IndividualUserTemplate<'_> {
        IndividualUserTemplate(user_canister, &self.agent)
    }
}

pub fn init_yral_metadata_client(conf: &AppConfig) -> MetadataClient<true> {
    MetadataClient::with_base_url(YRAL_METADATA_URL.clone())
        .with_jwt_token(conf.yral_metadata_token.clone())
}

pub fn init_identity() -> ic_agent::identity::Secp256k1Identity {
    #[cfg(not(any(feature = "local-bin", feature = "use-local-agent")))]
    {
        let pk = env::var("BACKEND_ADMIN_IDENTITY").expect("$BACKEND_ADMIN_IDENTITY is not set");
        match ic_agent::identity::Secp256k1Identity::from_pem(stringreader::StringReader::new(
            pk.as_str(),
        )) {
            Ok(identity) => identity,
            Err(err) => {
                panic!("Unable to create identity, error: {err:?}");
            }
        }
    }

    #[cfg(any(feature = "use-local-agent", feature = "local-bin"))]
    {
        use k256::elliptic_curve::{rand_core, SecretKey};
        use rand::rng;

        let mut rng = rand_core::OsRng {};

        ic_agent::identity::Secp256k1Identity::from_private_key(SecretKey::random(&mut rng))
    }
}

pub async fn init_agent() -> Agent {
    #[cfg(not(any(feature = "local-bin", feature = "use-local-agent")))]
    {
        let pk = env::var("BACKEND_ADMIN_IDENTITY").expect("$BACKEND_ADMIN_IDENTITY is not set");

        let identity = match ic_agent::identity::Secp256k1Identity::from_pem(
            stringreader::StringReader::new(pk.as_str()),
        ) {
            Ok(identity) => identity,
            Err(err) => {
                panic!("Unable to create identity, error: {err:?}");
            }
        };

        match Agent::builder()
            .with_url("https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app/") // https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app/
            .with_identity(identity)
            .build()
        {
            Ok(agent) => agent,
            Err(err) => {
                panic!("Unable to create agent, error: {err:?}");
            }
        }
    }

    #[cfg(feature = "local-bin")]
    {
        let agent = Agent::builder()
            .with_url("https://ic0.app")
            .build()
            .unwrap();

        // agent.fetch_root_key().await.unwrap();

        agent
    }

    #[cfg(feature = "use-local-agent")]
    {
        let pk = env::var("BACKEND_ADMIN_IDENTITY").expect("$BACKEND_ADMIN_IDENTITY is not set");

        let identity = match ic_agent::identity::Secp256k1Identity::from_pem(
            stringreader::StringReader::new(pk.as_str()),
        ) {
            Ok(identity) => identity,
            Err(err) => {
                panic!("Unable to create identity, error: {:?}", err);
            }
        };

        match Agent::builder()
            .with_url("https://ic0.app") // https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app/
            .with_identity(identity)
            .build()
        {
            Ok(agent) => agent,
            Err(err) => {
                panic!("Unable to create agent, error: {:?}", err);
            }
        }
    }
}

pub async fn init_auth() -> Authenticator<HttpsConnector<HttpConnector>> {
    let sa_key_file = env::var("GOOGLE_SA_KEY").expect("GOOGLE_SA_KEY is required");

    // Load your service account key
    let sa_key = yup_oauth2::parse_service_account_key(sa_key_file).expect("GOOGLE_SA_KEY.json");

    ServiceAccountAuthenticator::builder(sa_key)
        .build()
        .await
        .unwrap()
}

/// Initialize Google Chat App authenticator using YRAL_MOBILE_SERVICE_ACCOUNT_KEY
/// This is needed to send messages as the Chat App (so interactive buttons work)
pub async fn init_gchat_auth() -> Authenticator<HttpsConnector<HttpConnector>> {
    let sa_key_file = env::var("YRAL_MOBILE_SERVICE_ACCOUNT_KEY")
        .expect("YRAL_MOBILE_SERVICE_ACCOUNT_KEY is required");

    let sa_key = yup_oauth2::parse_service_account_key(sa_key_file)
        .expect("Invalid YRAL_MOBILE_SERVICE_ACCOUNT_KEY");

    ServiceAccountAuthenticator::builder(sa_key)
        .build()
        .await
        .expect("Failed to build Google Chat authenticator")
}

pub fn init_qstash() -> QStashState {
    let qstash_key =
        env::var("QSTASH_CURRENT_SIGNING_KEY").expect("QSTASH_CURRENT_SIGNING_KEY is required");

    QStashState::init(qstash_key)
}

pub async fn init_bigquery_client() -> Client {
    let (config, _) = ClientConfig::new_with_auth().await.unwrap();
    Client::new(config).await.unwrap()
}

pub async fn init_nsfw_detect_channel() -> Channel {
    let tls_config = ClientTlsConfig::new().with_webpki_roots();
    Channel::from_static(NSFW_SERVER_URL)
        .tls_config(tls_config)
        .expect("Couldn't update TLS config for nsfw agent")
        .connect()
        .await
        .expect("Couldn't connect to nsfw agent")
}

pub async fn init_qstash_client() -> QStashClient {
    let auth_token = env::var("QSTASH_AUTH_TOKEN").expect("QSTASH_AUTH_TOKEN is required");
    QStashClient::new(auth_token.as_str())
}

async fn init_alloydb_client() -> AlloyDbInstance {
    let sa_json_raw = env::var("ALLOYDB_SERVICE_ACCOUNT_JSON")
        .expect("`ALLOYDB_SERVICE_ACCOUNT_JSON` is required!");
    let sa_json: serde_json::Value =
        serde_json::from_str(&sa_json_raw).expect("Invalid `ALLOYDB_SERVICE_ACCOUNT_JSON`");
    let credentials = CredBuilder::new(sa_json)
        .build()
        .expect("Invalid `ALLOYDB_SERVICE_ACCOUNT_JSON`");

    let client = AlloyDBAdmin::builder()
        .with_credentials(credentials)
        .build()
        .await
        .expect("Failed to create AlloyDB client");

    let instance = env::var("ALLOYDB_INSTANCE").expect("`ALLOYDB_INSTANCE` is required!");
    let db_name = env::var("ALLOYDB_DB_NAME").expect("`ALLOYDB_DB_NAME` is required!");
    let db_user = env::var("ALLOYDB_DB_USER").expect("`ALLOYDB_DB_USER` is required!");
    let db_password = env::var("ALLOYDB_DB_PASSWORD").expect("`ALLOYDB_DB_PASSWORD` is required!");

    AlloyDbInstance::new(client, instance, db_name, db_user, db_password)
}

async fn init_canister_backup_redis_pool() -> RedisPool {
    let redis_url = std::env::var("CANISTER_BACKUP_CACHE_REDIS_URL")
        .expect("CANISTER_BACKUP_CACHE_REDIS_URL must be set");

    let manager = bb8_redis::RedisConnectionManager::new(redis_url.clone())
        .expect("failed to open connection to redis");
    RedisPool::builder().build(manager).await.unwrap()
}

async fn init_leaderboard_redis_pool() -> RedisPool {
    let redis_url =
        std::env::var("LEADERBOARD_REDIS_URL").expect("Either LEADERBOARD_REDIS_URL must be set");

    let manager = bb8_redis::RedisConnectionManager::new(redis_url.clone())
        .expect("failed to open connection to redis");
    RedisPool::builder().build(manager).await.unwrap()
}

async fn init_service_canister_migration_redis_pool() -> RedisPool {
    let redis_url = std::env::var("SERVICE_CANISTER_MIGRATION_REDIS_URL")
        .expect("SERVICE_CANISTER_MIGRATION_REDIS_URL is not set");

    let manager = bb8_redis::RedisConnectionManager::new(redis_url.clone())
        .expect("failed to open connection to redis");
    RedisPool::builder().build(manager).await.unwrap()
}

async fn init_dragonfly_redis_pool() -> Arc<DragonflyPool> {
    let ca_cert_bytes = get_ca_cert_pem().expect("Failed to read DRAGONFLY_CA_CERT");
    let client_cert_bytes = get_client_cert_pem().expect("Failed to read DRAGONFLY_CLIENT_CERT");
    let client_key_bytes = get_client_key_pem().expect("Failed to read DRAGONFLY_CLIENT_KEY");
    let dragonfly_pool: Arc<DragonflyPool> =
        init_dragonfly_redis(ca_cert_bytes, client_cert_bytes, client_key_bytes)
            .await
            .expect("failed to initalize DragonflyPool");

    dragonfly_pool
}

#[cfg(not(feature = "local-bin"))]
async fn init_milvus_client(app_config: &AppConfig) -> Option<MilvusClient> {
    use crate::milvus;

    let milvus_url = match &app_config.milvus_url {
        Some(url) => url,
        None => {
            log::warn!("MILVUS_URL not set, Milvus functionality will be disabled");
            return None;
        }
    };

    log::info!("Initializing Milvus client at {}", milvus_url);

    match milvus::create_milvus_client(milvus_url.clone()).await {
        Ok(client) => {
            log::info!("Milvus client connected successfully");

            // Initialize collection if it doesn't exist
            if let Err(e) = milvus::init_collection(&client).await {
                log::error!("Failed to initialize Milvus collection: {}", e);
                return None;
            }

            Some(client)
        }
        Err(e) => {
            log::error!("Failed to connect to Milvus: {}", e);
            None
        }
    }
}

async fn init_kvrocks_client() -> KvrocksClient {
    crate::kvrocks::init_kvrocks_client()
        .await
        .expect("Failed to connect to kvrocks - this is required for the application to function")
}

async fn init_scratchpad_client() -> ScratchpadClient {
    crate::scratchpad::init_scratchpad_client()
        .await
        .expect("Failed to connect to scratchpad Dragonfly - this is required for the application to function")
}
