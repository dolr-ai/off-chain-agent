use std::{
    env,
    fs::OpenOptions,
    io::{BufWriter, Write},
};

use config::{Config, ConfigError, Environment, File};
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde_with::serde_as;

use crate::consts::{STORJ_BACKUP_CANISTER_ACCESS_GRANT, STORJ_INTERFACE_TOKEN};

#[serde_as]
#[derive(Deserialize, Clone)]
pub struct AppConfig {
    pub yral_metadata_token: String,
    pub google_sa_key: String,
    #[cfg(not(feature = "local-bin"))]
    pub yral_auth_redis_url: String,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        Lazy::force(&STORJ_INTERFACE_TOKEN);
        Lazy::force(&STORJ_BACKUP_CANISTER_ACCESS_GRANT);
        // set env var
        let sa_key_file = env::var("GOOGLE_SA_KEY").expect("GOOGLE_SA_KEY");
        env::set_var("SERVICE_ACCOUNT_JSON", sa_key_file.clone());

        // create a file
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("google_service_account.json")
            .expect("create file");

        let mut f = BufWriter::new(file);
        f.write_all(sa_key_file.as_bytes()).expect("write file");

        env::set_var(
            "GOOGLE_APPLICATION_CREDENTIALS",
            "google_service_account.json",
        );

        let conf = Config::builder()
            .add_source(File::with_name("config.toml").required(false))
            .add_source(File::with_name(".env").required(false))
            .add_source(Environment::default())
            .build()?;

        conf.try_deserialize()
    }
}
