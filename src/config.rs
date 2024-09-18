use std::{
    env,
    fs::OpenOptions,
    io::{BufWriter, Write},
};

use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use serde_with::serde_as;

#[serde_as]
#[derive(Deserialize, Clone)]
pub struct AppConfig {
    pub yral_metadata_token: String,
    pub google_sa_key: String,
}

impl AppConfig {
    pub fn load() -> Result<Self, ConfigError> {
        // set env var
        let sa_key_file = env::var("GOOGLE_SA_KEY").expect("GOOGLE_SA_KEY");
        env::set_var("SERVICE_ACCOUNT_JSON", sa_key_file.clone());

        // create a file
        let file = OpenOptions::new()
            .write(true)
            .create(true)
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
