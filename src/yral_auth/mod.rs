use candid::Principal;
use redis::AsyncCommands;

use crate::{config::AppConfig, types::RedisPool};

#[derive(Clone)]
pub struct YralAuthRedis {
    pool: RedisPool,
}

impl YralAuthRedis {
    #[cfg(not(feature = "local-bin"))]
    pub async fn init(app_config: &AppConfig) -> Self {
        let redis_url = app_config.yral_auth_redis_url.clone();

        let manager = bb8_redis::RedisConnectionManager::new(redis_url.clone())
            .expect("failed to open connection to redis");
        let pool = RedisPool::builder().build(manager).await.unwrap();

        Self { pool }
    }

    #[cfg(feature = "local-bin")]
    pub async fn init(_app_config: &AppConfig) -> Self {
        panic!("YralAuthRedis is not available in local-bin mode")
    }

    pub async fn delete_principal(&self, principal: Principal) -> Result<(), anyhow::Error> {
        let mut conn = self.pool.get().await.unwrap();
        let key = principal.to_string();
        
        conn.del::<String, ()>(key).await?;
        Ok(())
    }
}