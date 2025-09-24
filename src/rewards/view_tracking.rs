use crate::types::RedisPool;
use anyhow::{Context, Result};
use candid::Principal;
use redis::AsyncCommands;
use sha1::{Digest, Sha1};

const LUA_ATOMIC_VIEW_SCRIPT: &str = r#"
    -- Atomic operation for view counting with config change handling
    local views_set = KEYS[1]  -- rewards:views:{video_id}
    local count_key = KEYS[2]  -- rewards:view_count:{video_id}
    local config_version_key = KEYS[3]  -- rewards:config_version:{video_id}

    local user_id = ARGV[1]
    local current_config_version = ARGV[2]

    -- Check if config changed
    local stored_version = redis.call('GET', config_version_key)
    if stored_version ~= current_config_version then
        -- Config changed, reset counter but preserve set
        redis.call('SET', count_key, 0)
        redis.call('SET', config_version_key, current_config_version)
    end

    -- Check if user already viewed (critical check)
    local added = redis.call('SADD', views_set, user_id)
    if added == 1 then
        -- New view, increment counter
        return redis.call('INCR', count_key)
    else
        return nil  -- Duplicate view
    end
"#;

fn calculate_script_sha(script: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(script.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

pub struct ViewTracker {
    pool: RedisPool,
    script_sha: Option<String>,
}

impl ViewTracker {
    pub fn new(pool: RedisPool) -> Self {
        Self {
            pool,
            script_sha: None,
        }
    }

    pub async fn load_lua_scripts(&mut self) -> Result<String> {
        let mut conn = self.pool.get().await?;
        let sha: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(LUA_ATOMIC_VIEW_SCRIPT)
            .query_async(&mut *conn)
            .await
            .context("Failed to load Lua script")?;

        // Verify the SHA matches what we expect
        let expected_sha = calculate_script_sha(LUA_ATOMIC_VIEW_SCRIPT);
        if sha != expected_sha {
            log::warn!(
                "Loaded script SHA {} doesn't match calculated SHA {}",
                sha,
                expected_sha
            );
        }

        self.script_sha = Some(sha.clone());
        log::info!("Loaded view tracking Lua script with SHA: {}", sha);
        Ok(sha)
    }

    pub async fn track_view(
        &self,
        video_id: &str,
        user_id: &Principal,
        config_version: u64,
    ) -> Result<Option<u64>> {
        let mut conn = self.pool.get().await?;

        // Keys for the Lua script
        let views_set_key = format!("rewards:views:{}", video_id);
        let count_key = format!("rewards:view_count:{}", video_id);
        let config_version_key = format!("rewards:config_version:{}", video_id);

        // Try to use the loaded script SHA, fallback to EVAL if not loaded
        let result: Option<u64> = if let Some(sha) = &self.script_sha {
            // Use EVALSHA for better performance
            let evalsha_result = redis::cmd("EVALSHA")
                .arg(sha)
                .arg(3) // number of keys
                .arg(&views_set_key)
                .arg(&count_key)
                .arg(&config_version_key)
                .arg(user_id.to_string())
                .arg(config_version.to_string())
                .query_async(&mut *conn)
                .await;

            match evalsha_result {
                Ok(result) => result,
                Err(e) => {
                    // If script not in cache, use EVAL
                    log::warn!("EVALSHA failed, falling back to EVAL: {}", e);
                    redis::cmd("EVAL")
                        .arg(LUA_ATOMIC_VIEW_SCRIPT)
                        .arg(3)
                        .arg(&views_set_key)
                        .arg(&count_key)
                        .arg(&config_version_key)
                        .arg(user_id.to_string())
                        .arg(config_version.to_string())
                        .query_async(&mut *conn)
                        .await
                        .context("Failed to execute view tracking script")?
                }
            }
        } else {
            // Script not loaded, use EVAL
            redis::cmd("EVAL")
                .arg(LUA_ATOMIC_VIEW_SCRIPT)
                .arg(3)
                .arg(&views_set_key)
                .arg(&count_key)
                .arg(&config_version_key)
                .arg(user_id.to_string())
                .arg(config_version.to_string())
                .query_async(&mut *conn)
                .await
                .context("Failed to execute view tracking script")?
        };

        Ok(result)
    }

    pub async fn get_view_count(&self, video_id: &str) -> Result<u64> {
        let mut conn = self.pool.get().await?;
        let count_key = format!("rewards:view_count:{}", video_id);
        let count: Option<u64> = conn.get(&count_key).await?;
        Ok(count.unwrap_or(0))
    }

    pub async fn get_last_milestone(&self, video_id: &str) -> Result<u64> {
        let mut conn = self.pool.get().await?;
        let milestone_key = format!("rewards:last_milestone:{}", video_id);
        let milestone: Option<u64> = conn.get(&milestone_key).await?;
        Ok(milestone.unwrap_or(0))
    }

    pub async fn set_last_milestone(&self, video_id: &str, milestone: u64) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let milestone_key = format!("rewards:last_milestone:{}", video_id);
        conn.set(&milestone_key, milestone).await?;
        Ok(())
    }
}