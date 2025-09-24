use crate::types::RedisPool;
use anyhow::{Context, Result};
use candid::Principal;
use redis::AsyncCommands;
use sha1::{Digest, Sha1};

const LUA_ATOMIC_VIEW_SCRIPT: &str = r#"
    -- Atomic operation for view counting with config change handling
    -- Using HSET to store both count and config_version in single hash
    local views_set = KEYS[1]  -- rewards:views:{video_id} (set of user IDs)
    local video_hash = KEYS[2]  -- rewards:video:{video_id} (hash with count & config_version)

    local user_id = ARGV[1]

    -- Get current global config version from Redis
    local current_global_version = redis.call('GET', 'rewards:config:version') or '1'

    -- Get video's stored config version from hash
    local video_config_version = redis.call('HGET', video_hash, 'config_version') or '0'

    -- If config changed for THIS video, reset its counter
    if video_config_version ~= current_global_version then
        -- Reset counter to 0 and update config version
        redis.call('HSET', video_hash, 'count', 0, 'config_version', current_global_version)
        -- Note: We preserve the views set for deduplication
    end

    -- Check if user already viewed (critical check)
    local added = redis.call('SADD', views_set, user_id)
    if added == 1 then
        -- New view, increment counter in hash and return new value
        return redis.call('HINCRBY', video_hash, 'count', 1)
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

#[derive(Clone)]
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
    ) -> Result<Option<u64>> {
        let mut conn = self.pool.get().await?;

        // Keys for the Lua script
        let views_set_key = format!("rewards:views:{}", video_id);
        let video_hash_key = format!("rewards:video:{}", video_id);

        // Try to use the loaded script SHA, fallback to EVAL if not loaded
        let result: Option<u64> = if let Some(sha) = &self.script_sha {
            // Use EVALSHA for better performance
            let evalsha_result = redis::cmd("EVALSHA")
                .arg(sha)
                .arg(2) // number of keys (reduced from 3 to 2)
                .arg(&views_set_key)
                .arg(&video_hash_key)
                .arg(user_id.to_string())
                .query_async(&mut *conn)
                .await;

            match evalsha_result {
                Ok(result) => result,
                Err(e) => {
                    // If script not in cache, use EVAL
                    log::warn!("EVALSHA failed, falling back to EVAL: {}", e);
                    redis::cmd("EVAL")
                        .arg(LUA_ATOMIC_VIEW_SCRIPT)
                        .arg(2)
                        .arg(&views_set_key)
                        .arg(&video_hash_key)
                        .arg(user_id.to_string())
                        .query_async(&mut *conn)
                        .await
                        .context("Failed to execute view tracking script")?
                }
            }
        } else {
            // Script not loaded, use EVAL
            redis::cmd("EVAL")
                .arg(LUA_ATOMIC_VIEW_SCRIPT)
                .arg(2)
                .arg(&views_set_key)
                .arg(&video_hash_key)
                .arg(user_id.to_string())
                .query_async(&mut *conn)
                .await
                .context("Failed to execute view tracking script")?
        };

        Ok(result)
    }

    pub async fn get_view_count(&self, video_id: &str) -> Result<u64> {
        let mut conn = self.pool.get().await?;
        let video_hash_key = format!("rewards:video:{}", video_id);
        let count: Option<String> = conn.hget(&video_hash_key, "count").await?;
        Ok(count.and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub async fn get_last_milestone(&self, video_id: &str) -> Result<u64> {
        let mut conn = self.pool.get().await?;
        let video_hash_key = format!("rewards:video:{}", video_id);
        let milestone: Option<String> = conn.hget(&video_hash_key, "last_milestone").await?;
        Ok(milestone.and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub async fn set_last_milestone(&self, video_id: &str, milestone: u64) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let video_hash_key = format!("rewards:video:{}", video_id);
        conn.hset(&video_hash_key, "last_milestone", milestone).await?;
        Ok(())
    }
}