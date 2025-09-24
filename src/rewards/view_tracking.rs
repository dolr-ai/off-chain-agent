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
        -- Clear the views set to allow all users to view again
        redis.call('DEL', views_set)
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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn test_key_prefix() -> String {
        format!("test_rewards_{}", Uuid::new_v4().to_string())
    }

    struct TestViewTracker {
        tracker: ViewTracker,
        key_prefix: String,
    }

    impl TestViewTracker {
        async fn new() -> Self {
            let redis_url = std::env::var("TEST_REDIS_URL")
                .unwrap_or_else(|_| "redis://localhost:6379".to_string());

            let manager = bb8_redis::RedisConnectionManager::new(redis_url)
                .expect("Failed to create Redis connection manager");
            let pool = bb8::Pool::builder()
                .build(manager)
                .await
                .expect("Failed to build Redis pool");

            let key_prefix = test_key_prefix();
            let mut tracker = ViewTracker::new(pool);
            tracker.load_lua_scripts().await.expect("Failed to load Lua scripts");

            Self {
                tracker,
                key_prefix,
            }
        }

        async fn cleanup(&self) -> Result<()> {
            let mut conn = self.tracker.pool.get().await?;

            let patterns = vec![
                format!("rewards:video:{}*", self.key_prefix),
                format!("rewards:views:{}*", self.key_prefix),
                "rewards:config:version".to_string(),
            ];

            for pattern in patterns {
                let mut cursor = 0;
                loop {
                    let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                        .arg(cursor)
                        .arg("MATCH")
                        .arg(&pattern)
                        .arg("COUNT")
                        .arg(100)
                        .query_async(&mut *conn)
                        .await?;

                    if !keys.is_empty() {
                        let _: () = conn.del(keys).await?;
                    }

                    cursor = new_cursor;
                    if cursor == 0 {
                        break;
                    }
                }
            }
            Ok(())
        }

        fn test_video_id(&self, suffix: &str) -> String {
            format!("{}_{}", self.key_prefix, suffix)
        }
    }

    #[tokio::test]
    async fn test_track_new_view() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video1");
        let user_id = Principal::from_text("aaaaa-aa").unwrap();

        let count = test_tracker.tracker.track_view(&video_id, &user_id).await.unwrap();
        assert_eq!(count, Some(1));

        test_tracker.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_duplicate_view_prevention() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video2");
        let user_id = Principal::from_text("aaaaa-aa").unwrap();

        let count1 = test_tracker.tracker.track_view(&video_id, &user_id).await.unwrap();
        assert_eq!(count1, Some(1));

        let count2 = test_tracker.tracker.track_view(&video_id, &user_id).await.unwrap();
        assert_eq!(count2, None);

        test_tracker.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_users_viewing() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video3");

        let user1 = Principal::self_authenticating("user1");
        let user2 = Principal::self_authenticating("user2");
        let user3 = Principal::self_authenticating("user3");

        let count1 = test_tracker.tracker.track_view(&video_id, &user1).await.unwrap();
        assert_eq!(count1, Some(1));

        let count2 = test_tracker.tracker.track_view(&video_id, &user2).await.unwrap();
        assert_eq!(count2, Some(2));

        let count3 = test_tracker.tracker.track_view(&video_id, &user3).await.unwrap();
        assert_eq!(count3, Some(3));

        let total_count = test_tracker.tracker.get_view_count(&video_id).await.unwrap();
        assert_eq!(total_count, 3);

        test_tracker.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_config_version_reset() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video4");
        let user1 = Principal::self_authenticating("reset_user1");
        let user2 = Principal::self_authenticating("reset_user2");

        test_tracker.tracker.track_view(&video_id, &user1).await.unwrap();
        test_tracker.tracker.track_view(&video_id, &user2).await.unwrap();

        let count_before = test_tracker.tracker.get_view_count(&video_id).await.unwrap();
        assert_eq!(count_before, 2);

        let mut conn = test_tracker.tracker.pool.get().await.unwrap();
        let _: () = conn.set("rewards:config:version", "2").await.unwrap();

        // After config change, both users should be able to view again
        let count_after_reset = test_tracker.tracker.track_view(&video_id, &user1).await.unwrap();
        assert_eq!(count_after_reset, Some(1)); // Counter reset to 1

        let count_user2_after = test_tracker.tracker.track_view(&video_id, &user2).await.unwrap();
        assert_eq!(count_user2_after, Some(2)); // User2 can also view again

        test_tracker.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_view_count() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video5");

        let count = test_tracker.tracker.get_view_count(&video_id).await.unwrap();
        assert_eq!(count, 0);

        let user1 = Principal::self_authenticating("count_user1");
        let user2 = Principal::self_authenticating("count_user2");

        test_tracker.tracker.track_view(&video_id, &user1).await.unwrap();
        test_tracker.tracker.track_view(&video_id, &user2).await.unwrap();

        let count = test_tracker.tracker.get_view_count(&video_id).await.unwrap();
        assert_eq!(count, 2);

        test_tracker.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_milestone_tracking() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video6");

        let milestone = test_tracker.tracker.get_last_milestone(&video_id).await.unwrap();
        assert_eq!(milestone, 0);

        test_tracker.tracker.set_last_milestone(&video_id, 5).await.unwrap();

        let milestone = test_tracker.tracker.get_last_milestone(&video_id).await.unwrap();
        assert_eq!(milestone, 5);

        test_tracker.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_views() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video7");

        let users: Vec<Principal> = (0..10)
            .map(|i| Principal::self_authenticating(format!("test_user_{}", i)))
            .collect();

        let mut handles = vec![];
        for user in users {
            let tracker = test_tracker.tracker.clone();
            let vid = video_id.clone();
            let handle = tokio::spawn(async move {
                tracker.track_view(&vid, &user).await
            });
            handles.push(handle);
        }

        let mut successful_views = 0;
        for handle in handles {
            if let Ok(Ok(Some(_))) = handle.await {
                successful_views += 1;
            }
        }

        assert_eq!(successful_views, 10);

        let final_count = test_tracker.tracker.get_view_count(&video_id).await.unwrap();
        assert_eq!(final_count, 10);

        test_tracker.cleanup().await.unwrap();
    }
}