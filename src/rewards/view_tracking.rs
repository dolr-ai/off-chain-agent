use crate::yral_auth::dragonfly::DragonflyPool;
use anyhow::{Context, Result};
use candid::Principal;
use redis::AsyncCommands;
use sha1::{Digest, Sha1};
use std::sync::Arc;

const LUA_ATOMIC_VIEW_SCRIPT: &str = r#"
    --!df flags=allow-undeclared-keys
    -- Atomic operation for view counting with config change handling
    -- This script only runs for logged-in users
    -- Using HSET to store both count and config_version in single hash
    local views_set = KEYS[1]  -- impressions:rewards:views:{video_id} (set of user IDs)
    local video_hash = KEYS[2]  -- impressions:rewards:video:{video_id} (hash with count & config_version)
    local config_version_key = KEYS[3]  -- impressions:rewards:config:version

    local user_id = ARGV[1]

    -- Always increment total_count_all (counts all views including duplicates)
    redis.call('HINCRBY', video_hash, 'total_count_all', 1)

    -- Get current global config version from Redis
    local current_global_version = redis.call('GET', config_version_key) or '1'

    -- Get video's stored config version from hash
    local video_config_version = redis.call('HGET', video_hash, 'config_version') or '0'

    -- If config changed for THIS video, reset its counter
    if video_config_version ~= current_global_version then
        -- Reset counter to 0 and update config version
        redis.call('HSET', video_hash, 'count', 0, 'config_version', current_global_version)
        -- Note: We do NOT delete the views_set, so users who already viewed cannot view again
    end

    -- Check if user already viewed (critical check for unique views)
    local added = redis.call('SADD', views_set, user_id)
    if added == 1 then
        -- New unique logged-in view: increment unique counters
        redis.call('HINCRBY', video_hash, 'count', 1)
        redis.call('HINCRBY', video_hash, 'total_count_loggedin', 1)
        return redis.call('HGET', video_hash, 'count')
    else
        return nil  -- Duplicate view (total_count_all already incremented above)
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
    pool: Arc<DragonflyPool>,
    script_sha: Option<String>,
}

impl ViewTracker {
    pub fn new(pool: Arc<DragonflyPool>) -> Self {
        Self {
            pool,
            script_sha: None,
        }
    }

    pub async fn load_lua_scripts(&mut self) -> Result<String> {
        let sha: String = self
            .pool
            .execute_with_retry(|mut conn| {
                async move {
                    redis::cmd("SCRIPT")
                        .arg("LOAD")
                        .arg(LUA_ATOMIC_VIEW_SCRIPT)
                        .query_async(&mut conn)
                        .await
                }
            })
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
        is_logged_in: bool,
    ) -> Result<Option<u64>> {
        // Fast path for non-logged-in: just increment total_count_all
        if !is_logged_in {
            let video_hash_key = format!("impressions:rewards:video:{}", video_id);
            self.pool
                .execute_with_retry(|mut conn| {
                    let key = video_hash_key.clone();
                    async move { conn.hincr::<_, _, _, ()>(&key, "total_count_all", 1).await }
                })
                .await?;
            return Ok(None);
        }

        // Logged-in path: use Lua script for atomic duplicate checking
        let views_set_key = format!("impressions:rewards:views:{}", video_id);
        let video_hash_key = format!("impressions:rewards:video:{}", video_id);
        let config_version_key = "impressions:rewards:config:version".to_string();
        let user_id_str = user_id.to_string();
        let sha = self.script_sha.clone();

        // Try to use the loaded script SHA, fallback to EVAL if not loaded
        let result: Option<u64> = self
            .pool
            .execute_with_retry(|mut conn| {
                let views_key = views_set_key.clone();
                let video_key = video_hash_key.clone();
                let config_key = config_version_key.clone();
                let user = user_id_str.clone();
                let script_sha = sha.clone();

                async move {
                    if let Some(sha_str) = &script_sha {
                        // Use EVALSHA for better performance
                        let evalsha_result = redis::cmd("EVALSHA")
                            .arg(sha_str)
                            .arg(3) // number of keys
                            .arg(&views_key)
                            .arg(&video_key)
                            .arg(&config_key)
                            .arg(&user)
                            .query_async(&mut conn)
                            .await;

                        match evalsha_result {
                            Ok(result) => Ok(result),
                            Err(e) => {
                                // If script not in cache, use EVAL
                                log::warn!("EVALSHA failed, falling back to EVAL: {}", e);
                                redis::cmd("EVAL")
                                    .arg(LUA_ATOMIC_VIEW_SCRIPT)
                                    .arg(3)
                                    .arg(&views_key)
                                    .arg(&video_key)
                                    .arg(&config_key)
                                    .arg(&user)
                                    .query_async(&mut conn)
                                    .await
                            }
                        }
                    } else {
                        // Script not loaded, use EVAL
                        redis::cmd("EVAL")
                            .arg(LUA_ATOMIC_VIEW_SCRIPT)
                            .arg(3)
                            .arg(&views_key)
                            .arg(&video_key)
                            .arg(&config_key)
                            .arg(&user)
                            .query_async(&mut conn)
                            .await
                    }
                }
            })
            .await
            .context("Failed to execute view tracking script")?;

        Ok(result)
    }

    pub async fn get_view_count(&self, video_id: &str) -> Result<u64> {
        let video_hash_key = format!("impressions:rewards:video:{}", video_id);
        let count: Option<String> = self
            .pool
            .execute_with_retry(|mut conn| {
                let key = video_hash_key.clone();
                async move { conn.hget(&key, "count").await }
            })
            .await?;
        Ok(count.and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub async fn get_last_milestone(&self, video_id: &str) -> Result<u64> {
        let video_hash_key = format!("impressions:rewards:video:{}", video_id);
        let milestone: Option<String> = self
            .pool
            .execute_with_retry(|mut conn| {
                let key = video_hash_key.clone();
                async move { conn.hget(&key, "last_milestone").await }
            })
            .await?;
        Ok(milestone.and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub async fn set_last_milestone(&self, video_id: &str, milestone: u64) -> Result<()> {
        let video_hash_key = format!("impressions:rewards:video:{}", video_id);
        self.pool
            .execute_with_retry(|mut conn| {
                let key = video_hash_key.clone();
                async move { conn.hset::<_, _, _, ()>(&key, "last_milestone", milestone).await }
            })
            .await?;
        Ok(())
    }

    pub async fn get_total_count_loggedin(&self, video_id: &str) -> Result<u64> {
        let video_hash_key = format!("impressions:rewards:video:{}", video_id);
        let count: Option<String> = self
            .pool
            .execute_with_retry(|mut conn| {
                let key = video_hash_key.clone();
                async move { conn.hget(&key, "total_count_loggedin").await }
            })
            .await?;
        Ok(count.and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    pub async fn get_total_count_all(&self, video_id: &str) -> Result<u64> {
        let video_hash_key = format!("impressions:rewards:video:{}", video_id);
        let count: Option<String> = self
            .pool
            .execute_with_retry(|mut conn| {
                let key = video_hash_key.clone();
                async move { conn.hget(&key, "total_count_all").await }
            })
            .await?;
        Ok(count.and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    /// Get all video stats in a single Redis call
    pub async fn get_all_video_stats(&self, video_id: &str) -> Result<(u64, u64, u64, u64)> {
        let video_hash_key = format!("impressions:rewards:video:{}", video_id);

        // Get all fields in one call
        let data: std::collections::HashMap<String, String> = self
            .pool
            .execute_with_retry(|mut conn| {
                let key = video_hash_key.clone();
                async move { conn.hgetall(&key).await }
            })
            .await?;

        let count = data.get("count").and_then(|s| s.parse().ok()).unwrap_or(0);
        let total_count_loggedin = data
            .get("total_count_loggedin")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let total_count_all = data
            .get("total_count_all")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let last_milestone = data
            .get("last_milestone")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        Ok((count, total_count_loggedin, total_count_all, last_milestone))
    }

    /// Get stats for multiple videos using Redis pipelining (batched)
    pub async fn get_bulk_video_stats(
        &self,
        video_ids: &[String],
    ) -> Result<std::collections::HashMap<String, (u64, u64, u64, u64)>> {
        if video_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        // Build pipeline with HGETALL for each video
        let mut pipe = redis::pipe();
        for video_id in video_ids {
            let video_hash_key = format!("impressions:rewards:video:{}", video_id);
            pipe.hgetall(&video_hash_key);
        }

        // Execute pipeline - all commands in single round-trip
        let results: Vec<std::collections::HashMap<String, String>> = self
            .pool
            .execute_with_retry(|mut conn| {
                let p = pipe.clone();
                async move { p.query_async(&mut conn).await }
            })
            .await?;

        // Parse results
        let mut response = std::collections::HashMap::new();
        for (i, video_id) in video_ids.iter().enumerate() {
            if let Some(data) = results.get(i) {
                let count = data.get("count").and_then(|s| s.parse().ok()).unwrap_or(0);
                let total_count_loggedin = data
                    .get("total_count_loggedin")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                let total_count_all = data
                    .get("total_count_all")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                let last_milestone = data
                    .get("last_milestone")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                response.insert(
                    video_id.clone(),
                    (count, total_count_loggedin, total_count_all, last_milestone),
                );
            }
        }

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;
    use uuid::Uuid;

    static INIT: Once = Once::new();

    fn init_crypto() {
        INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }

    fn test_key_prefix() -> String {
        format!("test_rewards_{}", Uuid::new_v4().to_string())
    }

    struct TestViewTracker {
        tracker: ViewTracker,
        key_prefix: String,
    }

    impl TestViewTracker {
        async fn new() -> Self {
            init_crypto();

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
            tracker
                .load_lua_scripts()
                .await
                .expect("Failed to load Lua scripts");

            Self {
                tracker,
                key_prefix,
            }
        }

        async fn cleanup(&self) -> Result<()> {
            let mut conn = self.tracker.pool.get().await?;

            let patterns = vec![
                format!("impressions:rewards:video:{}*", self.key_prefix),
                format!("impressions:rewards:views:{}*", self.key_prefix),
                "impressions:rewards:config:version".to_string(),
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

        let count = test_tracker
            .tracker
            .track_view(&video_id, &user_id, true)
            .await
            .unwrap();
        assert_eq!(count, Some(1));

        test_tracker.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_duplicate_view_prevention() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video2");
        let user_id = Principal::from_text("aaaaa-aa").unwrap();

        let count1 = test_tracker
            .tracker
            .track_view(&video_id, &user_id, true)
            .await
            .unwrap();
        assert_eq!(count1, Some(1));

        let count2 = test_tracker
            .tracker
            .track_view(&video_id, &user_id, true)
            .await
            .unwrap();
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

        let count1 = test_tracker
            .tracker
            .track_view(&video_id, &user1, true)
            .await
            .unwrap();
        assert_eq!(count1, Some(1));

        let count2 = test_tracker
            .tracker
            .track_view(&video_id, &user2, true)
            .await
            .unwrap();
        assert_eq!(count2, Some(2));

        let count3 = test_tracker
            .tracker
            .track_view(&video_id, &user3, true)
            .await
            .unwrap();
        assert_eq!(count3, Some(3));

        let total_count = test_tracker
            .tracker
            .get_view_count(&video_id)
            .await
            .unwrap();
        assert_eq!(total_count, 3);

        test_tracker.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_config_version_reset() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video4");
        let user1 = Principal::self_authenticating("reset_user1");
        let user2 = Principal::self_authenticating("reset_user2");
        let user3 = Principal::self_authenticating("reset_user3"); // New user after config change

        test_tracker
            .tracker
            .track_view(&video_id, &user1, true)
            .await
            .unwrap();
        test_tracker
            .tracker
            .track_view(&video_id, &user2, true)
            .await
            .unwrap();

        let count_before = test_tracker
            .tracker
            .get_view_count(&video_id)
            .await
            .unwrap();
        assert_eq!(count_before, 2);

        let mut conn = test_tracker.tracker.pool.get().await.unwrap();
        let _: () = conn
            .set::<_, _, ()>("impressions:rewards:config:version", "2")
            .await
            .unwrap();

        // After config change, users who already viewed should NOT be able to view again
        let count_after_reset = test_tracker
            .tracker
            .track_view(&video_id, &user1, true)
            .await
            .unwrap();
        assert_eq!(count_after_reset, None); // User1 already viewed, cannot view again

        let count_user2_after = test_tracker
            .tracker
            .track_view(&video_id, &user2, true)
            .await
            .unwrap();
        assert_eq!(count_user2_after, None); // User2 already viewed, cannot view again

        // New user should be able to view and counter starts from 1 (after reset)
        let count_user3 = test_tracker
            .tracker
            .track_view(&video_id, &user3, true)
            .await
            .unwrap();
        assert_eq!(count_user3, Some(1)); // New user can view, counter is 1 after reset

        test_tracker.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_view_count() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video5");

        let count = test_tracker
            .tracker
            .get_view_count(&video_id)
            .await
            .unwrap();
        assert_eq!(count, 0);

        let user1 = Principal::self_authenticating("count_user1");
        let user2 = Principal::self_authenticating("count_user2");

        test_tracker
            .tracker
            .track_view(&video_id, &user1, true)
            .await
            .unwrap();
        test_tracker
            .tracker
            .track_view(&video_id, &user2, true)
            .await
            .unwrap();

        let count = test_tracker
            .tracker
            .get_view_count(&video_id)
            .await
            .unwrap();
        assert_eq!(count, 2);

        test_tracker.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_milestone_tracking() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video6");

        let milestone = test_tracker
            .tracker
            .get_last_milestone(&video_id)
            .await
            .unwrap();
        assert_eq!(milestone, 0);

        test_tracker
            .tracker
            .set_last_milestone(&video_id, 5)
            .await
            .unwrap();

        let milestone = test_tracker
            .tracker
            .get_last_milestone(&video_id)
            .await
            .unwrap();
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
            let handle = tokio::spawn(async move { tracker.track_view(&vid, &user, true).await });
            handles.push(handle);
        }

        let mut successful_views = 0;
        for handle in handles {
            if let Ok(Ok(Some(_))) = handle.await {
                successful_views += 1;
            }
        }

        assert_eq!(successful_views, 10);

        let final_count = test_tracker
            .tracker
            .get_view_count(&video_id)
            .await
            .unwrap();
        assert_eq!(final_count, 10);

        test_tracker.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_total_count_loggedin_persists_on_config_reset() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video8");
        let user1 = Principal::self_authenticating("total_user1");
        let user2 = Principal::self_authenticating("total_user2");
        let user3 = Principal::self_authenticating("total_user3");

        // Track 2 logged-in views
        test_tracker
            .tracker
            .track_view(&video_id, &user1, true)
            .await
            .unwrap();
        test_tracker
            .tracker
            .track_view(&video_id, &user2, true)
            .await
            .unwrap();

        let count_before = test_tracker
            .tracker
            .get_view_count(&video_id)
            .await
            .unwrap();
        let total_loggedin_before = test_tracker
            .tracker
            .get_total_count_loggedin(&video_id)
            .await
            .unwrap();
        assert_eq!(count_before, 2);
        assert_eq!(total_loggedin_before, 2);

        // Change config version
        let mut conn = test_tracker.tracker.pool.get().await.unwrap();
        let _: () = conn
            .set::<_, _, ()>("impressions:rewards:config:version", "2")
            .await
            .unwrap();

        // Track a new user view after config change
        test_tracker
            .tracker
            .track_view(&video_id, &user3, true)
            .await
            .unwrap();

        // count should reset to 1, but total_count_loggedin should be 3
        let count_after = test_tracker
            .tracker
            .get_view_count(&video_id)
            .await
            .unwrap();
        let total_loggedin_after = test_tracker
            .tracker
            .get_total_count_loggedin(&video_id)
            .await
            .unwrap();
        assert_eq!(count_after, 1); // Reset counter
        assert_eq!(total_loggedin_after, 3); // Never resets

        test_tracker.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_total_count_all_includes_non_logged_in() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video9");
        let user1 = Principal::self_authenticating("mixed_user1");
        let user2 = Principal::self_authenticating("mixed_user2");
        let user3 = Principal::self_authenticating("mixed_user3");
        let user4 = Principal::self_authenticating("mixed_user4");

        // 2 logged-in views
        test_tracker
            .tracker
            .track_view(&video_id, &user1, true)
            .await
            .unwrap();
        test_tracker
            .tracker
            .track_view(&video_id, &user2, true)
            .await
            .unwrap();

        // 2 non-logged-in views
        test_tracker
            .tracker
            .track_view(&video_id, &user3, false)
            .await
            .unwrap();
        test_tracker
            .tracker
            .track_view(&video_id, &user4, false)
            .await
            .unwrap();

        let count = test_tracker
            .tracker
            .get_view_count(&video_id)
            .await
            .unwrap();
        let total_loggedin = test_tracker
            .tracker
            .get_total_count_loggedin(&video_id)
            .await
            .unwrap();
        let total_all = test_tracker
            .tracker
            .get_total_count_all(&video_id)
            .await
            .unwrap();

        assert_eq!(count, 2); // Only logged-in views
        assert_eq!(total_loggedin, 2); // Only logged-in views
        assert_eq!(total_all, 4); // All views

        test_tracker.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn test_total_counters_with_duplicates() {
        let test_tracker = TestViewTracker::new().await;
        let video_id = test_tracker.test_video_id("video10");
        let user1 = Principal::self_authenticating("dup_user1");

        // First view (logged-in)
        let result1 = test_tracker
            .tracker
            .track_view(&video_id, &user1, true)
            .await
            .unwrap();
        assert_eq!(result1, Some(1));

        // Duplicate view (logged-in) - should only increment total_count_all
        let result2 = test_tracker
            .tracker
            .track_view(&video_id, &user1, true)
            .await
            .unwrap();
        assert_eq!(result2, None);

        let count = test_tracker
            .tracker
            .get_view_count(&video_id)
            .await
            .unwrap();
        let total_loggedin = test_tracker
            .tracker
            .get_total_count_loggedin(&video_id)
            .await
            .unwrap();
        let total_all = test_tracker
            .tracker
            .get_total_count_all(&video_id)
            .await
            .unwrap();

        assert_eq!(count, 1); // Unique views only
        assert_eq!(total_loggedin, 1); // Unique logged-in views only
        assert_eq!(total_all, 2); // All views including duplicates

        test_tracker.cleanup().await.unwrap();
    }
}
