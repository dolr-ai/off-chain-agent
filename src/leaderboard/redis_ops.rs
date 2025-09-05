use anyhow::{Context, Result};
use candid::Principal;
use chrono::Utc;
use once_cell::sync::Lazy;
use redis::AsyncCommands;
use serde_json;
use sha1::{Digest, Sha1};
use std::collections::HashMap;

use super::types::*;
use crate::types::RedisPool;

// Constants for tie-breaking in leaderboard ranking
const TIEBREAKER_WEIGHT: f64 = 0.0000000001; // 10^-10, negligible impact on actual scores
const TIMESTAMP_BASE: i64 = 1_700_000_000; // Base timestamp to keep numbers manageable

// Lua script for atomic increment - simple and reliable
// This script needs to be loaded into Redis with SCRIPT LOAD
const LUA_INCREMENT_SCRIPT: &str = r#"
    local users_key = KEYS[1]
    local scores_key = KEYS[2]
    local principal = ARGV[1]
    local increment = tonumber(ARGV[2])
    local composite_score_offset = tonumber(ARGV[3])
    
    -- Atomically increment the score in the hash (handles nil as 0)
    local new_score = redis.call('HINCRBYFLOAT', users_key, principal, increment)
    
    -- Convert to number for composite score calculation
    new_score = tonumber(new_score)
    
    -- Calculate composite score: actual_score - timestamp_offset
    -- The offset is pre-calculated in Rust as (timestamp - base) * weight
    local composite_score = new_score - composite_score_offset
    
    -- Update sorted set with composite score
    redis.call('ZADD', scores_key, composite_score, principal)
    
    return tostring(new_score)
"#;

// Helper function to calculate SHA1 hash of a script
fn calculate_script_sha(script: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(script.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

// Lazily compute and store the SHA1 of the Lua script
// This is computed once on first access and then cached
static LUA_INCREMENT_SCRIPT_SHA: Lazy<String> = Lazy::new(|| {
    let sha = calculate_script_sha(LUA_INCREMENT_SCRIPT);
    log::info!("Calculated Lua script SHA: {}", sha);
    sha
});

#[derive(Clone)]
pub struct LeaderboardRedis {
    pool: RedisPool,
    key_prefix: String,
}

impl LeaderboardRedis {
    pub fn new(pool: RedisPool) -> Self {
        Self {
            pool,
            key_prefix: "leaderboard".to_string(),
        }
    }

    pub fn new_with_prefix(pool: RedisPool, key_prefix: String) -> Self {
        Self { pool, key_prefix }
    }

    // Load the Lua script into Redis and return its SHA
    pub async fn load_lua_scripts(&self) -> Result<String> {
        let mut conn = self.pool.get().await?;
        let sha: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(LUA_INCREMENT_SCRIPT)
            .query_async(&mut *conn)
            .await?;

        // Verify the SHA matches what we expect
        let expected_sha = &*LUA_INCREMENT_SCRIPT_SHA;
        if sha != *expected_sha {
            log::warn!(
                "Loaded script SHA {} doesn't match calculated SHA {}",
                sha,
                expected_sha
            );
        }

        Ok(sha)
    }

    // Key generators
    fn tournament_scores_key(&self, tournament_id: &str) -> String {
        format!("{}:tournament:{}:scores", self.key_prefix, tournament_id)
    }

    fn tournament_users_key(&self, tournament_id: &str) -> String {
        format!("{}:tournament:{}:users", self.key_prefix, tournament_id)
    }

    fn tournament_info_key(&self, tournament_id: &str) -> String {
        format!("{}:tournament:{}:info", self.key_prefix, tournament_id)
    }

    fn username_cache_key(&self, principal: &Principal) -> String {
        format!("{}:username:{}", self.key_prefix, principal)
    }

    fn current_tournament_key(&self) -> String {
        format!("{}:tournament:current", self.key_prefix)
    }

    fn upcoming_tournament_key(&self) -> String {
        format!("{}:tournament:upcoming", self.key_prefix)
    }

    fn tournament_history_key(&self) -> String {
        format!("{}:tournaments:history", self.key_prefix)
    }

    fn tournament_results_key(&self, tournament_id: &str) -> String {
        format!("{}:tournament:{}:results", self.key_prefix, tournament_id)
    }

    fn internal_users_key(&self) -> String {
        format!("{}:internal-users", self.key_prefix)
    }

    // Get current active tournament
    pub async fn get_current_tournament(&self) -> Result<Option<String>> {
        let mut conn = self.pool.get().await?;
        let tournament_id: Option<String> = conn.get(self.current_tournament_key()).await?;
        Ok(tournament_id)
    }

    // Set current tournament
    pub async fn set_current_tournament(&self, tournament_id: &str) -> Result<()> {
        let mut conn = self.pool.get().await?;
        conn.set::<_, _, ()>(self.current_tournament_key(), tournament_id)
            .await?;
        Ok(())
    }

    // Get upcoming tournament
    pub async fn get_upcoming_tournament(&self) -> Result<Option<String>> {
        let mut conn = self.pool.get().await?;
        let tournament_id: Option<String> = conn.get(self.upcoming_tournament_key()).await?;
        Ok(tournament_id)
    }

    // Set upcoming tournament
    pub async fn set_upcoming_tournament(&self, tournament_id: &str) -> Result<()> {
        let mut conn = self.pool.get().await?;
        conn.set::<_, _, ()>(self.upcoming_tournament_key(), tournament_id)
            .await?;
        Ok(())
    }

    // Clear upcoming tournament
    pub async fn clear_upcoming_tournament(&self) -> Result<()> {
        let mut conn = self.pool.get().await?;
        conn.del::<_, ()>(self.upcoming_tournament_key()).await?;
        Ok(())
    }

    // Get internal users for testing
    pub async fn get_internal_users(&self) -> Result<Vec<String>> {
        let mut conn = self.pool.get().await?;
        let users: Vec<String> = conn.smembers(self.internal_users_key()).await?;
        Ok(users)
    }

    // Get tournament info
    pub async fn get_tournament_info(&self, tournament_id: &str) -> Result<Option<Tournament>> {
        let mut conn = self.pool.get().await?;
        let data: Option<String> = conn.get(self.tournament_info_key(tournament_id)).await?;

        match data {
            Some(json_str) => {
                let tournament = serde_json::from_str(&json_str)
                    .context("Failed to deserialize tournament info")?;
                Ok(Some(tournament))
            }
            None => Ok(None),
        }
    }

    // Store tournament info
    pub async fn set_tournament_info(&self, tournament: &Tournament) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let json_str = serde_json::to_string(tournament)?;
        conn.set::<_, _, ()>(self.tournament_info_key(&tournament.id), json_str)
            .await?;
        Ok(())
    }

    // Update user score in tournament with generic metric
    pub async fn update_user_score(
        &self,
        tournament_id: &str,
        principal: Principal,
        metric_value: f64,
        operation: &ScoreOperation,
    ) -> Result<f64> {
        let mut conn = self.pool.get().await?;
        let scores_key = self.tournament_scores_key(tournament_id);
        let users_key = self.tournament_users_key(tournament_id);
        let timestamp = Utc::now().timestamp();

        let new_score = match operation {
            ScoreOperation::Increment => {
                // Calculate the composite score offset in Rust
                let time_component = (timestamp - TIMESTAMP_BASE) as f64;
                let composite_offset = time_component * TIEBREAKER_WEIGHT;

                // Get the pre-calculated SHA1 of our script for EVALSHA
                let script_sha = &*LUA_INCREMENT_SCRIPT_SHA;

                // Try EVALSHA first for better performance, fallback to EVAL on NOSCRIPT
                let score_str: String = {
                    // First attempt with EVALSHA
                    let result: Result<String, redis::RedisError> = redis::cmd("EVALSHA")
                        .arg(script_sha)
                        .arg(2) // number of keys
                        .arg(&users_key)
                        .arg(&scores_key)
                        .arg(principal.to_string())
                        .arg(metric_value)
                        .arg(composite_offset)
                        .query_async(&mut *conn)
                        .await;

                    match result {
                        Ok(s) => s,
                        Err(e) => {
                            // Check for NOSCRIPT error (different formats possible)
                            let error_str = e.to_string();
                            if error_str.contains("NOSCRIPT")
                                || error_str.contains("No matching script")
                                || e.kind() == redis::ErrorKind::NoScriptError
                            {
                                log::debug!(
                                    "Script not in cache, loading with EVAL for SHA: {}",
                                    script_sha
                                );

                                // Script not loaded, use EVAL which loads it automatically
                                redis::cmd("EVAL")
                                    .arg(LUA_INCREMENT_SCRIPT)
                                    .arg(2) // number of keys
                                    .arg(&users_key)
                                    .arg(&scores_key)
                                    .arg(principal.to_string())
                                    .arg(metric_value)
                                    .arg(composite_offset)
                                    .query_async(&mut *conn)
                                    .await
                                    .map_err(|e| {
                                        log::error!("EVAL failed after NOSCRIPT: {:?}", e);
                                        e
                                    })?
                            } else {
                                // Some other error, propagate it
                                log::error!("EVALSHA failed with non-NOSCRIPT error: {:?}", e);
                                return Err(e.into());
                            }
                        }
                    }
                };

                // Parse the returned string to f64
                score_str
                    .parse::<f64>()
                    .context("Failed to parse score from Lua script")?
            }
            ScoreOperation::Set => {
                return Err(anyhow::anyhow!(
                    "Set operation is not supported in update_user_score. Use set_user_score instead."
                ));
            }
            ScoreOperation::SetIfHigher => {
                return Err(anyhow::anyhow!(
                    "SetIfHigher operation is not supported in update_user_score. Use set_user_score_if_higher instead."
                ));
            }
        };

        Ok(new_score)
    }

    // Cache username
    pub async fn cache_username(
        &self,
        principal: Principal,
        username: &str,
        ttl: u64,
    ) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let key = self.username_cache_key(&principal);
        conn.set_ex::<_, _, ()>(&key, username, ttl).await?;
        Ok(())
    }

    // Get cached username
    pub async fn get_cached_username(&self, principal: Principal) -> Result<Option<String>> {
        let mut conn = self.pool.get().await?;
        let username: Option<String> = conn.get(self.username_cache_key(&principal)).await?;
        Ok(username)
    }

    // Get cached usernames in bulk
    pub async fn get_cached_usernames_bulk(
        &self,
        principals: &[Principal],
    ) -> Result<HashMap<Principal, String>> {
        if principals.is_empty() {
            return Ok(HashMap::new());
        }

        let mut conn = self.pool.get().await?;
        let mut username_map = HashMap::with_capacity(principals.len());

        const BATCH_SIZE: usize = 100;
        let total_batches = principals.len().div_ceil(BATCH_SIZE);

        for (batch_idx, batch) in principals.chunks(BATCH_SIZE).enumerate() {
            // Build keys for this batch
            let keys: Vec<String> = batch.iter().map(|p| self.username_cache_key(p)).collect();

            // Log progress for large operations
            if total_batches > 10 && (batch_idx == 0 || batch_idx % 10 == 0) {
                log::debug!(
                    "Fetching username batch {}/{} ({} principals)",
                    batch_idx + 1,
                    total_batches,
                    batch.len()
                );
            }

            // Fetch this batch
            match conn.mget(&keys).await {
                Ok(batch_results) => {
                    let batch_results: Vec<Option<String>> = batch_results;
                    // Validate batch size
                    if batch_results.len() != batch.len() {
                        log::warn!(
                            "Batch {} returned {} results but expected {}",
                            batch_idx + 1,
                            batch_results.len(),
                            batch.len()
                        );
                        continue;
                    }

                    // Add successful results to map
                    for (principal, username_opt) in batch.iter().zip(batch_results.iter()) {
                        if let Some(username) = username_opt {
                            username_map.insert(*principal, username.clone());
                        }
                    }
                }
                Err(e) => {
                    log::error!(
                        "Failed to fetch username batch {}/{}: {:?}",
                        batch_idx + 1,
                        total_batches,
                        e
                    );
                    // Continue with next batch instead of failing entirely
                    continue;
                }
            }
        }

        log::debug!(
            "Retrieved {} usernames out of {} principals",
            username_map.len(),
            principals.len()
        );

        Ok(username_map)
    }

    // Get user scores in bulk
    pub async fn get_user_scores_bulk(
        &self,
        tournament_id: &str,
        principals: &[String],
    ) -> Result<HashMap<String, f64>> {
        if principals.is_empty() {
            return Ok(HashMap::new());
        }

        let mut conn = self.pool.get().await?;
        let key = self.tournament_users_key(tournament_id);
        let mut score_map = HashMap::with_capacity(principals.len());

        // Use HMGET to fetch all scores in one call
        let results: Vec<Option<f64>> = redis::cmd("HMGET")
            .arg(&key)
            .arg(principals)
            .query_async(&mut *conn)
            .await
            .context("Failed to fetch user scores in bulk")?;

        // Process results and build map
        for (principal_str, score_opt) in principals.iter().zip(results.iter()) {
            if let Some(score) = score_opt {
                score_map.insert(principal_str.clone(), *score);
            }
        }

        Ok(score_map)
    }

    // Get leaderboard with pagination
    pub async fn get_leaderboard(
        &self,
        tournament_id: &str,
        start: isize,
        stop: isize,
        sort_order: SortOrder,
    ) -> Result<Vec<(String, f64)>> {
        let mut conn = self.pool.get().await?;
        let scores_key = self.tournament_scores_key(tournament_id);

        // Get ranking from sorted set (contains composite scores for tie-breaking)
        let ranked_members: Vec<(String, f64)> = match sort_order {
            SortOrder::Asc => conn.zrange_withscores(&scores_key, start, stop).await?,
            SortOrder::Desc => conn.zrevrange_withscores(&scores_key, start, stop).await?,
        };

        if ranked_members.is_empty() {
            return Ok(vec![]);
        }

        // Collect all principal strings for batch fetching
        let principal_strings: Vec<String> = ranked_members
            .iter()
            .map(|(principal_str, _)| principal_str.clone())
            .collect();

        // Batch fetch all user scores
        let score_map = self
            .get_user_scores_bulk(tournament_id, &principal_strings)
            .await?;

        // Build result with actual scores
        let mut results = Vec::with_capacity(ranked_members.len());
        for (principal_str, _composite_score) in ranked_members {
            // Get actual score from hash, fallback to 0 if not found
            let actual_score = score_map.get(&principal_str).copied().unwrap_or(0.0);

            results.push((principal_str, actual_score));
        }

        Ok(results)
    }

    // Get user rank
    pub async fn get_user_rank(
        &self,
        tournament_id: &str,
        principal: Principal,
    ) -> Result<Option<u32>> {
        let mut conn = self.pool.get().await?;
        let key = self.tournament_scores_key(tournament_id);

        // ZREVRANK returns 0-based rank for highest to lowest
        let rank: Option<isize> = conn.zrevrank(&key, principal.to_string()).await?;

        Ok(rank.map(|r| (r + 1) as u32)) // Convert to 1-based rank
    }

    // Get user score
    pub async fn get_user_score(
        &self,
        tournament_id: &str,
        principal: Principal,
    ) -> Result<Option<f64>> {
        // Get score directly from hash
        let mut conn = self.pool.get().await?;
        let key = self.tournament_users_key(tournament_id);
        let score: Option<f64> = conn.hget(&key, principal.to_string()).await?;
        Ok(score)
    }

    // Get total participants
    pub async fn get_total_participants(&self, tournament_id: &str) -> Result<u32> {
        let mut conn = self.pool.get().await?;
        let key = self.tournament_scores_key(tournament_id);

        let count: usize = conn.zcard(&key).await?;
        Ok(count as u32)
    }

    // Search users by username (using cached usernames)
    pub async fn search_users(
        &self,
        tournament_id: &str,
        query: &str,
        limit: u32,
        sort_order: SortOrder,
    ) -> Result<Vec<(Principal, f64)>> {
        let mut conn = self.pool.get().await?;

        // Get all participants with scores (sorted by composite score)
        let scores_key = self.tournament_scores_key(tournament_id);
        let participants: Vec<(String, f64)> = match sort_order {
            SortOrder::Asc => conn.zrange_withscores(&scores_key, 0, -1).await?,
            SortOrder::Desc => conn.zrevrange_withscores(&scores_key, 0, -1).await?,
        };

        if participants.is_empty() {
            return Ok(vec![]);
        }

        // Collect all principal strings for bulk operations
        let principal_strings: Vec<String> = participants
            .iter()
            .map(|(principal_str, _)| principal_str.clone())
            .collect();

        // Parse all valid principals
        let principals: Vec<Principal> = participants
            .iter()
            .filter_map(|(principal_str, _)| Principal::from_text(principal_str).ok())
            .collect();

        // Batch retrieve all usernames and all user scores in parallel
        let username_map = self.get_cached_usernames_bulk(&principals).await?;
        let score_map = self
            .get_user_scores_bulk(tournament_id, &principal_strings)
            .await?;

        // Filter and build results with actual scores
        let mut results = Vec::new();
        let query_lower = query.to_lowercase();

        for (principal_str, _composite_score) in participants.iter() {
            if let Ok(principal) = Principal::from_text(principal_str) {
                if let Some(username) = username_map.get(&principal) {
                    if username.to_lowercase().contains(&query_lower) {
                        // Get actual score from pre-fetched scores
                        let actual_score = score_map.get(principal_str).copied().unwrap_or(0.0); // Fallback to 0 if no score

                        results.push((principal, actual_score));
                        if results.len() >= limit as usize {
                            break;
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    // Add tournament to history
    pub async fn add_to_history(&self, tournament_id: &str) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let key = self.tournament_history_key();

        // Add to left of list (most recent first)
        conn.lpush::<_, _, ()>(&key, tournament_id).await?;

        // Keep only last 5 tournaments
        conn.ltrim::<_, ()>(&key, 0, 4).await?;

        Ok(())
    }

    // Get tournament history
    pub async fn get_tournament_history(&self, limit: isize) -> Result<Vec<String>> {
        let mut conn = self.pool.get().await?;
        let key = self.tournament_history_key();

        let history: Vec<String> = conn.lrange(&key, 0, limit - 1).await?;
        Ok(history)
    }

    // Save tournament results (winners with their rewards)
    pub async fn save_tournament_results(&self, results: &TournamentResult) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let key = self.tournament_results_key(&results.tournament_id);

        let serialized =
            serde_json::to_string(results).context("Failed to serialize tournament results")?;

        // Store permanently (no expiry)
        conn.set::<_, _, ()>(&key, serialized).await?;
        Ok(())
    }

    // Get saved tournament results
    pub async fn get_tournament_results(
        &self,
        tournament_id: &str,
    ) -> Result<Option<TournamentResult>> {
        let mut conn = self.pool.get().await?;
        let key = self.tournament_results_key(tournament_id);

        let data: Option<String> = conn.get(&key).await?;

        match data {
            Some(json_str) => {
                let results = serde_json::from_str(&json_str)
                    .context("Failed to deserialize tournament results")?;
                Ok(Some(results))
            }
            None => Ok(None),
        }
    }

    // User's last tournament tracking methods
    pub async fn save_user_last_tournament(
        &self,
        principal: Principal,
        info: &UserLastTournament,
    ) -> Result<()> {
        let mut conn = self.pool.get().await?;
        let key = format!("{}:user_last_tournament", self.key_prefix);
        let json_value =
            serde_json::to_string(info).context("Failed to serialize user last tournament info")?;

        conn.hset::<_, _, _, ()>(&key, principal.to_string(), json_value)
            .await?;
        Ok(())
    }

    pub async fn get_user_last_tournament(
        &self,
        principal: Principal,
    ) -> Result<Option<UserLastTournament>> {
        let mut conn = self.pool.get().await?;
        let key = format!("{}:user_last_tournament", self.key_prefix);

        let data: Option<String> = conn.hget(&key, principal.to_string()).await?;

        match data {
            Some(json_str) => {
                let info = serde_json::from_str(&json_str)
                    .context("Failed to deserialize user last tournament info")?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    pub async fn mark_last_tournament_seen(&self, principal: Principal) -> Result<()> {
        // Get existing info
        if let Some(mut info) = self.get_user_last_tournament(principal).await? {
            // Update status to seen
            info.status = "seen".to_string();
            // Save back
            self.save_user_last_tournament(principal, &info).await?;
        }
        Ok(())
    }

    pub async fn save_batch_user_last_tournaments(
        &self,
        entries: Vec<(Principal, UserLastTournament)>,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut conn = self.pool.get().await?;
        let key = format!("{}:user_last_tournament", self.key_prefix);

        // Use pipeline for batch operations
        let mut pipeline = redis::pipe();

        for (principal, info) in entries {
            let json_value = serde_json::to_string(&info)
                .context("Failed to serialize user last tournament info")?;
            pipeline.hset(&key, principal.to_string(), json_value);
        }

        pipeline.query_async::<()>(&mut *conn).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::Principal;
    use futures::future;
    use std::time::{SystemTime, UNIX_EPOCH};
    use uuid::Uuid;

    // Test helper to create isolated test keys
    fn test_key_prefix() -> String {
        format!("test:leaderboard:{}", Uuid::new_v4())
    }

    // Test helper to create a test Redis instance with isolated keys
    struct TestLeaderboardRedis {
        redis: LeaderboardRedis,
        key_prefix: String,
    }

    impl TestLeaderboardRedis {
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
            Self {
                redis: LeaderboardRedis::new_with_prefix(pool, key_prefix.clone()),
                key_prefix,
            }
        }

        // Cleanup helper to delete all test keys after test completion
        async fn cleanup(&self) -> Result<()> {
            let mut conn = self.redis.pool.get().await?;
            let pattern = format!("{}:*", self.key_prefix);

            // Use SCAN instead of KEYS for better performance
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
            Ok(())
        }
    }

    // Test fixture for creating test tournaments
    fn create_test_tournament(id: &str) -> Tournament {
        Tournament {
            id: id.to_string(),
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            end_time: (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 86400) as i64, // 1 day later
            prize_pool: 1000000.0,
            prize_token: TokenType::YRAL,
            status: TournamentStatus::Active,
            metric_type: MetricType::GamesPlayed,
            metric_display_name: "Games Played".to_string(),
            allowed_sources: vec!["web".to_string()],
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            updated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        }
    }

    // Test fixture for creating test principals
    fn create_test_principals(count: usize) -> Vec<Principal> {
        (0..count)
            .map(|i| {
                // Create principals from bytes to ensure they're valid and unique
                let mut bytes = vec![0u8; 29];
                bytes[0] = (i & 0xFF) as u8;
                bytes[1] = ((i >> 8) & 0xFF) as u8;
                bytes[2] = ((i >> 16) & 0xFF) as u8;
                bytes[3] = ((i >> 24) & 0xFF) as u8;

                Principal::from_slice(&bytes)
            })
            .collect()
    }

    #[tokio::test]
    async fn test_tournament_lifecycle() {
        let test_redis = TestLeaderboardRedis::new().await;
        let redis = &test_redis.redis;
        let tournament_id = format!("test_tournament_{}", Uuid::new_v4());
        let tournament = create_test_tournament(&tournament_id);

        // Test setting tournament info
        redis
            .set_tournament_info(&tournament)
            .await
            .expect("Failed to set tournament info");

        // Test getting tournament info
        let retrieved = redis
            .get_tournament_info(&tournament_id)
            .await
            .expect("Failed to get tournament info")
            .expect("Tournament not found");

        assert_eq!(retrieved.id, tournament.id);
        assert_eq!(retrieved.prize_pool, tournament.prize_pool);
        assert_eq!(retrieved.metric_type, tournament.metric_type);

        // Test setting current tournament
        redis
            .set_current_tournament(&tournament_id)
            .await
            .expect("Failed to set current tournament");

        // Test getting current tournament
        let current = redis
            .get_current_tournament()
            .await
            .expect("Failed to get current tournament")
            .expect("No current tournament");

        assert_eq!(current, tournament_id);

        // Cleanup
        test_redis.cleanup().await.expect("Failed to cleanup");
    }

    #[tokio::test]
    async fn test_score_operations() {
        let test_redis = TestLeaderboardRedis::new().await;
        let redis = &test_redis.redis;
        let tournament_id = format!("test_tournament_{}", Uuid::new_v4());
        let principals = create_test_principals(5);

        // Test increment operation
        let score1 = redis
            .update_user_score(
                &tournament_id,
                principals[0],
                10.0,
                &ScoreOperation::Increment,
            )
            .await
            .expect("Failed to update score");
        assert_eq!(score1, 10.0);

        // Test increment again (should add)
        let score2 = redis
            .update_user_score(
                &tournament_id,
                principals[0],
                5.0,
                &ScoreOperation::Increment,
            )
            .await
            .expect("Failed to update score");
        assert_eq!(score2, 15.0);

        // Cleanup
        test_redis.cleanup().await.expect("Failed to cleanup");
    }

    #[tokio::test]
    async fn test_leaderboard_queries() {
        let test_redis = TestLeaderboardRedis::new().await;
        let redis = &test_redis.redis;
        let tournament_id = format!("test_tournament_{}", Uuid::new_v4());
        let principals = create_test_principals(10);

        // Add scores for multiple users
        for (i, principal) in principals.iter().enumerate() {
            redis
                .update_user_score(
                    &tournament_id,
                    *principal,
                    100.0 - i as f64 * 10.0, // Descending scores
                    &ScoreOperation::Increment,
                )
                .await
                .expect("Failed to update score");
        }

        // Test getting leaderboard (top 5)
        let top5 = redis
            .get_leaderboard(&tournament_id, 0, 4, SortOrder::Desc)
            .await
            .expect("Failed to get leaderboard");

        assert_eq!(top5.len(), 5);
        assert_eq!(top5[0].1, 100.0); // First place score
        assert_eq!(top5[4].1, 60.0); // Fifth place score

        // Test pagination
        let next5 = redis
            .get_leaderboard(&tournament_id, 5, 9, SortOrder::Desc)
            .await
            .expect("Failed to get leaderboard");

        assert_eq!(next5.len(), 5);
        assert_eq!(next5[0].1, 50.0); // Sixth place score
        assert_eq!(next5[4].1, 10.0); // Tenth place score

        // Test getting user rank
        let rank = redis
            .get_user_rank(&tournament_id, principals[2])
            .await
            .expect("Failed to get user rank")
            .expect("User not found");

        assert_eq!(rank, 3); // Third place (0-indexed becomes 1-indexed)

        // Test getting user score
        let score = redis
            .get_user_score(&tournament_id, principals[4])
            .await
            .expect("Failed to get user score")
            .expect("Score not found");

        assert_eq!(score, 60.0);

        // Test total participants
        let total = redis
            .get_total_participants(&tournament_id)
            .await
            .expect("Failed to get total participants");

        assert_eq!(total, 10);

        // Cleanup
        test_redis.cleanup().await.expect("Failed to cleanup");
    }

    #[tokio::test]
    async fn test_username_caching() {
        let test_redis = TestLeaderboardRedis::new().await;
        let redis = &test_redis.redis;
        let principal = create_test_principals(1)[0];
        let username = "test_user_123";

        // Cache username
        redis
            .cache_username(principal, username, 60)
            .await
            .expect("Failed to cache username");

        // Retrieve cached username
        let cached = redis
            .get_cached_username(principal)
            .await
            .expect("Failed to get cached username")
            .expect("Username not found");

        assert_eq!(cached, username);

        // Cleanup
        test_redis.cleanup().await.expect("Failed to cleanup");
    }

    #[tokio::test]
    async fn test_search_users() {
        let test_redis = TestLeaderboardRedis::new().await;
        let redis = &test_redis.redis;
        let tournament_id = format!("test_tournament_{}", Uuid::new_v4());
        let principals = create_test_principals(5);

        // Add scores and cache usernames
        let usernames = vec!["alice", "bob", "charlie", "alice2", "david"];
        for (i, (principal, username)) in principals.iter().zip(usernames.iter()).enumerate() {
            redis
                .update_user_score(
                    &tournament_id,
                    *principal,
                    100.0 - i as f64 * 10.0,
                    &ScoreOperation::Increment,
                )
                .await
                .expect("Failed to update score");

            redis
                .cache_username(*principal, username, 3600)
                .await
                .expect("Failed to cache username");
        }

        // Search for "alice"
        let results = redis
            .search_users(&tournament_id, "alice", 10, SortOrder::Desc)
            .await
            .expect("Failed to search users");

        assert_eq!(results.len(), 2); // Should find "alice" and "alice2"

        // Verify principals match
        let alice_principals: Vec<Principal> = results.iter().map(|(p, _)| *p).collect();
        assert!(alice_principals.contains(&principals[0]));
        assert!(alice_principals.contains(&principals[3]));

        // Cleanup
        test_redis.cleanup().await.expect("Failed to cleanup");
    }

    #[tokio::test]
    async fn test_tournament_history() {
        let test_redis = TestLeaderboardRedis::new().await;
        let redis = &test_redis.redis;

        // Create and add multiple tournaments to history
        let tournament_ids: Vec<String> = (0..5)
            .map(|i| format!("historical_tournament_{}", i))
            .collect();

        for id in &tournament_ids {
            redis
                .add_to_history(id)
                .await
                .expect("Failed to add to history");
        }

        // Get history (should be in reverse order - most recent first)
        let history = redis
            .get_tournament_history(3)
            .await
            .expect("Failed to get tournament history");

        assert_eq!(history.len(), 3);
        assert_eq!(history[0], tournament_ids[4]); // Most recent
        assert_eq!(history[1], tournament_ids[3]);
        assert_eq!(history[2], tournament_ids[2]);

        // Add another tournament (should maintain max of 5)
        let new_tournament = "historical_tournament_6";
        redis
            .add_to_history(new_tournament)
            .await
            .expect("Failed to add to history");

        let updated_history = redis
            .get_tournament_history(10)
            .await
            .expect("Failed to get tournament history");

        assert_eq!(updated_history.len(), 5); // Should maintain max of 5
        assert_eq!(updated_history[0], new_tournament); // Most recent
        assert!(!updated_history.contains(&tournament_ids[0].to_string())); // Oldest should be removed

        // Cleanup
        test_redis.cleanup().await.expect("Failed to cleanup");
    }

    #[tokio::test]
    async fn test_concurrent_score_updates() {
        let test_redis = TestLeaderboardRedis::new().await;
        let redis = &test_redis.redis;
        let tournament_id = format!("test_tournament_{}", Uuid::new_v4());
        let principal = create_test_principals(1)[0];

        // Simulate concurrent increments
        let futures: Vec<_> = (0..10)
            .map(|_| {
                let redis_clone = redis.clone();
                let tid = tournament_id.clone();
                async move {
                    redis_clone
                        .update_user_score(&tid, principal, 1.0, &ScoreOperation::Increment)
                        .await
                }
            })
            .collect();

        // Execute all concurrently
        let _results = future::try_join_all(futures)
            .await
            .expect("Failed to execute concurrent updates");

        // Final score should be 10.0 (10 increments of 1.0)
        let final_score = redis
            .get_user_score(&tournament_id, principal)
            .await
            .expect("Failed to get user score")
            .expect("Score not found");

        assert_eq!(final_score, 10.0);

        // Cleanup
        test_redis.cleanup().await.expect("Failed to cleanup");
    }

    #[tokio::test]
    async fn test_empty_leaderboard() {
        let test_redis = TestLeaderboardRedis::new().await;
        let redis = &test_redis.redis;
        let tournament_id = format!("test_tournament_{}", Uuid::new_v4());

        // Test getting leaderboard from empty tournament
        let leaderboard = redis
            .get_leaderboard(&tournament_id, 0, 99, SortOrder::Desc)
            .await
            .expect("Failed to get leaderboard");

        assert_eq!(leaderboard.len(), 0);

        // Test total participants on empty tournament
        let total = redis
            .get_total_participants(&tournament_id)
            .await
            .expect("Failed to get total participants");

        assert_eq!(total, 0);

        // Test user rank for non-existent user
        let principal = create_test_principals(1)[0];
        let rank = redis
            .get_user_rank(&tournament_id, principal)
            .await
            .expect("Failed to get user rank");

        assert!(rank.is_none());

        // Cleanup
        test_redis.cleanup().await.expect("Failed to cleanup");
    }

    #[tokio::test]
    async fn test_tournament_status_transitions() {
        let test_redis = TestLeaderboardRedis::new().await;
        let redis = &test_redis.redis;
        let tournament_id = format!("test_tournament_{}", Uuid::new_v4());
        let mut tournament = create_test_tournament(&tournament_id);

        // Start with Upcoming status
        tournament.status = TournamentStatus::Upcoming;
        redis
            .set_tournament_info(&tournament)
            .await
            .expect("Failed to set tournament info");

        // Transition to Active
        tournament.status = TournamentStatus::Active;
        redis
            .set_tournament_info(&tournament)
            .await
            .expect("Failed to update tournament info");

        let retrieved = redis
            .get_tournament_info(&tournament_id)
            .await
            .expect("Failed to get tournament info")
            .expect("Tournament not found");
        assert_eq!(retrieved.status, TournamentStatus::Active);

        // Transition to Finalizing
        tournament.status = TournamentStatus::Finalizing;
        redis
            .set_tournament_info(&tournament)
            .await
            .expect("Failed to update tournament info");

        // Transition to Completed
        tournament.status = TournamentStatus::Completed;
        redis
            .set_tournament_info(&tournament)
            .await
            .expect("Failed to update tournament info");

        let final_status = redis
            .get_tournament_info(&tournament_id)
            .await
            .expect("Failed to get tournament info")
            .expect("Tournament not found");
        assert_eq!(final_status.status, TournamentStatus::Completed);

        // Cleanup
        test_redis.cleanup().await.expect("Failed to cleanup");
    }
}
