use anyhow::{Context, Result};
use bb8_redis::RedisConnectionManager;
use candid::Principal;
use ic_agent::Agent;
use redis::AsyncCommands;
use sha1::{Digest, Sha1};
use std::env;
use std::fs::OpenOptions;
use std::io::Write as IoWrite;
use std::time::Instant;
use yral_canisters_client::user_post_service::{FetchPostsArgs, UserPostService};

const LUA_MAX_SCRIPT: &str = r#"
local key = KEYS[1]
local new_val = tonumber(ARGV[1])
local current = tonumber(redis.call('HGET', key, 'total_count_all') or '0')
return redis.call('HSET', key, 'total_count_all', math.max(current, new_val))
"#;

fn calculate_script_sha(script: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(script.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Install default crypto provider for Rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    println!("🚀 Video View Count Backfill Script");
    println!("====================================\n");

    // Configuration
    let ic_url = env::var("IC_URL").unwrap_or_else(|_| "https://ic0.app".to_string());
    // let pem_content = env::var("PEM_PATH").unwrap_or_else(|_| "private.pem".to_string());
    let redis_url = env::var("REDIS_URL")
        .or_else(|_| env::var("TEST_REDIS_URL"))
        .unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let batch_size: u64 = env::var("BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let csv_output =
        env::var("CSV_OUTPUT").unwrap_or_else(|_| "video_backfill_output.csv".to_string());
    let max_posts_limit: Option<u64> = env::var("MAX_POSTS_LIMIT")
        .ok()
        .and_then(|s| s.parse().ok());
    let debug_mode = env::var("DEBUG")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    let user_post_service_canister = Principal::from_text("gxhc3-pqaaa-aaaas-qbh3q-cai")
        .context("Invalid USER_POST_SERVICE_CANISTER_ID")?;

    println!("📋 Configuration:");
    println!("  IC URL: {}", ic_url);
    // println!("  PEM Path: {}", pem_path);
    println!("  Redis URL: {}", redis_url);
    println!("  Batch Size: {}", batch_size);
    println!("  Max Posts Limit: {:?}", max_posts_limit);
    println!("  Debug Mode: {}", debug_mode);
    println!("  CSV Output: {}", csv_output);
    println!("  Post Service Canister: {}\n", user_post_service_canister);

    // Setup IC Agent
    println!("🔌 Connecting to IC...");
    // // let pem_content =
    // let identity = ic_agent::identity::Secp256k1Identity::from_pem(pem_content.as_bytes())
    //     .context("Failed to parse PEM identity")?;

    let agent = Agent::builder()
        .with_url(&ic_url)
        .build()
        .context("Failed to build IC agent")?;

    agent
        .fetch_root_key()
        .await
        .context("Failed to fetch root key")?;
    println!("✅ Connected to IC\n");

    // Setup Redis
    println!("🔌 Connecting to Redis...");
    let manager = RedisConnectionManager::new(redis_url.clone())
        .context("Failed to create Redis connection manager")?;
    let pool = bb8::Pool::builder()
        .build(manager)
        .await
        .context("Failed to build Redis pool")?;

    let mut conn = pool.get().await.context("Failed to get Redis connection")?;
    println!("✅ Connected to Redis\n");

    // Load Lua script
    println!("📜 Loading Lua MAX script...");
    let script_sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg(LUA_MAX_SCRIPT)
        .query_async(&mut *conn)
        .await
        .context("Failed to load Lua script")?;

    let expected_sha = calculate_script_sha(LUA_MAX_SCRIPT);
    if script_sha != expected_sha {
        println!(
            "⚠️  Warning: Script SHA mismatch (expected: {}, got: {})",
            expected_sha, script_sha
        );
    }
    println!("✅ Lua script loaded: {}\n", script_sha);

    // Create CSV file with header
    let mut csv_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&csv_output)
        .context("Failed to create CSV file")?;
    writeln!(
        csv_file,
        "post_id,video_uid,total_view_count,creator_principal,post_json"
    )
    .context("Failed to write CSV header")?;
    println!("📄 CSV file created: {}\n", csv_output);

    // Create UserPostService client
    let post_service = UserPostService(user_post_service_canister, &agent);

    // Start backfill
    println!("🔄 Starting backfill...\n");
    let start_time = Instant::now();

    let mut last_uuid_processed: Option<String> = None;
    let mut total_posts = 0u64;
    let mut total_batches = 0u64;
    let mut total_updates = 0u64;

    loop {
        // Calculate how many posts to fetch in this batch
        let posts_to_fetch = if let Some(limit) = max_posts_limit {
            let remaining = limit.saturating_sub(total_posts);
            if remaining == 0 {
                println!("✅ Reached max posts limit: {}", limit);
                break;
            }
            std::cmp::min(batch_size, remaining)
        } else {
            batch_size
        };

        total_batches += 1;
        println!(
            "📦 Fetching batch #{} (cursor: {:?}, requesting {} posts)...",
            total_batches, last_uuid_processed, posts_to_fetch
        );

        // Fetch posts from canister with dynamic limit
        let result = post_service
            .fetch_posts(FetchPostsArgs {
                limit: posts_to_fetch,
                last_uuid_processed: last_uuid_processed.clone(),
            })
            .await
            .context("Failed to fetch posts from canister")?;

        let batch_size_actual = result.posts.len();
        total_posts += batch_size_actual as u64;

        if batch_size_actual == 0 {
            println!("✅ No more posts to process");
            break;
        }

        println!("  📊 Fetched {} posts", batch_size_actual);

        // Bulk update Redis using pipeline AND write to CSV
        let mut pipe = redis::pipe();
        let mut video_updates = 0;
        let mut csv_batch = String::new();

        // Debug: Get before values if in debug mode
        let before_values: std::collections::HashMap<String, u64> = if debug_mode {
            let mut conn = pool
                .get()
                .await
                .context("Failed to get Redis connection for debug")?;
            let mut before_map = std::collections::HashMap::new();
            for post in &result.posts {
                if post.view_stats.total_view_count > 0 {
                    let key = format!("impressions:test:rewards:video:{}", post.video_uid);
                    let current: Option<String> =
                        conn.hget(&key, "total_count_all").await.ok().flatten();
                    let current_val = current.and_then(|s| s.parse().ok()).unwrap_or(0);
                    before_map.insert(post.video_uid.clone(), current_val);
                }
            }
            before_map
        } else {
            std::collections::HashMap::new()
        };

        for post in &result.posts {
            let video_uid = &post.video_uid;
            let total_view_count = post.view_stats.total_view_count;
            let post_id = &post.id;
            let creator = post.creator_principal.to_text();

            // Manually create JSON string for bookkeeping (Post doesn't implement Serialize)
            let hashtags_json = post
                .hashtags
                .iter()
                .map(|h| format!("\"{}\"", h.replace("\"", "\\\"")))
                .collect::<Vec<_>>()
                .join(",");
            let status_str = format!("{:?}", post.status);
            let description_escaped = post.description.replace("\"", "\\\"").replace("\n", "\\n");

            let post_json = format!(
                "{{\"id\":\"{}\",\"video_uid\":\"{}\",\"status\":\"{}\",\"creator\":\"{}\",\"hashtags\":[{}],\"description\":\"{}\",\"likes_count\":{},\"share_count\":{},\"view_stats\":{{\"total_view_count\":{},\"threshold_view_count\":{},\"average_watch_percentage\":{}}},\"created_at\":{{\"secs\":{},\"nanos\":{}}}}}",
                post_id,
                video_uid,
                status_str,
                creator,
                hashtags_json,
                description_escaped,
                post.likes.len(),
                post.share_count,
                post.view_stats.total_view_count,
                post.view_stats.threshold_view_count,
                post.view_stats.average_watch_percentage,
                post.created_at.secs_since_epoch,
                post.created_at.nanos_since_epoch
            ).replace("\"", "\"\""); // Escape all quotes for CSV

            // Append to CSV batch (write all posts, even with 0 views)
            csv_batch.push_str(&format!(
                "{},{},{},{},\"{}\"\n",
                post_id, video_uid, total_view_count, creator, post_json
            ));

            if total_view_count == 0 {
                continue; // Skip Redis update for 0 views
            }

            let key = format!("impressions:test:rewards:video:{}", video_uid);
            pipe.cmd("EVALSHA")
                .arg(&script_sha)
                .arg(1) // number of keys
                .arg(&key)
                .arg(total_view_count);

            video_updates += 1;
        }

        // Write CSV batch to file
        csv_file
            .write_all(csv_batch.as_bytes())
            .context("Failed to write CSV batch")?;

        // Execute Redis pipeline
        if video_updates > 0 {
            let mut conn = pool.get().await.context("Failed to get Redis connection")?;
            let _results: Vec<i32> = pipe
                .query_async(&mut *conn)
                .await
                .context("Failed to execute Redis pipeline")?;
            total_updates += video_updates;

            // Debug: Show before/after values
            if debug_mode {
                println!("\n  🐛 DEBUG: Before/After Redis Values:");
                for post in &result.posts {
                    if post.view_stats.total_view_count > 0 {
                        let before = before_values.get(&post.video_uid).copied().unwrap_or(0);
                        let key = format!("impressions:test:rewards:video:{}", post.video_uid);
                        let after: Option<String> =
                            conn.hget(&key, "total_count_all").await.ok().flatten();
                        let after_val = after.and_then(|s| s.parse().ok()).unwrap_or(0);
                        let canister_val = post.view_stats.total_view_count;

                        println!(
                            "    video_id={} | before={} | canister={} | after={} | action={}",
                            post.video_uid,
                            before,
                            canister_val,
                            after_val,
                            if after_val > before {
                                "UPDATED"
                            } else {
                                "NO_CHANGE"
                            }
                        );
                    }
                }
                println!();
            }
        }

        println!(
            "  ✅ Updated {} video counts, wrote {} rows to CSV",
            video_updates, batch_size_actual
        );

        // Update cursor
        last_uuid_processed = result.last_post_id_fetched.clone();

        // Check if we're done
        if result.last_post_id_fetched.is_none() {
            println!("✅ Reached end of posts");
            break;
        }

        println!();
    }

    // Summary
    let elapsed = start_time.elapsed();
    let posts_per_sec = if elapsed.as_secs() > 0 {
        total_posts as f64 / elapsed.as_secs() as f64
    } else {
        total_posts as f64
    };

    // Flush and close CSV file
    csv_file.flush().context("Failed to flush CSV file")?;
    drop(csv_file);

    println!("\n🎉 Backfill Complete!");
    println!("==================");
    println!("  Total Posts Processed: {}", total_posts);
    println!("  Total Redis Updates: {}", total_updates);
    println!("  Total Batches: {}", total_batches);
    println!("  Time Taken: {:.2}s", elapsed.as_secs_f64());
    println!("  Throughput: {:.2} posts/sec", posts_per_sec);
    println!("  CSV Output: {}", csv_output);
    println!();

    Ok(())
}
