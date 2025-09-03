use candid::Principal;
use std::collections::HashMap;
use yral_metadata_client::MetadataClient;
use yral_username_gen::random_username_from_principal;

use super::redis_ops::LeaderboardRedis;

/// Retrieves usernames for a list of principals using a three-tier fallback strategy:
/// 1. Check Redis cache for cached usernames
/// 2. Fetch from metadata API for uncached principals
/// 3. Generate deterministic usernames for any remaining principals
///
/// All newly fetched or generated usernames are cached in Redis for future use.
pub async fn get_usernames_with_fallback<const AUTH: bool>(
    redis: &LeaderboardRedis,
    metadata_client: &MetadataClient<AUTH>,
    principals: Vec<Principal>,
) -> HashMap<Principal, String> {
    let mut final_usernames = HashMap::new();

    if principals.is_empty() {
        return final_usernames;
    }

    // Step 1: Check Redis cache for all principals
    let cached_usernames = match redis.get_cached_usernames_bulk(&principals).await {
        Ok(cache_map) => {
            log::info!(
                "Found {} cached usernames out of {}",
                cache_map.len(),
                principals.len()
            );
            cache_map
        }
        Err(e) => {
            log::warn!("Failed to get cached usernames: {:?}", e);
            HashMap::new()
        }
    };

    // Add cached usernames to final map and identify missing principals
    let mut missing_principals = Vec::new();
    for principal in &principals {
        if let Some(username) = cached_usernames.get(principal) {
            final_usernames.insert(*principal, username.clone());
        } else {
            missing_principals.push(*principal);
        }
    }

    if missing_principals.is_empty() {
        return final_usernames;
    }

    // Step 2: Fetch metadata for missing principals
    let metadata_map = match metadata_client
        .get_user_metadata_bulk(missing_principals.clone())
        .await
    {
        Ok(map) => {
            log::info!("Fetched metadata for {} principals", map.len());
            map
        }
        Err(e) => {
            log::warn!("Failed to fetch bulk metadata: {:?}", e);
            HashMap::new()
        }
    };

    // Process metadata results and identify principals still without usernames
    let mut still_missing = Vec::new();
    for principal in missing_principals {
        if let Some(Some(metadata)) = metadata_map.get(&principal) {
            if !metadata.user_name.trim().is_empty() {
                let username = metadata.user_name.clone();
                final_usernames.insert(principal, username.clone());

                // Cache the fetched username
                if let Err(e) = redis.cache_username(principal, &username, 3600).await {
                    log::warn!("Failed to cache username for {}: {:?}", principal, e);
                }
                continue;
            }
        }
        still_missing.push(principal);
    }

    // Step 3: Generate usernames for any remaining principals
    for principal in still_missing {
        let generated_username = random_username_from_principal(principal, 15);
        log::info!(
            "Generated username for principal {}: {}",
            principal,
            generated_username
        );

        final_usernames.insert(principal, generated_username.clone());

        // Cache the generated username
        if let Err(e) = redis
            .cache_username(principal, &generated_username, 3600)
            .await
        {
            log::warn!(
                "Failed to cache generated username for {}: {:?}",
                principal,
                e
            );
        }
    }

    final_usernames
}
