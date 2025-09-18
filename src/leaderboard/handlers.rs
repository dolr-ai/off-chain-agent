use axum::{
    extract::{Json, Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
};
use candid::Principal;
use chrono::Utc;
use std::sync::Arc;
use yral_username_gen::random_username_from_principal;

use super::redis_ops::LeaderboardRedis;
use super::types::*;
use super::utils::get_usernames_with_fallback;
use crate::{app_state::AppState, auth::check_auth_events};
use chrono::{DateTime, TimeZone};
use chrono_tz::Tz;
use serde::Deserialize;

// Conversion rate: 1 USD = 886 SATS (ckBTC satoshis)
const USD_TO_CKBTC_SATS_RATE: f64 = 886.0;

// Timezone API response structure
#[derive(Debug, Deserialize)]
struct TimezoneApiResponse {
    timezone: Option<String>,
    // Add other fields if needed
}

// Helper function to extract client IP from headers
fn extract_client_ip(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.split(',').next()) // take first if multiple
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string()) // Default fallback for local/unknown
}

// Helper function to get timezone from IP using the API
async fn get_timezone_from_ip(ip: &str) -> Option<(String, Tz)> {
    // Get the bearer token from environment or config
    let token = std::env::var("TIMEZONE_API_TOKEN").ok()?;

    let url = format!(
        "https://marketing-analytics-server.fly.dev/api/ip_v2/{}",
        ip
    );

    let client = reqwest::Client::new();
    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", token))
        .header("Content-Type", "application/json")
        .send()
        .await
        .ok()?;

    if !response.status().is_success() {
        log::warn!(
            "Timezone API returned non-success status for IP {}: {}",
            ip,
            response.status()
        );
        return None;
    }

    let data: TimezoneApiResponse = response.json().await.ok()?;
    let timezone_str = data.timezone?;

    // Parse the timezone string to Tz
    let tz: Tz = timezone_str.parse().ok()?;

    Some((timezone_str, tz))
}

// Helper function to convert Unix timestamp to ISO 8601 string in given timezone
fn convert_timestamp_to_timezone(timestamp: i64, tz: &Tz) -> String {
    let utc_dt = DateTime::from_timestamp(timestamp, 0).unwrap_or_else(Utc::now);
    let local_dt = tz.from_utc_datetime(&utc_dt.naive_utc());
    local_dt.to_rfc3339()
}

// Internal API: Update user score on balance change (requires authentication)
#[utoipa::path(
    post,
    path = "/score/update",
    tag = "leaderboard",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Score updated successfully"),
        (status = 401, description = "Authentication failed"),
        (status = 404, description = "No active tournament"),
        (status = 400, description = "Invalid request")
    ),
    security(
        ("bearer" = [])
    )
)]
pub async fn update_score_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<UpdateScoreRequest>,
) -> impl IntoResponse {
    // Extract and validate auth token
    let auth_token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim_start_matches("Bearer ").to_string());

    if let Err(e) = check_auth_events(auth_token) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": format!("Authentication failed: {}", e)
            })),
        )
            .into_response();
    }

    let redis = LeaderboardRedis::new(state.leaderboard_redis_pool.clone());

    // Get current tournament
    let current_tournament = match redis.get_current_tournament().await {
        Ok(Some(id)) => id,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "No active tournament"
                })),
            )
                .into_response();
        }
        Err(e) => {
            log::error!("Failed to get current tournament: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to get current tournament"
                })),
            )
                .into_response();
        }
    };

    // Verify tournament is still active
    let tournament = match redis.get_tournament_info(&current_tournament).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "Tournament info not found"
                })),
            )
                .into_response();
        }
        Err(e) => {
            log::error!("Failed to get tournament info: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to get tournament info"
                })),
            )
                .into_response();
        }
    };

    // Check if tournament is active
    let now = Utc::now().timestamp();
    if tournament.status != TournamentStatus::Active
        || now < tournament.start_time
        || now > tournament.end_time
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "Tournament is not active"
            })),
        )
            .into_response();
    }

    // Validate metric type matches tournament
    if request.metric_type != tournament.metric_type.to_string() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!("Invalid metric type. Expected: {}, Got: {}",
                    tournament.metric_type, request.metric_type)
            })),
        )
            .into_response();
    }

    // Validate source is allowed
    if !tournament.allowed_sources.contains(&request.source) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!("Source '{}' not allowed for this tournament", request.source)
            })),
        )
            .into_response();
    }

    // Determine operation based on metric type
    let operation = match tournament.metric_type {
        MetricType::GamesPlayed
        | MetricType::GamesWon
        | MetricType::TokensEarned
        | MetricType::VideosWatched
        | MetricType::ReferralsMade => ScoreOperation::Increment,
        MetricType::Custom(_) => ScoreOperation::Increment, // Default to increment for custom
    };

    // Update score
    let new_score = match redis
        .update_user_score(
            &current_tournament,
            request.principal_id,
            request.metric_value,
            &operation,
        )
        .await
    {
        Ok(score) => score,
        Err(e) => {
            log::error!("Failed to update score: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to update score"
                })),
            )
                .into_response();
        }
    };

    // Fetch username from metadata service (async, don't block)
    let principal = request.principal_id;
    let metadata_client = state.yral_metadata_client.clone();
    let redis_clone = LeaderboardRedis::new(state.leaderboard_redis_pool.clone());

    tokio::spawn(async move {
        // Use the utility function to get username with fallback
        let username_map =
            get_usernames_with_fallback(&redis_clone, &metadata_client, vec![principal]).await;

        // Get the username (guaranteed to exist)
        let username = username_map.get(&principal).cloned().unwrap_or_else(|| {
            log::error!("Missing username for principal {} in map", principal);
            random_username_from_principal(principal, 15)
        });

        if let Err(e) = redis.cache_username(principal, &username).await {
            log::warn!("Failed to cache generated username: {:?}", e);
        }
    });

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "success": true,
            "tournament_id": current_tournament,
            "principal_id": request.principal_id.to_string(),
            "new_score": new_score,
            "metric_type": tournament.metric_type.to_string()
        })),
    )
        .into_response()
}

// Get current leaderboard with pagination
#[utoipa::path(
    get,
    path = "/current",
    tag = "leaderboard",
    responses(
        (status = 200, description = "Leaderboard data retrieved", body = LeaderboardWithTournamentResponse),
        (status = 404, description = "No active tournament")
    )
)]
pub async fn get_leaderboard_handler(
    headers: HeaderMap,
    Query(params): Query<LeaderboardQueryParams>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let redis = LeaderboardRedis::new(state.leaderboard_redis_pool.clone());

    let start = params.get_start();
    let limit = params.get_limit();
    let sort_order = params.get_sort_order();

    // Determine which tournament to use
    let tournament_id = if let Some(ref id) = params.tournament_id {
        // Use specified tournament for historical data
        id.clone()
    } else {
        // Get current tournament
        match redis.get_current_tournament().await {
            Ok(Some(id)) => id,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({
                        "error": "No active tournament"
                    })),
                )
                    .into_response();
            }
            Err(e) => {
                log::error!("Failed to get current tournament: {:?}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": "Failed to get current tournament"
                    })),
                )
                    .into_response();
            }
        }
    };

    // Get tournament info
    let tournament = match redis.get_tournament_info(&tournament_id).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "Tournament info not found"
                })),
            )
                .into_response();
        }
        Err(e) => {
            log::error!("Failed to get tournament info: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to get tournament info"
                })),
            )
                .into_response();
        }
    };

    // Check if tournament has saved results (was finalized) when tournament is completed
    let saved_results = if tournament.status == TournamentStatus::Completed {
        match redis.get_tournament_results(&tournament_id).await {
            Ok(results) => results,
            Err(e) => {
                log::warn!("Failed to get saved tournament results: {:?}", e);
                None
            }
        }
    } else {
        None
    };

    // If tournament was finalized, create rewards map from saved results
    let rewards_map: std::collections::HashMap<Principal, u64> =
        if let Some(ref results) = saved_results {
            results
                .user_results
                .iter()
                .filter_map(|entry| entry.reward.map(|r| (entry.principal_id, r)))
                .collect()
        } else {
            std::collections::HashMap::new()
        };

    // Get total participants first (needed for rank calculation in ascending order)
    let total_participants = redis
        .get_total_participants(&tournament_id)
        .await
        .unwrap_or(0);

    // Get paginated players
    let leaderboard_data = match redis
        .get_leaderboard(
            &tournament_id,
            start as isize,
            (start + limit - 1) as isize,
            sort_order.clone(),
        )
        .await
    {
        Ok(data) => data,
        Err(e) => {
            log::error!("Failed to get leaderboard: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to get leaderboard"
                })),
            )
                .into_response();
        }
    };

    // Collect principals for bulk username fetch
    let principals: Vec<Principal> = leaderboard_data
        .iter()
        .filter_map(|(principal_str, _)| Principal::from_text(principal_str).ok())
        .collect();

    // Get usernames using the three-tier fallback strategy
    let username_map =
        get_usernames_with_fallback(&redis, &state.yral_metadata_client, principals.clone()).await;

    // Build leaderboard entries
    let entries: Vec<LeaderboardEntry> = leaderboard_data
        .iter()
        .enumerate()
        .filter_map(|(index, (principal_str, score))| {
            if let Ok(principal) = Principal::from_text(principal_str) {
                // Calculate rank based on sort order
                let rank = match sort_order {
                    SortOrder::Desc => start + index as u32 + 1, // Normal: 1, 2, 3...
                    SortOrder::Asc => {
                        // Reversed: N, N-1, N-2...
                        if total_participants > 0 {
                            total_participants - start - index as u32
                        } else {
                            1 // Fallback if no participants
                        }
                    }
                };
                // Username is guaranteed to exist for every principal
                let username = username_map.get(&principal).cloned().unwrap_or_else(|| {
                    // This should never happen since get_usernames_with_fallback
                    // always returns a username for every principal
                    log::error!("Missing username for principal {} in map", principal);
                    random_username_from_principal(principal, 15)
                });

                // For rewards, check if we have saved results from a finalized tournament
                let reward = if tournament.status == TournamentStatus::Completed {
                    // Use saved reward if exists (for winners), None for others
                    let saved_reward = rewards_map.get(&principal).copied();
                    // Convert CKBTC rewards from sats to USD for display (only for saved rewards)
                    if tournament.prize_token == TokenType::CKBTC {
                        saved_reward.map(|r| (r as f64 / USD_TO_CKBTC_SATS_RATE) as u64)
                    } else {
                        saved_reward
                    }
                } else {
                    // Tournament still active - calculate potential reward
                    // Always use the "real" rank (1 = top prize)
                    let reward_rank = match sort_order {
                        SortOrder::Desc => rank,                         // Same as display rank
                        SortOrder::Asc => total_participants - rank + 1, // Convert back to real rank
                    };
                    calculate_reward(reward_rank, tournament.prize_pool as u64)
                };

                Some(LeaderboardEntry {
                    principal_id: principal,
                    username,
                    score: *score,
                    rank, // Display rank (reversed for ascending)
                    reward,
                })
            } else {
                None
            }
        })
        .collect();

    // Total participants already fetched above

    // Calculate cursor info
    let has_more = (start + limit) < total_participants;
    let next_cursor = if has_more { Some(start + limit) } else { None };

    let cursor_info = CursorInfo {
        start,
        limit,
        total_count: total_participants,
        next_cursor,
        has_more,
    };

    // Fetch user info if user_id is provided
    let user_info = if let Some(ref user_id) = params.user_id {
        // Parse principal ID
        if let Ok(user_principal) = Principal::from_text(user_id) {
            // Get user's rank
            let user_rank = match redis.get_user_rank(&tournament_id, user_principal).await {
                Ok(Some(rank)) => rank,
                _ => 0,
            };

            // Get user's score
            let user_score = match redis.get_user_score(&tournament_id, user_principal).await {
                Ok(Some(score)) => score,
                _ => 0.0,
            };

            // Fetch username
            let username = match state
                .yral_metadata_client
                .get_user_metadata_v2(user_principal.to_string())
                .await
            {
                Ok(Some(metadata)) if !metadata.user_name.trim().is_empty() => {
                    let name = metadata.user_name;
                    // Cache the username from metadata
                    if let Err(e) = redis.cache_username(user_principal, &name).await {
                        log::warn!("Failed to cache username from metadata: {:?}", e);
                    }
                    name
                }
                _ => {
                    // Generate deterministic username from principal for empty/missing usernames
                    let generated = random_username_from_principal(user_principal, 15);
                    // Cache the generated username
                    if let Err(e) = redis.cache_username(user_principal, &generated).await {
                        log::warn!("Failed to cache generated username: {:?}", e);
                    }
                    generated
                }
            };

            if user_rank > 0 {
                // User is in the tournament
                // Determine reward based on tournament status
                let user_reward = if tournament.status == TournamentStatus::Completed {
                    // Use saved reward if exists
                    let saved_reward = rewards_map.get(&user_principal).copied();
                    // Convert CKBTC rewards from sats to USD for display (only for saved rewards)
                    if tournament.prize_token == TokenType::CKBTC {
                        saved_reward.map(|r| (r as f64 / USD_TO_CKBTC_SATS_RATE) as u64)
                    } else {
                        saved_reward
                    }
                } else {
                    // Tournament still active - calculate potential reward
                    calculate_reward(user_rank, tournament.prize_pool as u64)
                };

                Some(serde_json::json!({
                    "principal_id": user_principal.to_string(),
                    "username": username,
                    "rank": user_rank,
                    "score": user_score,
                    "percentile": ((total_participants - user_rank + 1) as f32 / total_participants as f32 * 100.0),
                    "reward": user_reward,
                }))
            } else {
                // User is not in the tournament - return with rank = total_participants + 1, score = 0
                Some(serde_json::json!({
                    "principal_id": user_principal.to_string(),
                    "username": username,
                    "rank": total_participants + 1,
                    "score": 0.0,
                    "percentile": 0.0,
                    "reward": null,
                }))
            }
        } else {
            None
        }
    } else {
        None
    };

    // Get client IP and timezone
    let client_ip = extract_client_ip(&headers);
    log::debug!("Client IP: {}", client_ip);

    // Get timezone info from IP
    let timezone_info = get_timezone_from_ip(&client_ip).await;

    // Build tournament info for response with timezone-adjusted times
    let tournament_info = if let Some((ref timezone_str, ref tz)) = timezone_info {
        TournamentInfo {
            id: tournament.id.clone(),
            start_time: tournament.start_time,
            end_time: tournament.end_time,
            status: tournament.status,
            prize_pool: tournament.prize_pool,
            prize_token: tournament.prize_token,
            metric_type: tournament.metric_type,
            metric_display_name: tournament.metric_display_name,
            client_timezone: Some(timezone_str.clone()),
            client_start_time: Some(convert_timestamp_to_timezone(tournament.start_time, tz)),
            client_end_time: Some(convert_timestamp_to_timezone(tournament.end_time, tz)),
            num_winners: tournament.num_winners,
        }
    } else {
        // Fallback when timezone cannot be determined
        TournamentInfo {
            id: tournament.id.clone(),
            start_time: tournament.start_time,
            end_time: tournament.end_time,
            status: tournament.status,
            prize_pool: tournament.prize_pool,
            prize_token: tournament.prize_token,
            metric_type: tournament.metric_type,
            metric_display_name: tournament.metric_display_name,
            client_timezone: None,
            client_start_time: None,
            client_end_time: None,
            num_winners: tournament.num_winners,
        }
    };

    // Fetch upcoming tournament info if available
    let upcoming_tournament_info =
        if let Ok(Some(upcoming_id)) = redis.get_upcoming_tournament().await {
            if let Ok(Some(upcoming_tournament)) = redis.get_tournament_info(&upcoming_id).await {
                // Build tournament info with timezone-adjusted times
                let upcoming_info = if let Some((ref timezone_str, ref tz)) = timezone_info {
                    TournamentInfo {
                        id: upcoming_tournament.id.clone(),
                        start_time: upcoming_tournament.start_time,
                        end_time: upcoming_tournament.end_time,
                        status: upcoming_tournament.status,
                        prize_pool: upcoming_tournament.prize_pool,
                        prize_token: upcoming_tournament.prize_token,
                        metric_type: upcoming_tournament.metric_type,
                        metric_display_name: upcoming_tournament.metric_display_name,
                        client_timezone: Some(timezone_str.clone()),
                        client_start_time: Some(convert_timestamp_to_timezone(
                            upcoming_tournament.start_time,
                            tz,
                        )),
                        client_end_time: Some(convert_timestamp_to_timezone(
                            upcoming_tournament.end_time,
                            tz,
                        )),
                        num_winners: upcoming_tournament.num_winners,
                    }
                } else {
                    TournamentInfo {
                        id: upcoming_tournament.id.clone(),
                        start_time: upcoming_tournament.start_time,
                        end_time: upcoming_tournament.end_time,
                        status: upcoming_tournament.status,
                        prize_pool: upcoming_tournament.prize_pool,
                        prize_token: upcoming_tournament.prize_token,
                        metric_type: upcoming_tournament.metric_type,
                        metric_display_name: upcoming_tournament.metric_display_name,
                        client_timezone: None,
                        client_start_time: None,
                        client_end_time: None,
                        num_winners: upcoming_tournament.num_winners,
                    }
                };
                Some(upcoming_info)
            } else {
                None
            }
        } else {
            None
        };

    // Fetch last tournament info if user_id is provided
    let last_tournament_info = if let Some(ref user_id) = params.user_id {
        if let Ok(user_principal) = Principal::from_text(user_id) {
            // Get user's last tournament info
            match redis.get_user_last_tournament(user_principal).await {
                Ok(Some(info)) => {
                    // Convert to response format
                    let response_info = UserLastTournamentResponse {
                        tournament_id: info.tournament_id.clone(),
                        rank: info.rank,
                        reward: info.reward,
                        status: info.status.clone(),
                    };

                    // Mark as seen if this is the current tournament endpoint (no tournament_id param)
                    if params.tournament_id.is_none() && info.status == "unseen" {
                        if let Err(e) = redis.mark_last_tournament_seen(user_principal).await {
                            log::error!("Failed to mark last tournament as seen: {:?}", e);
                        }
                    }

                    Some(response_info)
                }
                Ok(None) => None,
                Err(e) => {
                    log::error!("Failed to get user last tournament info: {:?}", e);
                    None
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    // Build response with tournament info and optional user info
    let response = LeaderboardWithTournamentResponse {
        data: entries,
        cursor_info,
        tournament_info,
        user_info,
        upcoming_tournament_info,
        last_tournament_info,
    };

    (StatusCode::OK, Json(response)).into_response()
}

// Get user's current rank
#[utoipa::path(
    get,
    path = "/rank/{user_id}",
    tag = "leaderboard",
    responses(
        (status = 200, description = "User rank data retrieved"),
        (status = 404, description = "User not found in leaderboard")
    )
)]
pub async fn get_user_rank_handler(
    Path(user_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let redis = LeaderboardRedis::new(state.leaderboard_redis_pool.clone());

    // Parse principal ID
    let principal = match Principal::from_text(&user_id) {
        Ok(p) => p,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Invalid principal ID"
                })),
            )
                .into_response();
        }
    };

    // Get current tournament
    let current_tournament = match redis.get_current_tournament().await {
        Ok(Some(id)) => id,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "No active tournament"
                })),
            )
                .into_response();
        }
        Err(e) => {
            log::error!("Failed to get current tournament: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to get current tournament"
                })),
            )
                .into_response();
        }
    };

    // Get total participants first
    let total_participants = redis
        .get_total_participants(&current_tournament)
        .await
        .unwrap_or(0);

    // Get user's rank and determine if they're in the leaderboard
    let (user_rank, is_in_leaderboard) =
        match redis.get_user_rank(&current_tournament, principal).await {
            Ok(Some(rank)) => (rank, true),
            Ok(None) => {
                // User not in leaderboard - assign last rank
                (total_participants + 1, false)
            }
            Err(e) => {
                log::error!("Failed to get user rank: {:?}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": "Failed to get user rank"
                    })),
                )
                    .into_response();
            }
        };

    // Get user's score
    let user_score = if is_in_leaderboard {
        match redis.get_user_score(&current_tournament, principal).await {
            Ok(Some(score)) => score,
            Ok(None) => 0.0,
            Err(_) => 0.0,
        }
    } else {
        0.0 // No score for users not in leaderboard
    };

    // Get tournament info for prize calculation
    let tournament = match redis.get_tournament_info(&current_tournament).await {
        Ok(Some(t)) => t,
        _ => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to get tournament info"
                })),
            )
                .into_response();
        }
    };

    // Fetch username
    let username = match state
        .yral_metadata_client
        .get_user_metadata_v2(principal.to_string())
        .await
    {
        Ok(Some(metadata)) if !metadata.user_name.trim().is_empty() => {
            let name = metadata.user_name;
            // Cache the username from metadata
            if let Err(e) = redis.cache_username(principal, &name).await {
                log::warn!("Failed to cache username from metadata: {:?}", e);
            }
            name
        }
        _ => {
            // Generate deterministic username from principal for empty/missing usernames
            let generated = random_username_from_principal(principal, 15);
            // Cache the generated username
            if let Err(e) = redis.cache_username(principal, &generated).await {
                log::warn!("Failed to cache generated username: {:?}", e);
            }
            generated
        }
    };

    // Get surrounding players only if user is in leaderboard
    let surrounding_entries: Vec<LeaderboardEntry> = if is_in_leaderboard {
        // Get surrounding players (2 above, 2 below)
        let context_start = (user_rank as i32 - 3).max(0) as u32;
        let context_end = context_start + 4; // 5 total including user

        let surrounding_data = redis
            .get_leaderboard(
                &current_tournament,
                context_start as isize,
                context_end as isize,
                SortOrder::Desc, // Always desc for rank context
            )
            .await
            .unwrap_or_default();

        // Build surrounding entries
        surrounding_data
            .iter()
            .enumerate()
            .filter_map(|(index, (principal_str, score))| {
                if let Ok(p) = Principal::from_text(principal_str) {
                    let rank = context_start + index as u32 + 1;
                    // For active tournaments, calculate_reward already returns the correct value
                    let reward = calculate_reward(rank, tournament.prize_pool as u64);

                    Some(LeaderboardEntry {
                        principal_id: p,
                        username: if p == principal {
                            username.clone()
                        } else {
                            "Anonymous".to_string() // We could batch fetch these if needed
                        },
                        score: *score,
                        rank,
                        reward,
                    })
                } else {
                    None
                }
            })
            .collect()
    } else {
        // Empty list for users not in leaderboard
        vec![]
    };

    // Build user info struct
    // For active tournaments, calculate_reward already returns the correct value
    let user_reward = if is_in_leaderboard {
        calculate_reward(user_rank, tournament.prize_pool as u64)
    } else {
        None
    };

    let user = UserRankInfo {
        principal_id: principal,
        username,
        rank: user_rank,
        score: user_score,
        percentile: if is_in_leaderboard && total_participants > 0 {
            (total_participants - user_rank + 1) as f32 / total_participants as f32 * 100.0
        } else {
            0.0
        },
        reward: user_reward,
    };

    // Build tournament info struct
    let tournament_info = TournamentRankInfo {
        id: tournament.id,
        metric_type: tournament.metric_type.to_string(),
        metric_display_name: tournament.metric_display_name,
        status: match tournament.status {
            TournamentStatus::Upcoming => "upcoming".to_string(),
            TournamentStatus::Active => "active".to_string(),
            TournamentStatus::Finalizing => "finalizing".to_string(),
            TournamentStatus::Completed => "completed".to_string(),
            TournamentStatus::Ended => "ended".to_string(),
            TournamentStatus::Cancelled => "cancelled".to_string(),
        },
    };

    // Build response struct
    let response = UserRankResponse {
        user,
        surrounding_players: surrounding_entries,
        tournament: tournament_info,
        total_participants,
    };

    (StatusCode::OK, Json(response)).into_response()
}

// Search users in leaderboard
#[utoipa::path(
    get,
    path = "/search",
    tag = "leaderboard",
    responses(
        (status = 200, description = "Search results retrieved"),
        (status = 404, description = "No active tournament")
    )
)]
pub async fn search_users_handler(
    Query(params): Query<SearchParams>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let redis = LeaderboardRedis::new(state.leaderboard_redis_pool.clone());

    let start = params.get_start();
    let limit = params.get_limit();
    let sort_order = params.get_sort_order();

    // Get current tournament
    let current_tournament = match redis.get_current_tournament().await {
        Ok(Some(id)) => id,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "No active tournament"
                })),
            )
                .into_response();
        }
        Err(e) => {
            log::error!("Failed to get current tournament: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to get current tournament"
                })),
            )
                .into_response();
        }
    };

    // Search users (this is a simplified version - could be optimized)
    let search_results = match redis
        .search_users(
            &current_tournament,
            &params.q,
            limit * 10,
            sort_order.clone(),
        )
        .await
    {
        Ok(results) => results,
        Err(e) => {
            log::error!("Failed to search users: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to search users"
                })),
            )
                .into_response();
        }
    };

    // Apply pagination to search results
    let paginated_results: Vec<_> = search_results
        .into_iter()
        .skip(start as usize)
        .take(limit as usize)
        .collect();

    // Fetch usernames for results using the fallback strategy
    let principals: Vec<Principal> = paginated_results.iter().map(|(p, _)| *p).collect();

    // Get usernames using the three-tier fallback strategy
    let username_map =
        get_usernames_with_fallback(&redis, &state.yral_metadata_client, principals.clone()).await;

    // Get tournament info
    let tournament = match redis.get_tournament_info(&current_tournament).await {
        Ok(Some(t)) => t,
        _ => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to get tournament info"
                })),
            )
                .into_response();
        }
    };

    // Get total participants for rank calculation in ascending order
    let _total_participants = redis
        .get_total_participants(&current_tournament)
        .await
        .unwrap_or(0);

    // Build search result entries with ranks
    let mut entries = Vec::new();
    for (principal, score) in &paginated_results {
        if let Some(rank) = redis
            .get_user_rank(&current_tournament, *principal)
            .await
            .ok()
            .flatten()
        {
            // Username is guaranteed to exist for every principal
            let username = username_map.get(principal).cloned().unwrap_or_else(|| {
                log::error!("Missing username for principal {} in map", principal);
                random_username_from_principal(*principal, 15)
            });

            // For active tournaments, calculate_reward already returns the correct value
            let reward = calculate_reward(rank, tournament.prize_pool as u64);

            entries.push(LeaderboardEntry {
                principal_id: *principal,
                username,
                score: *score,
                rank,
                reward,
            });
        }
    }

    let total_count = entries.len() as u32;
    let has_more = total_count == limit;
    let next_cursor = if has_more { Some(start + limit) } else { None };

    let cursor_info = CursorInfo {
        start,
        limit,
        total_count,
        next_cursor,
        has_more,
    };

    let response = CursorPaginatedResponse {
        data: entries,
        cursor_info,
    };

    (StatusCode::OK, Json(response)).into_response()
}

// Get tournament history
#[utoipa::path(
    get,
    path = "/history",
    tag = "leaderboard",
    responses(
        (status = 200, description = "Tournament history retrieved")
    )
)]
pub async fn get_tournament_history_handler(
    Query(params): Query<CursorPaginationParams>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let redis = LeaderboardRedis::new(state.leaderboard_redis_pool.clone());

    let start = params.get_start();
    let limit = params.get_limit();

    // Get tournament history IDs
    let history_ids = match redis.get_tournament_history((start + limit) as isize).await {
        Ok(ids) => ids,
        Err(e) => {
            log::error!("Failed to get tournament history: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to get tournament history"
                })),
            )
                .into_response();
        }
    };

    // Apply pagination
    let paginated_ids: Vec<_> = history_ids
        .into_iter()
        .skip(start as usize)
        .take(limit as usize)
        .collect();

    // Build tournament summaries
    let mut summaries = Vec::new();
    for tournament_id in &paginated_ids {
        if let Ok(Some(tournament)) = redis.get_tournament_info(tournament_id).await {
            // Get winner (rank 1)
            let winner_info = if let Ok(top_players) = redis
                .get_leaderboard(tournament_id, 0, 0, SortOrder::Desc)
                .await
            {
                if let Some((principal_str, score)) = top_players.first() {
                    if let Ok(principal) = Principal::from_text(principal_str) {
                        // Fetch winner metadata
                        let username = match state
                            .yral_metadata_client
                            .get_user_metadata_v2(principal.to_string())
                            .await
                        {
                            Ok(Some(metadata)) if !metadata.user_name.trim().is_empty() => {
                                metadata.user_name
                            }
                            _ => {
                                // Generate deterministic username from principal for empty/missing usernames
                                random_username_from_principal(principal, 15)
                            }
                        };

                        // For historical tournaments, calculate_reward returns the correct value
                        let reward = calculate_reward(1, tournament.prize_pool as u64).unwrap_or(0);

                        Some(WinnerInfo {
                            principal_id: principal,
                            username,
                            score: *score,
                            reward,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            let total_participants = redis
                .get_total_participants(tournament_id)
                .await
                .unwrap_or(0);

            summaries.push(TournamentSummary {
                id: tournament.id.clone(),
                start_time: tournament.start_time,
                end_time: tournament.end_time,
                status: tournament.status,
                prize_pool: tournament.prize_pool,
                prize_token: tournament.prize_token,
                total_participants,
                winner: winner_info,
                num_winners: tournament.num_winners,
            });
        }
    }

    let total_count = summaries.len() as u32;
    let has_more = paginated_ids.len() == limit as usize;
    let next_cursor = if has_more { Some(start + limit) } else { None };

    let cursor_info = CursorInfo {
        start,
        limit,
        total_count,
        next_cursor,
        has_more,
    };

    let response = serde_json::json!({
        "tournaments": summaries,
        "cursor_info": cursor_info,
    });

    (StatusCode::OK, Json(response)).into_response()
}

// Admin: Create new tournament
pub async fn create_tournament_handler(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateTournamentRequest>,
) -> impl IntoResponse {
    let redis = LeaderboardRedis::new(state.leaderboard_redis_pool.clone());

    // Generate tournament ID
    let tournament_id = format!("tournament_{}", Utc::now().timestamp());
    let now = Utc::now().timestamp();

    // Determine initial status based on start time
    let status = if request.start_time <= now {
        // Tournament should start immediately
        TournamentStatus::Active
    } else {
        // Tournament starts in the future
        TournamentStatus::Upcoming
    };

    // Create tournament
    let tournament = Tournament {
        id: tournament_id.clone(),
        start_time: request.start_time,
        end_time: request.end_time,
        prize_pool: request.prize_pool,
        prize_token: request.prize_token,
        status: status.clone(),
        metric_type: request.metric_type,
        metric_display_name: request.metric_display_name,
        allowed_sources: request.allowed_sources,
        created_at: now,
        updated_at: now,
        num_winners: request.num_winners.unwrap_or(10),
    };

    // Store tournament info
    if let Err(e) = redis.set_tournament_info(&tournament).await {
        log::error!("Failed to store tournament info: {:?}", e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "Failed to create tournament"
            })),
        )
            .into_response();
    }

    // If tournament is active, set as current and schedule finalize
    if status == TournamentStatus::Active {
        if let Err(e) = redis.set_current_tournament(&tournament_id).await {
            log::error!("Failed to set current tournament: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to set current tournament"
                })),
            )
                .into_response();
        }

        // Send start notifications
        if let Err(e) = super::tournament::start_tournament(&tournament_id, &state).await {
            log::error!("Failed to send start notifications: {:?}", e);
        }

        // Schedule finalize for end_time
        let delay = tournament.end_time - now;
        if delay > 0 {
            if let Err(e) = state
                .qstash_client
                .schedule_tournament_finalize(&tournament_id, delay)
                .await
            {
                log::error!("Failed to schedule tournament finalize: {:?}", e);
            } else {
                log::info!(
                    "Tournament {} created and started immediately. Scheduled finalize for {} (in {} seconds)",
                    tournament_id,
                    tournament.end_time,
                    delay
                );
            }
        }
    } else {
        // Set as upcoming tournament
        if let Err(e) = redis.set_upcoming_tournament(&tournament_id).await {
            log::error!("Failed to set upcoming tournament: {:?}", e);
            // Continue anyway, this is not critical
        } else {
            log::info!("Tournament {} set as upcoming tournament", tournament_id);
        }

        // Schedule start for start_time
        let delay = tournament.start_time - now;
        if delay > 0 {
            if let Err(e) = state
                .qstash_client
                .schedule_tournament_start(&tournament_id, delay)
                .await
            {
                log::error!("Failed to schedule tournament start: {:?}", e);
            } else {
                log::info!(
                    "Tournament {} created with Upcoming status. Scheduled start for {} (in {} seconds)",
                    tournament_id,
                    tournament.start_time,
                    delay
                );
            }
        }
    }

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "success": true,
            "tournament": tournament,
            "status_message": if status == TournamentStatus::Active {
                "Tournament created and started immediately"
            } else {
                "Tournament created and scheduled to start"
            }
        })),
    )
        .into_response()
}

// Admin: Finalize tournament and distribute prizes
pub async fn finalize_tournament_handler(
    Path(tournament_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match super::tournament::finalize_tournament(&tournament_id, &state).await {
        Ok(_) => {
            // Schedule the creation of the next tournament 10 minutes from now
            let redis = LeaderboardRedis::new(state.leaderboard_redis_pool.clone());

            // Get the finalized tournament info to use as template for next tournament
            if let Ok(Some(tournament)) = redis.get_tournament_info(&tournament_id).await {
                // Calculate next tournament times using same duration as previous
                let now = Utc::now().timestamp();
                let tournament_duration = tournament.end_time - tournament.start_time;
                let next_start_time = now + 600; // 10 minutes from now
                let next_end_time = next_start_time + tournament_duration; // Same duration as previous tournament

                // Prepare configuration for next tournament
                let _next_tournament_config = serde_json::json!({
                    "start_time": next_start_time,
                    "end_time": next_end_time,
                    "prize_pool": tournament.prize_pool,
                    "prize_token": tournament.prize_token,
                    "metric_type": tournament.metric_type,
                    "metric_display_name": tournament.metric_display_name,
                    "allowed_sources": tournament.allowed_sources,
                });

                // Schedule tournament creation via QStash (10 minutes delay)
                // if let Err(e) = state
                //     .qstash_client
                //     .schedule_tournament_create(next_tournament_config, 60)
                //     .await
                // {
                //     log::error!("Failed to schedule next tournament creation: {:?}", e);
                //     // Don't fail the finalization if scheduling fails
                // } else {
                //     log::info!(
                //         "Scheduled next tournament to be created in 10 minutes (at {})",
                //         next_start_time
                //     );
                // }
            }

            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "success": true,
                    "message": format!("Tournament {} finalized successfully", tournament_id),
                })),
            )
                .into_response()
        }
        Err(e) => {
            log::error!("Failed to finalize tournament {}: {:?}", tournament_id, e);
            let (status, message) = if e.to_string().contains("not found") {
                (StatusCode::NOT_FOUND, "Tournament not found")
            } else if e.to_string().contains("not active") {
                (StatusCode::BAD_REQUEST, "Tournament is not active")
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to finalize tournament",
                )
            };
            (
                status,
                Json(serde_json::json!({
                    "error": format!("{}: {}", message, e)
                })),
            )
                .into_response()
        }
    }
}

// Get tournament results
#[utoipa::path(
    get,
    path = "/tournament/{id}/results",
    tag = "leaderboard",
    responses(
        (status = 200, description = "Tournament results retrieved"),
        (status = 404, description = "Tournament not found")
    )
)]
pub async fn get_tournament_results_handler(
    Path(tournament_id): Path<String>,
    Query(params): Query<CursorPaginationParams>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let redis = LeaderboardRedis::new(state.leaderboard_redis_pool.clone());

    let start = params.get_start();
    let limit = params.get_limit();

    // Get tournament info
    let tournament = match redis.get_tournament_info(&tournament_id).await {
        Ok(Some(t)) => t,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "Tournament not found"
                })),
            )
                .into_response();
        }
        Err(e) => {
            log::error!("Failed to get tournament info: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to get tournament info"
                })),
            )
                .into_response();
        }
    };

    // Check if tournament has saved results (was finalized)
    let saved_results = match redis.get_tournament_results(&tournament_id).await {
        Ok(results) => results,
        Err(e) => {
            log::warn!("Failed to get saved tournament results: {:?}", e);
            None
        }
    };

    // Get leaderboard data
    let leaderboard_data = match redis
        .get_leaderboard(
            &tournament_id,
            start as isize,
            (start + limit - 1) as isize,
            SortOrder::Desc,
        )
        .await
    {
        Ok(data) => data,
        Err(e) => {
            log::error!("Failed to get tournament results: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to get tournament results"
                })),
            )
                .into_response();
        }
    };

    // If tournament was finalized, create rewards map from saved results
    let rewards_map: std::collections::HashMap<Principal, u64> =
        if let Some(ref results) = saved_results {
            results
                .user_results
                .iter()
                .filter_map(|entry| entry.reward.map(|r| (entry.principal_id, r)))
                .collect()
        } else {
            std::collections::HashMap::new()
        };

    // Collect principals for bulk username fetch
    let principals: Vec<Principal> = leaderboard_data
        .iter()
        .filter_map(|(principal_str, _)| Principal::from_text(principal_str).ok())
        .collect();

    // Get usernames using the three-tier fallback strategy
    let username_map =
        get_usernames_with_fallback(&redis, &state.yral_metadata_client, principals.clone()).await;

    // Build result entries
    let entries: Vec<LeaderboardEntry> = leaderboard_data
        .iter()
        .enumerate()
        .filter_map(|(index, (principal_str, score))| {
            if let Ok(principal) = Principal::from_text(principal_str) {
                let rank = start + index as u32 + 1;

                // Determine reward based on tournament status
                let reward = if tournament.status == TournamentStatus::Completed {
                    // Use saved reward if exists (for winners), None for others
                    let saved_reward = rewards_map.get(&principal).copied();
                    // Convert CKBTC rewards from sats to USD for display (only for saved rewards)
                    if tournament.prize_token == TokenType::CKBTC {
                        saved_reward.map(|r| (r as f64 / USD_TO_CKBTC_SATS_RATE) as u64)
                    } else {
                        saved_reward
                    }
                } else {
                    // Tournament still active - calculate potential reward
                    calculate_reward(rank, tournament.prize_pool as u64)
                };

                // Username is guaranteed to exist for every principal
                let username = username_map.get(&principal).cloned().unwrap_or_else(|| {
                    log::error!("Missing username for principal {} in map", principal);
                    random_username_from_principal(principal, 15)
                });

                Some(LeaderboardEntry {
                    principal_id: principal,
                    username,
                    score: *score,
                    rank,
                    reward,
                })
            } else {
                None
            }
        })
        .collect();

    // Get total participants
    let total_participants = redis
        .get_total_participants(&tournament_id)
        .await
        .unwrap_or(0);

    // Calculate cursor info
    let has_more = (start + limit) < total_participants;
    let next_cursor = if has_more { Some(start + limit) } else { None };

    let cursor_info = CursorInfo {
        start,
        limit,
        total_count: total_participants,
        next_cursor,
        has_more,
    };

    // Build tournament info struct
    let tournament_info = TournamentResultInfo {
        id: tournament.id,
        start_time: tournament.start_time,
        end_time: tournament.end_time,
        status: tournament.status,
        prize_pool: tournament.prize_pool,
        prize_token: tournament.prize_token.to_string(),
        metric_type: tournament.metric_type.to_string(),
        metric_display_name: tournament.metric_display_name,
        num_winners: tournament.num_winners,
    };

    // Build response struct
    let response = TournamentResultsResponse {
        tournament: tournament_info,
        results: entries,
        cursor_info,
    };

    (StatusCode::OK, Json(response)).into_response()
}

// Admin: Start tournament and send notifications
pub async fn start_tournament_handler(
    Path(tournament_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match super::tournament::start_tournament(&tournament_id, &state).await {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "message": format!("Tournament {} started successfully", tournament_id),
            })),
        )
            .into_response(),
        Err(e) => {
            log::error!("Failed to start tournament {}: {:?}", tournament_id, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to start tournament: {}", e)
                })),
            )
                .into_response()
        }
    }
}

// Admin: End tournament manually (just change status to Ended)
pub async fn end_tournament_handler(
    Path(tournament_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match super::tournament::end_tournament(&tournament_id, &state).await {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "message": format!("Tournament {} manually ended (status set to Ended)", tournament_id),
            })),
        )
            .into_response(),
        Err(e) => {
            log::error!("Failed to end tournament {}: {:?}", tournament_id, e);
            let (status, message) = if e.to_string().contains("not found") {
                (StatusCode::NOT_FOUND, "Tournament not found")
            } else if e.to_string().contains("cannot be ended") {
                (StatusCode::BAD_REQUEST, "Tournament cannot be ended from current status")
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, "Failed to end tournament")
            };
            (
                status,
                Json(serde_json::json!({
                    "error": format!("{}: {}", message, e)
                })),
            )
                .into_response()
        }
    }
}

// Lifecycle check endpoint (can be called by a cron job)
#[utoipa::path(
    post,
    path = "/tournament/lifecycle-check",
    tag = "leaderboard",
    responses(
        (status = 200, description = "Lifecycle check completed")
    )
)]
pub async fn tournament_lifecycle_check_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match super::tournament::check_tournament_lifecycle(&state).await {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "message": "Tournament lifecycle check completed",
            })),
        )
            .into_response(),
        Err(e) => {
            log::error!("Tournament lifecycle check failed: {:?}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Lifecycle check failed: {}", e)
                })),
            )
                .into_response()
        }
    }
}
