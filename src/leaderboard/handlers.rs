use axum::{
    extract::{Json, Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
};
use candid::Principal;
use chrono::Utc;
use futures::stream::{self, StreamExt};
use std::sync::Arc;
use yral_canisters_common::utils::token::{
    SatsOperations, TokenOperations, TokenOperationsProvider,
};
use yral_username_gen::random_username_from_principal;

use super::redis_ops::LeaderboardRedis;
use super::types::*;
use crate::{app_state::AppState, auth::check_auth_events};

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
    // let now = Utc::now().timestamp();
    // if now < tournament.start_time || now > tournament.end_time {
    //     return (
    //         StatusCode::BAD_REQUEST,
    //         Json(serde_json::json!({
    //             "error": "Tournament is not active"
    //         })),
    //     )
    //         .into_response();
    // }

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
    let score_for_metadata = new_score;
    let tournament_id_clone = current_tournament.clone();

    tokio::spawn(async move {
        match metadata_client
            .get_user_metadata_v2(principal.to_string())
            .await
        {
            Ok(Some(metadata)) if !metadata.user_name.trim().is_empty() => {
                // Cache username for 1 hour
                if let Err(e) = redis_clone
                    .cache_username(principal, &metadata.user_name, 3600)
                    .await
                {
                    log::warn!("Failed to cache username: {:?}", e);
                }

                // Store user metadata
                let user_data = UserTournamentData {
                    principal_id: principal,
                    username: metadata.user_name,
                    score: score_for_metadata,
                    last_updated: Utc::now().timestamp(),
                };

                if let Err(e) = redis_clone
                    .store_user_metadata(&tournament_id_clone, principal, &user_data)
                    .await
                {
                    log::warn!("Failed to store user metadata: {:?}", e);
                }
            }
            Ok(Some(metadata)) if metadata.user_name.trim().is_empty() => {
                log::info!("Empty username for principal {}, generating one", principal);
                // Fall through to generation logic below
                let generated_username = random_username_from_principal(principal, 15);

                log::info!(
                    "Using generated username for principal {}: {}",
                    principal,
                    generated_username
                );

                // Cache the generated username for consistency
                if let Err(e) = redis_clone
                    .cache_username(principal, &generated_username, 3600)
                    .await
                {
                    log::warn!("Failed to cache generated username: {:?}", e);
                }

                // Store user metadata with generated username
                let user_data = UserTournamentData {
                    principal_id: principal,
                    username: generated_username,
                    score: score_for_metadata,
                    last_updated: Utc::now().timestamp(),
                };

                if let Err(e) = redis_clone
                    .store_user_metadata(&tournament_id_clone, principal, &user_data)
                    .await
                {
                    log::warn!("Failed to store user metadata: {:?}", e);
                }
            }
            Ok(None) | Err(_) | _ => {
                // Generate deterministic username from principal
                let generated_username = random_username_from_principal(principal, 15);

                log::info!(
                    "Using generated username for principal {}: {}",
                    principal,
                    generated_username
                );

                // Cache the generated username for consistency
                if let Err(e) = redis_clone
                    .cache_username(principal, &generated_username, 3600)
                    .await
                {
                    log::warn!("Failed to cache generated username: {:?}", e);
                }

                // Store user metadata with generated username
                let user_data = UserTournamentData {
                    principal_id: principal,
                    username: generated_username,
                    score: score_for_metadata,
                    last_updated: Utc::now().timestamp(),
                };

                if let Err(e) = redis_clone
                    .store_user_metadata(&tournament_id_clone, principal, &user_data)
                    .await
                {
                    log::warn!("Failed to store user metadata: {:?}", e);
                }
            }
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
        (status = 200, description = "Leaderboard data retrieved"),
        (status = 404, description = "No active tournament")
    )
)]
pub async fn get_leaderboard_handler(
    Query(params): Query<CursorPaginationParams>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let redis = LeaderboardRedis::new(state.leaderboard_redis_pool.clone());

    let start = params.get_start();
    let limit = params.get_limit();

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

    // Get tournament info
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

    // Get paginated players
    let leaderboard_data = match redis
        .get_leaderboard(
            &current_tournament,
            start as isize,
            (start + limit - 1) as isize,
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

    // Collect principals for bulk metadata fetch
    let principals: Vec<Principal> = leaderboard_data
        .iter()
        .filter_map(|(principal_str, _)| Principal::from_text(principal_str).ok())
        .collect();

    // Bulk fetch usernames
    let metadata_map = match state
        .yral_metadata_client
        .get_user_metadata_bulk(principals.clone())
        .await
    {
        Ok(map) => map,
        Err(e) => {
            log::warn!("Failed to fetch bulk metadata: {:?}", e);
            std::collections::HashMap::new()
        }
    };

    // Build leaderboard entries
    let entries: Vec<LeaderboardEntry> = leaderboard_data
        .iter()
        .enumerate()
        .filter_map(|(index, (principal_str, score))| {
            if let Ok(principal) = Principal::from_text(principal_str) {
                let metadata = metadata_map.get(&principal);
                let rank = start + index as u32 + 1; // Calculate actual rank
                Some(LeaderboardEntry {
                    principal_id: principal,
                    username: metadata
                        .and_then(|m| m.as_ref().map(|m| m.user_name.clone()))
                        .unwrap_or_else(|| "Anonymous".to_string()),
                    score: *score,
                    rank,
                    reward: calculate_reward(rank, tournament.prize_pool as u64),
                })
            } else {
                None
            }
        })
        .collect();

    // Get total participants
    let total_participants = match redis.get_total_participants(&current_tournament).await {
        Ok(count) => count,
        Err(_) => 0,
    };

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

    let response = CursorPaginatedResponse {
        data: entries,
        cursor_info,
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

    // Get user's rank
    let user_rank = match redis.get_user_rank(&current_tournament, principal).await {
        Ok(Some(rank)) => rank,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "error": "User not found in leaderboard"
                })),
            )
                .into_response();
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
    let user_score = match redis.get_user_score(&current_tournament, principal).await {
        Ok(Some(score)) => score,
        Ok(None) => 0.0,
        Err(_) => 0.0,
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

    // Get total participants
    let total_participants = match redis.get_total_participants(&current_tournament).await {
        Ok(count) => count,
        Err(_) => 0,
    };

    // Fetch username
    let username = match state
        .yral_metadata_client
        .get_user_metadata_v2(principal.to_string())
        .await
    {
        Ok(Some(metadata)) if !metadata.user_name.trim().is_empty() => metadata.user_name,
        _ => {
            // Generate deterministic username from principal for empty/missing usernames
            let generated = random_username_from_principal(principal, 15);
            // Also cache it for consistency
            if let Err(e) = redis.cache_username(principal, &generated, 3600).await {
                log::warn!("Failed to cache generated username: {:?}", e);
            }
            generated
        }
    };

    // Get surrounding players (2 above, 2 below)
    let context_start = (user_rank as i32 - 3).max(0) as u32;
    let context_end = context_start + 4; // 5 total including user

    let surrounding_data = match redis
        .get_leaderboard(
            &current_tournament,
            context_start as isize,
            context_end as isize,
        )
        .await
    {
        Ok(data) => data,
        Err(_) => vec![],
    };

    // Build surrounding entries
    let surrounding_entries: Vec<LeaderboardEntry> = surrounding_data
        .iter()
        .enumerate()
        .filter_map(|(index, (principal_str, score))| {
            if let Ok(p) = Principal::from_text(principal_str) {
                let rank = context_start + index as u32 + 1;
                Some(LeaderboardEntry {
                    principal_id: p,
                    username: if p == principal {
                        username.clone()
                    } else {
                        "Anonymous".to_string() // We could batch fetch these if needed
                    },
                    score: *score,
                    rank,
                    reward: calculate_reward(rank, tournament.prize_pool as u64),
                })
            } else {
                None
            }
        })
        .collect();

    let response = serde_json::json!({
        "user": {
            "principal_id": principal.to_string(),
            "username": username,
            "rank": user_rank,
            "score": user_score,
            "percentile": ((total_participants - user_rank + 1) as f32 / total_participants as f32 * 100.0),
            "reward": calculate_reward(user_rank, tournament.prize_pool as u64),
        },
        "surrounding_players": surrounding_entries,
        "tournament": {
            "id": tournament.id,
            "metric_type": tournament.metric_type.to_string(),
            "metric_display_name": tournament.metric_display_name,
        },
        "total_participants": total_participants,
    });

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
        .search_users(&current_tournament, &params.q, limit * 10)
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

    // Fetch metadata for results
    let principals: Vec<Principal> = paginated_results.iter().map(|(p, _)| *p).collect();

    let metadata_map = match state
        .yral_metadata_client
        .get_user_metadata_bulk(principals.clone())
        .await
    {
        Ok(map) => map,
        Err(e) => {
            log::warn!("Failed to fetch bulk metadata: {:?}", e);
            std::collections::HashMap::new()
        }
    };

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

    // Build search result entries with ranks
    let mut entries = Vec::new();
    for (principal, score) in &paginated_results {
        if let Some(rank) = redis
            .get_user_rank(&current_tournament, *principal)
            .await
            .ok()
            .flatten()
        {
            let metadata = metadata_map.get(&principal);
            entries.push(LeaderboardEntry {
                principal_id: *principal,
                username: metadata
                    .and_then(|m| m.as_ref().map(|m| m.user_name.clone()))
                    .unwrap_or_else(|| "Anonymous".to_string()),
                score: *score,
                rank,
                reward: calculate_reward(rank, tournament.prize_pool as u64),
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
            let winner_info = if let Ok(top_players) =
                redis.get_leaderboard(tournament_id, 0, 0).await
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

                        Some(WinnerInfo {
                            principal_id: principal,
                            username,
                            score: *score,
                            reward: calculate_reward(1, tournament.prize_pool as u64).unwrap_or(0),
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
#[utoipa::path(
    post,
    path = "/tournament/create",
    tag = "leaderboard",
    request_body = serde_json::Value,
    responses(
        (status = 201, description = "Tournament created successfully"),
        (status = 500, description = "Failed to create tournament")
    )
)]
pub async fn create_tournament_handler(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateTournamentRequest>,
) -> impl IntoResponse {
    let redis = LeaderboardRedis::new(state.leaderboard_redis_pool.clone());

    // TODO: Add admin authentication check here
    // For now, we'll proceed without auth

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

        // TODO: Schedule finalize for end_time
        log::info!(
            "Tournament {} created and started immediately. Should schedule finalize for {}",
            tournament_id,
            tournament.end_time
        );
    } else {
        // TODO: Schedule start for start_time
        log::info!(
            "Tournament {} created with Upcoming status. Should schedule start for {}",
            tournament_id,
            tournament.start_time
        );
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
#[utoipa::path(
    post,
    path = "/tournament/finalize",
    tag = "leaderboard",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Tournament finalized successfully"),
        (status = 404, description = "Tournament not found"),
        (status = 400, description = "Tournament is not active")
    )
)]
pub async fn finalize_tournament_handler(
    State(state): State<Arc<AppState>>,
    Json(request): Json<FinalizeTournamentRequest>,
) -> impl IntoResponse {
    // TODO: Add admin authentication check here

    match super::tournament::finalize_tournament(&request.tournament_id, &state).await {
        Ok(_) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "success": true,
                "message": format!("Tournament {} finalized successfully", request.tournament_id),
            })),
        )
            .into_response(),
        Err(e) => {
            log::error!("Failed to finalize tournament {}: {:?}", request.tournament_id, e);
            let (status, message) = if e.to_string().contains("not found") {
                (StatusCode::NOT_FOUND, "Tournament not found")
            } else if e.to_string().contains("not active") {
                (StatusCode::BAD_REQUEST, "Tournament is not active")
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, "Failed to finalize tournament")
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
        .get_leaderboard(&tournament_id, start as isize, (start + limit - 1) as isize)
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
    let rewards_map: std::collections::HashMap<Principal, u64> = if let Some(ref results) = saved_results {
        results.user_results
            .iter()
            .filter_map(|entry| entry.reward.map(|r| (entry.principal_id, r)))
            .collect()
    } else {
        std::collections::HashMap::new()
    };

    // Collect principals for bulk metadata fetch
    let principals: Vec<Principal> = leaderboard_data
        .iter()
        .filter_map(|(principal_str, _)| Principal::from_text(principal_str).ok())
        .collect();

    // Bulk fetch usernames
    let metadata_map = match state
        .yral_metadata_client
        .get_user_metadata_bulk(principals)
        .await
    {
        Ok(map) => map,
        Err(e) => {
            log::warn!("Failed to fetch bulk metadata: {:?}", e);
            std::collections::HashMap::new()
        }
    };

    // Build result entries
    let entries: Vec<LeaderboardEntry> = leaderboard_data
        .iter()
        .enumerate()
        .filter_map(|(index, (principal_str, score))| {
            if let Ok(principal) = Principal::from_text(principal_str) {
                let metadata = metadata_map.get(&principal);
                let rank = start + index as u32 + 1;
                
                // Determine reward based on tournament status
                let reward = if tournament.status == TournamentStatus::Completed {
                    // Use saved reward if exists (for winners), None for others
                    rewards_map.get(&principal).copied()
                } else {
                    // Tournament still active - calculate potential reward
                    calculate_reward(rank, tournament.prize_pool as u64)
                };
                
                Some(LeaderboardEntry {
                    principal_id: principal,
                    username: metadata
                        .and_then(|m| m.as_ref().map(|m| m.user_name.clone()))
                        .unwrap_or_else(|| "Anonymous".to_string()),
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
    let total_participants = match redis.get_total_participants(&tournament_id).await {
        Ok(count) => count,
        Err(_) => 0,
    };

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

    let response = serde_json::json!({
        "tournament": {
            "id": tournament.id,
            "start_time": tournament.start_time,
            "end_time": tournament.end_time,
            "status": tournament.status,
            "prize_pool": tournament.prize_pool,
            "prize_token": tournament.prize_token.to_string(),
            "metric_type": tournament.metric_type.to_string(),
            "metric_display_name": tournament.metric_display_name,
        },
        "results": entries,
        "cursor_info": cursor_info,
    });

    (StatusCode::OK, Json(response)).into_response()
}

// Admin: Start tournament and send notifications
#[utoipa::path(
    post,
    path = "/tournament/{id}/start",
    tag = "leaderboard",
    responses(
        (status = 200, description = "Tournament started successfully"),
        (status = 404, description = "Tournament not found"),
        (status = 400, description = "Tournament cannot be started")
    )
)]
pub async fn start_tournament_handler(
    Path(tournament_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // TODO: Add admin authentication check here

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
#[utoipa::path(
    post,
    path = "/tournament/{id}/end",
    tag = "leaderboard",
    responses(
        (status = 200, description = "Tournament ended successfully"),
        (status = 404, description = "Tournament not found"),
        (status = 400, description = "Tournament cannot be ended")
    )
)]
pub async fn end_tournament_handler(
    Path(tournament_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // TODO: Add admin authentication check here

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
