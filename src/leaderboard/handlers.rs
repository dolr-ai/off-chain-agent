use axum::{
    extract::{Json, Path, Query, State},
    http::StatusCode,
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
use crate::app_state::AppState;

// Internal API: Update user score on balance change
#[utoipa::path(
    post,
    path = "/score/update",
    tag = "leaderboard",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Score updated successfully"),
        (status = 404, description = "No active tournament"),
        (status = 400, description = "Invalid request")
    )
)]
pub async fn update_score_handler(
    State(state): State<Arc<AppState>>,
    Json(request): Json<UpdateScoreRequest>,
) -> impl IntoResponse {
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

    // Create tournament
    let tournament = Tournament {
        id: tournament_id.clone(),
        start_time: request.start_time,
        end_time: request.end_time,
        prize_pool: request.prize_pool,
        prize_token: request.prize_token,
        status: TournamentStatus::Upcoming,
        metric_type: request.metric_type,
        metric_display_name: request.metric_display_name,
        allowed_sources: request.allowed_sources,
        created_at: Utc::now().timestamp(),
        updated_at: Utc::now().timestamp(),
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

    // Set as current tournament
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

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "success": true,
            "tournament": tournament,
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
    let redis = LeaderboardRedis::new(state.leaderboard_redis_pool.clone());

    // TODO: Add admin authentication check here

    // Get tournament info
    let mut tournament = match redis.get_tournament_info(&request.tournament_id).await {
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

    // Check if tournament can be finalized
    if tournament.status != TournamentStatus::Active {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "Tournament is not active"
            })),
        )
            .into_response();
    }

    // Get final leaderboard (top 10 for prizes)
    let top_players = match redis.get_leaderboard(&request.tournament_id, 0, 9).await {
        Ok(data) => data,
        Err(e) => {
            log::error!("Failed to get final leaderboard: {:?}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "Failed to get final leaderboard"
                })),
            )
                .into_response();
        }
    };

    // Calculate prize distribution and prepare for token distribution
    let mut prize_distribution = Vec::new();
    let mut distribution_tasks = Vec::new();
    
    for (rank, (principal_str, score)) in top_players.iter().enumerate() {
        if let Ok(principal) = Principal::from_text(principal_str) {
            let rank = (rank + 1) as u32;
            if let Some(reward) = calculate_reward(rank, tournament.prize_pool as u64) {
                prize_distribution.push(serde_json::json!({
                    "rank": rank,
                    "principal_id": principal.to_string(),
                    "score": score,
                    "reward": reward,
                }));
                
                // Also prepare for actual token distribution
                distribution_tasks.push((principal, reward, rank));
            }
        }
    }

    // Distribute prizes if tournament uses YRAL (which is Sats internally)
    if tournament.prize_token == TokenType::YRAL && !distribution_tasks.is_empty() {
        // Get JWT token from environment
        let jwt_token = std::env::var("YRAL_HON_WORKER_JWT").ok();
        
        // Create Sats operations provider
        let token_ops = TokenOperationsProvider::Sats(SatsOperations::new(jwt_token));
        let token_ops = Arc::new(token_ops);

        // Process distributions concurrently, 5 at a time
        let results: Vec<_> = stream::iter(distribution_tasks.clone())
            .map(|(principal, reward, rank)| {
                let token_ops = token_ops.clone();
                async move {
                    match token_ops.add_balance(principal, reward).await {
                        Ok(_) => {
                            log::info!("Distributed {} SATS to {} (rank {})", reward, principal, rank);
                            Ok((principal, reward, rank))
                        }
                        Err(e) => {
                            log::error!("Failed to distribute {} SATS to {} (rank {}): {:?}", 
                                       reward, principal, rank, e);
                            Err((principal, reward, rank, e))
                        }
                    }
                }
            })
            .buffer_unordered(5) // Process 5 concurrent requests at a time
            .collect()
            .await;

        // Log summary
        let successful = results.iter().filter(|r| r.is_ok()).count();
        let failed = results.iter().filter(|r| r.is_err()).count();
        log::info!("Prize distribution complete: {} successful, {} failed", successful, failed);
    }

    // Build and save tournament results for winners
    let mut winner_entries = Vec::new();
    let mut total_prize_distributed = 0u64;
    
    // Collect winner data from distribution_tasks (these have the actual rewards)
    for (principal, reward, rank) in &distribution_tasks {
        // Get username for winner
        let username = match state.yral_metadata_client.get_user_metadata_v2(principal.to_string()).await {
            Ok(Some(metadata)) if !metadata.user_name.trim().is_empty() => metadata.user_name,
            _ => {
                let generated = random_username_from_principal(*principal, 15);
                // Cache generated username
                if let Err(e) = redis.cache_username(*principal, &generated, 3600).await {
                    log::warn!("Failed to cache generated username: {:?}", e);
                }
                generated
            }
        };
        
        // Find the score for this principal from top_players
        let score = top_players
            .iter()
            .find(|(p_str, _)| Principal::from_text(p_str).ok() == Some(*principal))
            .map(|(_, s)| *s)
            .unwrap_or(0.0);
        
        winner_entries.push(LeaderboardEntry {
            principal_id: *principal,
            username,
            score,
            rank: *rank,
            reward: Some(*reward),
        });
        
        total_prize_distributed += reward;
    }
    
    // Create tournament result
    let tournament_result = TournamentResult {
        tournament_id: request.tournament_id.clone(),
        user_results: winner_entries,
        total_participants: redis
            .get_total_participants(&request.tournament_id)
            .await
            .unwrap_or(0),
        total_prize_distributed,
        finalized_at: Utc::now().timestamp(),
    };
    
    // Save tournament results
    if let Err(e) = redis.save_tournament_results(&tournament_result).await {
        log::error!("Failed to save tournament results: {:?}", e);
    }
    
    // Update tournament status
    tournament.status = TournamentStatus::Completed;
    tournament.updated_at = Utc::now().timestamp();

    if let Err(e) = redis.set_tournament_info(&tournament).await {
        log::error!("Failed to update tournament status: {:?}", e);
    }

    // Add to history
    if let Err(e) = redis.add_to_history(&request.tournament_id).await {
        log::error!("Failed to add tournament to history: {:?}", e);
    }

    // Clear current tournament if this was the current one
    if let Ok(Some(current)) = redis.get_current_tournament().await {
        if current == request.tournament_id {
            // You might want to set a new tournament here or clear it
            // For now, we'll leave it as is
        }
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "success": true,
            "tournament_id": request.tournament_id,
            "status": "completed",
            "prize_distribution": prize_distribution,
            "total_prizes_distributed": tournament.prize_pool,
        })),
    )
        .into_response()
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

// Admin: End tournament and send winner notifications
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
                "message": format!("Tournament {} ended successfully", tournament_id),
            })),
        )
            .into_response(),
        Err(e) => {
            log::error!("Failed to end tournament {}: {:?}", tournament_id, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("Failed to end tournament: {}", e)
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
