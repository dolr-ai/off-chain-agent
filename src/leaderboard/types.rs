use candid::Principal;
use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumString};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, Serialize, Deserialize, Display, EnumString, PartialEq, Eq, ToSchema)]
pub enum TokenType {
    #[strum(serialize = "YRAL")]
    #[serde(rename = "YRAL")]
    YRAL,
    #[strum(serialize = "CKBTC")]
    #[serde(rename = "CKBTC")]
    CKBTC, // Note : for CKBTC, prize pool is in USD units.
}

#[allow(clippy::derivable_impls)]
impl Default for TokenType {
    fn default() -> Self {
        TokenType::YRAL
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Display, EnumString, PartialEq, Eq, ToSchema)]
pub enum MetricType {
    #[strum(serialize = "games_played")]
    #[serde(rename = "games_played")]
    GamesPlayed,

    #[strum(serialize = "games_won")]
    #[serde(rename = "games_won")]
    GamesWon,

    #[strum(serialize = "tokens_earned")]
    #[serde(rename = "tokens_earned")]
    TokensEarned,

    #[strum(serialize = "videos_watched")]
    #[serde(rename = "videos_watched")]
    VideosWatched,

    #[strum(serialize = "referrals_made")]
    #[serde(rename = "referrals_made")]
    ReferralsMade,

    #[strum(serialize = "custom")]
    #[serde(rename = "custom")]
    Custom(String),
}

#[allow(clippy::derivable_impls)]
impl Default for MetricType {
    fn default() -> Self {
        MetricType::GamesWon
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ScoreOperation {
    #[serde(rename = "increment")]
    Increment,
    #[serde(rename = "set_if_higher")]
    SetIfHigher,
    #[serde(rename = "set")]
    Set,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub enum TournamentStatus {
    #[serde(rename = "upcoming")]
    Upcoming,
    #[serde(rename = "active")]
    Active,
    #[serde(rename = "finalizing")]
    Finalizing,
    #[serde(rename = "completed")]
    Completed,
    #[serde(rename = "ended")]
    Ended,
    #[serde(rename = "cancelled")]
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub enum SortOrder {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

#[allow(clippy::derivable_impls)]
impl Default for SortOrder {
    fn default() -> Self {
        SortOrder::Desc // Keep current behavior as default
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tournament {
    pub id: String,
    pub start_time: i64,
    pub end_time: i64,
    pub prize_pool: f64,
    pub prize_token: TokenType,
    pub status: TournamentStatus,
    pub metric_type: MetricType,
    pub metric_display_name: String,
    pub allowed_sources: Vec<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LeaderboardEntry {
    #[schema(value_type = String)]
    pub principal_id: Principal,
    pub username: String,
    pub score: f64,
    pub rank: u32,
    pub reward: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TournamentResult {
    pub tournament_id: String,
    pub user_results: Vec<LeaderboardEntry>,
    pub total_participants: u32,
    pub total_prize_distributed: u64,
    pub finalized_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTournamentRequest {
    pub start_time: i64,
    pub end_time: i64,
    pub prize_pool: f64,
    pub prize_token: TokenType,
    pub metric_type: MetricType,
    pub metric_display_name: String,
    pub allowed_sources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateScoreRequest {
    pub principal_id: Principal,
    pub metric_value: f64,
    pub metric_type: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderboardResponse {
    pub tournament: Tournament,
    pub entries: Vec<LeaderboardEntry>,
    pub user_rank: Option<UserRankInfo>,
    pub total_participants: u32,
    pub last_updated: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserRankInfo {
    #[schema(value_type = String)]
    pub principal_id: Principal,
    pub username: String,
    pub rank: u32,
    pub score: f64,
    pub reward: Option<u64>,
    pub percentile: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchUsersRequest {
    pub query: String,
    pub tournament_id: String,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TournamentHistoryResponse {
    pub tournaments: Vec<TournamentSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TournamentSummary {
    pub id: String,
    pub start_time: i64,
    pub end_time: i64,
    pub status: TournamentStatus,
    pub prize_pool: f64,
    pub prize_token: TokenType,
    pub total_participants: u32,
    pub winner: Option<WinnerInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WinnerInfo {
    pub principal_id: Principal,
    pub username: String,
    pub score: f64,
    pub reward: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserRankResponse {
    pub user: UserRankInfo,
    pub surrounding_players: Vec<LeaderboardEntry>,
    pub tournament: TournamentRankInfo,
    pub total_participants: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TournamentRankInfo {
    pub id: String,
    pub metric_type: String,
    pub metric_display_name: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TournamentResultsResponse {
    pub tournament: TournamentResultInfo,
    pub results: Vec<LeaderboardEntry>,
    pub cursor_info: CursorInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TournamentResultInfo {
    pub id: String,
    pub start_time: i64,
    pub end_time: i64,
    pub status: TournamentStatus,
    pub prize_pool: f64,
    pub prize_token: String,
    pub metric_type: String,
    pub metric_display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrizeDistribution {
    pub rank_range: RankRange,
    pub percentage: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RankRange {
    Single(u32),
    Range(u32, u32),
}

impl PrizeDistribution {
    fn default_distribution() -> Vec<PrizeDistribution> {
        vec![
            PrizeDistribution {
                rank_range: RankRange::Single(1),
                percentage: 20.0,
            },
            PrizeDistribution {
                rank_range: RankRange::Single(2),
                percentage: 15.0,
            },
            PrizeDistribution {
                rank_range: RankRange::Single(3),
                percentage: 11.0,
            },
            PrizeDistribution {
                rank_range: RankRange::Range(4, 5),
                percentage: 7.0, // Each gets 7%
            },
            PrizeDistribution {
                rank_range: RankRange::Range(6, 10),
                percentage: 4.0, // Each gets 4%
            },
            PrizeDistribution {
                rank_range: RankRange::Range(11, 20),
                percentage: 2.0, // Each gets 2%
            },
        ]
    }
}

pub fn calculate_reward(rank: u32, total_prize_pool: u64) -> Option<u64> {
    let distributions = PrizeDistribution::default_distribution();

    for dist in distributions.iter() {
        match dist.rank_range {
            RankRange::Single(r) if r == rank => {
                return Some((total_prize_pool as f64 * dist.percentage as f64 / 100.0) as u64);
            }
            RankRange::Range(start, end) if rank >= start && rank <= end => {
                return Some((total_prize_pool as f64 * dist.percentage as f64 / 100.0) as u64);
            }
            _ => continue,
        }
    }

    None
}

// Pagination types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorPaginationParams {
    pub start: Option<u32>, // Default: 0
    pub limit: Option<u32>, // Default: 50, Max: 100
}

// Extended pagination params for leaderboard with optional user info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderboardQueryParams {
    pub start: Option<u32>,            // Default: 0
    pub limit: Option<u32>,            // Default: 50, Max: 100
    pub user_id: Option<String>,       // Optional principal ID to get user's rank info
    pub sort_order: Option<SortOrder>, // Default: Desc
    pub tournament_id: Option<String>, // Optional tournament ID for historical data
}

// User's last completed tournament info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserLastTournament {
    pub tournament_id: String,
    pub rank: u32,
    pub reward: Option<u64>,
    pub status: String, // "seen" or "unseen"
}

// Response format for user's last tournament
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserLastTournamentResponse {
    pub tournament_id: String,
    pub rank: u32,
    pub reward: Option<u64>,
    pub status: String,
}

impl Default for CursorPaginationParams {
    fn default() -> Self {
        Self {
            start: Some(0),
            limit: Some(50),
        }
    }
}

impl CursorPaginationParams {
    pub fn get_start(&self) -> u32 {
        self.start.unwrap_or(0)
    }

    pub fn get_limit(&self) -> u32 {
        self.limit.unwrap_or(50).min(100) // Max 100
    }
}

impl Default for LeaderboardQueryParams {
    fn default() -> Self {
        Self {
            start: Some(0),
            limit: Some(50),
            user_id: None,
            sort_order: None,
            tournament_id: None,
        }
    }
}

impl LeaderboardQueryParams {
    pub fn get_start(&self) -> u32 {
        self.start.unwrap_or(0)
    }

    pub fn get_limit(&self) -> u32 {
        self.limit.unwrap_or(50).min(100) // Max 100
    }

    pub fn get_sort_order(&self) -> SortOrder {
        self.sort_order.clone().unwrap_or_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorPaginatedResponse<T> {
    pub data: Vec<T>,
    pub cursor_info: CursorInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CursorInfo {
    pub start: u32,
    pub limit: u32,
    pub total_count: u32,
    pub next_cursor: Option<u32>, // None if no more data
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TournamentInfo {
    pub id: String,
    pub start_time: i64,
    pub end_time: i64,
    pub status: TournamentStatus,
    pub prize_pool: f64,
    pub prize_token: TokenType,
    pub metric_type: MetricType,
    pub metric_display_name: String,
    pub client_timezone: Option<String>,
    pub client_start_time: Option<String>, // ISO 8601 formatted in client's timezone
    pub client_end_time: Option<String>,   // ISO 8601 formatted in client's timezone
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LeaderboardWithTournamentResponse {
    pub data: Vec<LeaderboardEntry>,
    pub cursor_info: CursorInfo,
    pub tournament_info: TournamentInfo,
    pub user_info: Option<serde_json::Value>,
    pub upcoming_tournament_info: Option<TournamentInfo>,
    pub last_tournament_info: Option<UserLastTournamentResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoParams)]
pub struct SearchParams {
    pub q: String,
    pub start: Option<u32>,            // Default: 0
    pub limit: Option<u32>,            // Default: 50, Max: 100
    pub sort_order: Option<SortOrder>, // Default: Desc
}

impl SearchParams {
    pub fn get_start(&self) -> u32 {
        self.start.unwrap_or(0)
    }

    pub fn get_limit(&self) -> u32 {
        self.limit.unwrap_or(50).min(100) // Max 100
    }

    pub fn get_sort_order(&self) -> SortOrder {
        self.sort_order.clone().unwrap_or_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeTournamentRequest {
    pub tournament_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LeaderboardError {
    TournamentNotFound,
    UserNotFound,
    InvalidTournamentStatus,
    RedisError(String),
    MetadataServiceError(String),
    InvalidRequest(String),
}

impl std::fmt::Display for LeaderboardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaderboardError::TournamentNotFound => write!(f, "Tournament not found"),
            LeaderboardError::UserNotFound => write!(f, "User not found"),
            LeaderboardError::InvalidTournamentStatus => write!(f, "Invalid tournament status"),
            LeaderboardError::RedisError(e) => write!(f, "Redis error: {}", e),
            LeaderboardError::MetadataServiceError(e) => write!(f, "Metadata service error: {}", e),
            LeaderboardError::InvalidRequest(e) => write!(f, "Invalid request: {}", e),
        }
    }
}

impl std::error::Error for LeaderboardError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_calculate_reward() {
        let total_prize = 1000000; // 1 million tokens

        assert_eq!(calculate_reward(1, total_prize), Some(200000)); // 20%
        assert_eq!(calculate_reward(2, total_prize), Some(150000)); // 15%
        assert_eq!(calculate_reward(3, total_prize), Some(110000)); // 11%
        assert_eq!(calculate_reward(4, total_prize), Some(70000)); // 7%
        assert_eq!(calculate_reward(5, total_prize), Some(70000)); // 7%
        assert_eq!(calculate_reward(6, total_prize), Some(40000)); // 4%
        assert_eq!(calculate_reward(10, total_prize), Some(40000)); // 4%
        assert_eq!(calculate_reward(11, total_prize), Some(20000)); // 2%
        assert_eq!(calculate_reward(20, total_prize), Some(20000)); // 2%
        assert_eq!(calculate_reward(21, total_prize), None); // No reward
    }

    #[test]
    fn test_token_type_enum_string() {
        // Test FromStr (via EnumString)
        assert_eq!(TokenType::from_str("YRAL").unwrap(), TokenType::YRAL);
        assert!(TokenType::from_str("INVALID").is_err());

        // Test Display
        assert_eq!(TokenType::YRAL.to_string(), "YRAL");

        // Test Default
        assert_eq!(TokenType::default(), TokenType::YRAL);

        // Test Serialize/Deserialize
        let token = TokenType::YRAL;
        let json = serde_json::to_string(&token).unwrap();
        assert_eq!(json, "\"YRAL\"");

        let parsed: TokenType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, TokenType::YRAL);
    }
}
