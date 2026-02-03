use candid::Principal;
use serde::{de::Error, Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use utoipa::ToSchema;
use yral_metadata_types::{
    AndroidConfig, AndroidNotification, ApnsConfig, ApnsFcmOptions, NotificationPayload,
    SendNotificationReq, WebpushConfig, WebpushFcmOptions,
};
use yral_metrics::metrics::{
    like_video::LikeVideo, sealed_metric::SealedMetric,
    video_duration_watched::VideoDurationWatched, video_watched::VideoWatched,
};

use crate::app_state::AppState;
use crate::rewards::config::RewardTokenType;

pub fn string_or_number<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrNumber {
        String(String),
        Number(u64),
    }

    match StringOrNumber::deserialize(deserializer)? {
        StringOrNumber::String(s) => Ok(s),
        StringOrNumber::Number(n) => Ok(n.to_string()),
    }
}

#[derive(Serialize, Clone, Debug, ToSchema)]
#[serde(tag = "event")]
pub enum AnalyticsEvent {
    VideoWatched(VideoWatched),
    VideoDurationWatched(VideoDurationWatched),
    VideoStarted(VideoStarted),
    LikeVideo(LikeVideo),
}

// open issues for tagged and untagged enums - https://github.com/serde-rs/json/issues/1046 and https://github.com/serde-rs/json/issues/1108
impl<'de> Deserialize<'de> for AnalyticsEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // First deserialize to a generic Value to handle arbitrary_precision issues
        let value = Value::deserialize(deserializer)?;

        // Then try to deserialize from the Value to our enum
        match value.get("event").and_then(|v| v.as_str()) {
            Some("VideoWatched") => {
                let video_watched: VideoWatched =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(AnalyticsEvent::VideoWatched(video_watched))
            }
            Some("VideoDurationWatched") => {
                let video_duration_watched: VideoDurationWatched =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(AnalyticsEvent::VideoDurationWatched(video_duration_watched))
            }
            Some("VideoStarted") => {
                let video_started: VideoStarted =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(AnalyticsEvent::VideoStarted(video_started))
            }
            Some("LikeVideo") => {
                let like_video: LikeVideo =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(AnalyticsEvent::LikeVideo(like_video))
            }
            Some(event_type) => Err(serde::de::Error::custom(format!(
                "Unknown event type: {event_type}"
            ))),
            None => Err(serde::de::Error::custom("Missing 'event' field")),
        }
    }
}

macro_rules! delegate_metric_method {
    ($self:ident, $method:ident) => {
        match $self {
            AnalyticsEvent::VideoWatched(event) => event.$method(),
            AnalyticsEvent::VideoDurationWatched(event) => event.$method(),
            AnalyticsEvent::VideoStarted(event) => event.$method(),
            AnalyticsEvent::LikeVideo(event) => event.$method(),
        }
    };
    // Overload for methods that need serde_json::to_value
    ($self:ident, $method:ident, to_value) => {
        match $self {
            AnalyticsEvent::VideoWatched(event) => serde_json::to_value(event).unwrap(),
            AnalyticsEvent::VideoDurationWatched(event) => serde_json::to_value(event).unwrap(),
            AnalyticsEvent::VideoStarted(event) => serde_json::to_value(event).unwrap(),
            AnalyticsEvent::LikeVideo(event) => serde_json::to_value(event).unwrap(),
        }
    };
}

impl SealedMetric for AnalyticsEvent {
    fn tag(&self) -> String {
        delegate_metric_method!(self, tag)
    }

    fn user_id(&self) -> Option<String> {
        delegate_metric_method!(self, user_id)
    }

    fn user_canister(&self) -> Option<Principal> {
        delegate_metric_method!(self, user_canister)
    }
}

impl AnalyticsEvent {
    pub fn params(&self) -> Value {
        // Use the overloaded macro variant for to_value
        delegate_metric_method!(self, params, to_value)
    }
}

// --------------------------------------------------
// VideoWatched
// --------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoDurationWatchedPayload {
    #[serde(rename = "publisher_user_id")]
    pub publisher_user_id: Option<Principal>,
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_logged_in: Option<bool>,
    #[serde(rename = "display_name", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "video_id", skip_serializing_if = "Option::is_none")]
    pub video_id: Option<String>,
    #[serde(rename = "video_category")]
    pub video_category: String,
    #[serde(rename = "creator_category")]
    pub creator_category: String,
    #[serde(rename = "hashtag_count", skip_serializing_if = "Option::is_none")]
    pub hashtag_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_nsfw: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_hot_or_not: Option<bool>,
    #[serde(rename = "feed_type")]
    pub feed_type: String,
    #[serde(rename = "view_count", skip_serializing_if = "Option::is_none")]
    pub view_count: Option<u64>,
    #[serde(rename = "like_count", skip_serializing_if = "Option::is_none")]
    pub like_count: Option<u64>,
    #[serde(rename = "share_count")]
    pub share_count: u64,
    #[serde(rename = "percentage_watched")]
    pub percentage_watched: f64,
    #[serde(rename = "absolute_watched")]
    pub absolute_watched: f64,
    #[serde(rename = "video_duration")]
    pub video_duration: f64,
    #[serde(deserialize_with = "string_or_number")]
    pub post_id: String,
    #[serde(
        rename = "publisher_canister_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub publisher_canister_id: Option<Principal>,
    #[serde(rename = "nsfw_probability", skip_serializing_if = "Option::is_none")]
    pub nsfw_probability: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct VideoDurationWatchedPayloadV2 {
    #[schema(value_type = String)]
    pub publisher_user_id: Option<Principal>,
    #[schema(value_type = String)]
    pub user_id: Principal,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_logged_in: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_id: Option<String>,
    pub video_category: String,
    pub creator_category: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hashtag_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_nsfw: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_hot_or_not: Option<bool>,
    pub feed_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub like_count: Option<u64>,
    pub share_count: u64,
    pub percentage_watched: f64,
    pub absolute_watched: f64,
    pub video_duration: f64,
    #[serde(deserialize_with = "string_or_number")]
    pub post_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nsfw_probability: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct VideoStartedPayload {
    pub video_id: String,
    pub publisher_user_id: String,
    pub feature_name: String,
    pub like_count: u64,
    pub share_count: u64,
    pub view_count: u64,
    pub is_game_enabled: bool,
    pub game_type: String,
    pub is_nsfw: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
}

// Wrapper type for VideoStarted that implements SealedMetric
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct VideoStarted {
    payload: VideoStartedPayload,
}

impl SealedMetric for VideoStarted {
    fn tag(&self) -> String {
        "video_started".to_string()
    }

    fn user_id(&self) -> Option<String> {
        None
    }

    fn user_canister(&self) -> Option<Principal> {
        None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoViewedPayload {
    #[serde(rename = "publisher_user_id")]
    pub publisher_user_id: Option<Principal>,
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    pub is_logged_in: bool,
    #[serde(rename = "display_name", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "video_id", skip_serializing_if = "Option::is_none")]
    pub video_id: Option<String>,
    #[serde(rename = "video_category")]
    pub video_category: String,
    #[serde(rename = "creator_category")]
    pub creator_category: String,
    #[serde(rename = "hashtag_count", skip_serializing_if = "Option::is_none")]
    pub hashtag_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_nsfw: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_hot_or_not: Option<bool>,
    #[serde(rename = "feed_type")]
    pub feed_type: String,
    #[serde(rename = "view_count", skip_serializing_if = "Option::is_none")]
    pub view_count: Option<u64>,
    #[serde(rename = "like_count", skip_serializing_if = "Option::is_none")]
    pub like_count: Option<u64>,
    #[serde(rename = "share_count")]
    pub share_count: u64,
    #[serde(deserialize_with = "string_or_number")]
    pub post_id: String,
    #[serde(
        rename = "publisher_canister_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub publisher_canister_id: Option<Principal>,
    #[serde(rename = "nsfw_probability", skip_serializing_if = "Option::is_none")]
    pub nsfw_probability: Option<f64>,
}

// --------------------------------------------------
// Like / Share
// --------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LikeVideoPayloadV2 {
    #[schema(value_type = String)]
    pub publisher_user_id: Principal,
    #[schema(value_type = String)]
    pub user_id: Principal,
    pub is_logged_in: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub video_id: String,
    pub video_category: String,
    pub creator_category: String,
    pub hashtag_count: usize,
    #[serde(default)]
    pub is_nsfw: bool,
    #[serde(default)]
    pub is_hot_or_not: bool,
    pub feed_type: String,
    pub view_count: u64,
    pub like_count: u64,
    pub share_count: u64,
    #[serde(deserialize_with = "string_or_number")]
    pub post_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nsfw_probability: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareVideoPayload {
    #[serde(rename = "publisher_user_id")]
    pub publisher_user_id: Principal,
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    pub is_logged_in: bool,
    #[serde(rename = "display_name", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "video_id")]
    pub video_id: String,
    #[serde(rename = "video_category")]
    pub video_category: String,
    #[serde(rename = "creator_category")]
    pub creator_category: String,
    #[serde(rename = "hashtag_count")]
    pub hashtag_count: usize,
    #[serde(default)]
    pub is_nsfw: bool,
    #[serde(default)]
    pub is_hot_or_not: bool,
    #[serde(rename = "feed_type")]
    pub feed_type: String,
    #[serde(rename = "view_count")]
    pub view_count: u64,
    #[serde(rename = "like_count")]
    pub like_count: u64,
    #[serde(rename = "share_count")]
    pub share_count: u64,
    #[serde(rename = "nsfw_probability", skip_serializing_if = "Option::is_none")]
    pub nsfw_probability: Option<f64>,
}

// --------------------------------------------------
// Video Upload
// --------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoUploadInitiatedPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "display_name", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "creator_category")]
    pub creator_category: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoUploadUploadButtonClickedPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "display_name", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "creator_category")]
    pub creator_category: String,
    #[serde(rename = "hashtag_count")]
    pub hashtag_count: usize,
    #[serde(default)]
    pub is_nsfw: bool,
    #[serde(default)]
    pub is_hot_or_not: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoUploadVideoSelectedPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "display_name", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "creator_category")]
    pub creator_category: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoUploadUnsuccessfulPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "display_name", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "creator_category")]
    pub creator_category: String,
    #[serde(rename = "hashtag_count")]
    pub hashtag_count: usize,
    #[serde(default)]
    pub is_nsfw: bool,
    #[serde(default)]
    pub is_hot_or_not: bool,
    #[serde(rename = "fail_reason")]
    pub fail_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoUploadSuccessfulPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "publisher_user_id")]
    pub publisher_user_id: Principal,
    #[serde(rename = "display_name", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "creator_category")]
    pub creator_category: String,
    #[serde(rename = "hashtag_count")]
    pub hashtag_count: usize,
    #[serde(default)]
    pub is_nsfw: bool,
    #[serde(default)]
    pub is_hot_or_not: bool,
    #[serde(rename = "is_filter_used")]
    pub is_filter_used: bool,
    #[serde(rename = "video_id")]
    pub video_id: String,
    #[serde(deserialize_with = "string_or_number")]
    pub post_id: String,
    #[serde(rename = "country", skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(rename = "internalUrl", skip_serializing_if = "Option::is_none")]
    pub internal_url: Option<String>,
}

// --------------------------------------------------
// Refer & Share link
// --------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    pub is_logged_in: bool,
    #[serde(rename = "display_name", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "refer_location", skip_serializing_if = "Option::is_none")]
    pub refer_location: Option<String>,
}

#[allow(dead_code)]
pub type ReferShareLinkPayload = ReferPayload;

// --------------------------------------------------
// Auth events
// --------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginSuccessfulPayload {
    #[serde(rename = "login_method")]
    pub login_method: String,
    #[serde(rename = "user_id")]
    pub user_id: String,
    #[serde(rename = "canister_id")]
    pub canister_id: String,
    #[serde(rename = "is_new_user")]
    pub is_new_user: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginMethodSelectedPayload {
    #[serde(rename = "login_method")]
    pub login_method: String,
    #[serde(rename = "attempt_count")]
    pub attempt_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginJoinOverlayViewedPayload {
    #[serde(rename = "user_id_viewer")]
    pub user_id_viewer: Principal,
    #[serde(rename = "previous_event")]
    pub previous_event: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginCtaPayload {
    #[serde(rename = "previous_event")]
    pub previous_event: String,
    #[serde(rename = "cta_location")]
    pub cta_location: String,
}

// --------------------------------------------------
// Logout / Error
// --------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogoutClickedPayload {
    #[serde(rename = "user_id_viewer")]
    pub user_id_viewer: Principal,
    #[serde(rename = "display_name", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
}

#[allow(dead_code)]
pub type LogoutConfirmationPayload = LogoutClickedPayload;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEventPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "description")]
    pub description: String,
    #[serde(rename = "previous_event")]
    pub previous_event: String,
}

// --------------------------------------------------
// Profile / Tokens / Page visit
// --------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileViewVideoPayload {
    #[serde(rename = "publisher_user_id")]
    pub publisher_user_id: Principal,
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    pub is_logged_in: bool,
    #[serde(rename = "display_name", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "video_id")]
    pub video_id: String,
    #[serde(rename = "profile_feed")]
    pub profile_feed: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenCreationStartedPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "token_name")]
    pub token_name: String,
    #[serde(rename = "token_symbol")]
    pub token_symbol: String,
    #[serde(rename = "name")]
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokensTransferredPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "amount")]
    pub amount: String,
    #[serde(rename = "to")]
    pub to: Principal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageVisitPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    pub is_logged_in: bool,
    #[serde(rename = "pathname")]
    pub pathname: String,
}

// --------------------------------------------------
// Payments (cents / sats)
// --------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CentsAddedPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "is_loggedin")]
    pub is_logged_in: bool,
    #[serde(rename = "amount_added")]
    pub amount_added: u64,
    #[serde(rename = "payment_source")]
    pub payment_source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CentsWithdrawnPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "is_loggedin")]
    pub is_logged_in: bool,
    #[serde(rename = "amount_withdrawn")]
    pub amount_withdrawn: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SatsWithdrawnPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "is_loggedin")]
    pub is_logged_in: bool,
    #[serde(rename = "amount_withdrawn")]
    pub amount_withdrawn: f64,
}

// --------------------------------------------------
// Tournament / Leaderboard
// --------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TournamentStartedPayload {
    #[serde(rename = "tournament_id")]
    pub tournament_id: String,
    #[serde(rename = "prize_pool")]
    pub prize_pool: f64,
    #[serde(rename = "prize_token")]
    pub prize_token: String,
    #[serde(rename = "end_time")]
    pub end_time: i64,
    #[serde(rename = "metric_display_name")]
    pub metric_display_name: String,
    #[serde(rename = "metric_type")]
    pub metric_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TournamentEndedWinnerPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "tournament_id")]
    pub tournament_id: String,
    #[serde(rename = "rank")]
    pub rank: u32,
    #[serde(rename = "prize_amount")]
    pub prize_amount: u64,
    #[serde(rename = "prize_token")]
    pub prize_token: String,
    #[serde(rename = "total_participants")]
    pub total_participants: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RewardEarnedPayload {
    #[serde(rename = "creator_id")]
    pub creator_id: Principal,
    #[serde(rename = "video_id")]
    pub video_id: String,
    #[serde(rename = "milestone")]
    pub milestone: u64,
    #[serde(rename = "reward_btc")]
    pub reward_btc: f64,
    #[serde(rename = "reward_inr")]
    pub reward_inr: f64,
    #[serde(rename = "view_count")]
    pub view_count: u64,
    #[serde(rename = "timestamp")]
    pub timestamp: i64,
    #[serde(rename = "rewards_received_bs")]
    pub rewards_received_bs: bool,
    #[serde(rename = "reward_token")]
    pub reward_token: RewardTokenType,
}

// ----------------------------------------------------------------------------------
// Unified wrapper enum so callers can work with a single return type
// ----------------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowUserPayload {
    #[serde(rename = "follower_principal_id")]
    pub follower_principal_id: Principal,
    #[serde(rename = "follower_username")]
    pub follower_username: Option<String>,
    #[serde(rename = "followee_principal_id")]
    pub followee_principal_id: Principal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoApprovalPayload {
    pub video_id: String,
    pub post_id: String,
    pub canister_id: Option<String>,
    pub user_id: Principal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EventPayload {
    VideoDurationWatched(VideoDurationWatchedPayloadV2),
    VideoViewed(VideoViewedPayload),
    LikeVideo(LikeVideoPayloadV2),
    ShareVideo(ShareVideoPayload),
    VideoUploadInitiated(VideoUploadInitiatedPayload),
    VideoUploadUploadButtonClicked(VideoUploadUploadButtonClickedPayload),
    VideoUploadVideoSelected(VideoUploadVideoSelectedPayload),
    VideoUploadUnsuccessful(VideoUploadUnsuccessfulPayload),
    #[serde(serialize_with = "serialize_video_upload_successful")]
    VideoUploadSuccessful(VideoUploadSuccessfulPayload),
    Refer(ReferPayload),
    ReferShareLink(ReferPayload),
    LoginSuccessful(LoginSuccessfulPayload),
    LoginMethodSelected(LoginMethodSelectedPayload),
    LoginJoinOverlayViewed(LoginJoinOverlayViewedPayload),
    LoginCta(LoginCtaPayload),
    LogoutClicked(LogoutClickedPayload),
    LogoutConfirmation(LogoutClickedPayload),
    ErrorEvent(ErrorEventPayload),
    ProfileViewVideo(ProfileViewVideoPayload),
    TokenCreationStarted(TokenCreationStartedPayload),
    TokensTransferred(TokensTransferredPayload),
    PageVisit(PageVisitPayload),
    CentsAdded(CentsAddedPayload),
    CentsWithdrawn(CentsWithdrawnPayload),
    SatsWithdrawn(SatsWithdrawnPayload),
    TournamentStarted(TournamentStartedPayload),
    TournamentEndedWinner(TournamentEndedWinnerPayload),
    #[serde(serialize_with = "serialize_reward_earned")]
    RewardEarned(RewardEarnedPayload),
    FollowUser(FollowUserPayload),
    VideoApproved(VideoApprovalPayload),
    VideoDisapproved(VideoApprovalPayload),
}

fn serialize_reward_earned<S>(
    payload: &RewardEarnedPayload,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeStruct;
    let mut state = serializer.serialize_struct("RewardEarned", 1)?;

    let token_str = match payload.reward_token {
        RewardTokenType::Btc => "btc",
        RewardTokenType::Dolr => "dolr",
    };

    let url = format!(
        "rewardsReceived?token={}&reward_on=video_views&creator_id={}&video_id={}&milestone={}&reward_btc={}&reward_inr={}&view_count={}&timestamp={}&rewards_received_bs={}",
        token_str,
        payload.creator_id,
        payload.video_id,
        payload.milestone,
        payload.reward_btc,
        payload.reward_inr,
        payload.view_count,
        payload.timestamp,
        payload.rewards_received_bs
    );
    state.serialize_field("internalUrl", &url)?;
    state.end()
}

fn serialize_video_upload_successful<S>(
    payload: &VideoUploadSuccessfulPayload,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::Serialize;

    // Create a modified payload with the internal_url populated
    let mut modified_payload = payload.clone();

    let mut url = format!(
        "profile/videos?video_id={}&user_id={}&publisher_user_id={}&canister_id={}&post_id={}&creator_category={}&hashtag_count={}&is_NSFW={}&is_hotorNot={}&is_filter_used={}",
        modified_payload.video_id,
        modified_payload.user_id,
        modified_payload.publisher_user_id,
        modified_payload.canister_id,
        modified_payload.post_id,
        modified_payload.creator_category,
        modified_payload.hashtag_count,
        modified_payload.is_nsfw,
        modified_payload.is_hot_or_not,
        modified_payload.is_filter_used
    );

    if let Some(display_name) = &modified_payload.display_name {
        url.push_str(&format!("&display_name={}", display_name));
    }

    if let Some(country) = &modified_payload.country {
        url.push_str(&format!("&country={}", country));
    }

    modified_payload.internal_url = Some(url);

    // Serialize the modified payload with default serialization
    #[derive(Serialize)]
    struct VideoUploadSuccessfulHelper {
        #[serde(rename = "user_id")]
        user_id: Principal,
        #[serde(rename = "publisher_user_id")]
        publisher_user_id: Principal,
        #[serde(rename = "display_name", skip_serializing_if = "Option::is_none")]
        display_name: Option<String>,
        #[serde(rename = "canister_id")]
        canister_id: Principal,
        #[serde(rename = "creator_category")]
        creator_category: String,
        #[serde(rename = "hashtag_count")]
        hashtag_count: usize,
        is_nsfw: bool,
        is_hot_or_not: bool,
        #[serde(rename = "is_filter_used")]
        is_filter_used: bool,
        #[serde(rename = "video_id")]
        video_id: String,
        post_id: String,
        #[serde(rename = "country", skip_serializing_if = "Option::is_none")]
        country: Option<String>,
        #[serde(rename = "internalUrl", skip_serializing_if = "Option::is_none")]
        internal_url: Option<String>,
    }

    let helper = VideoUploadSuccessfulHelper {
        user_id: modified_payload.user_id,
        publisher_user_id: modified_payload.publisher_user_id,
        display_name: modified_payload.display_name,
        canister_id: modified_payload.canister_id,
        creator_category: modified_payload.creator_category,
        hashtag_count: modified_payload.hashtag_count,
        is_nsfw: modified_payload.is_nsfw,
        is_hot_or_not: modified_payload.is_hot_or_not,
        is_filter_used: modified_payload.is_filter_used,
        video_id: modified_payload.video_id,
        post_id: modified_payload.post_id,
        country: modified_payload.country,
        internal_url: modified_payload.internal_url,
    };

    helper.serialize(serializer)
}

// ----------------------------------------------------------------------------------
// Deserialization helper
// ----------------------------------------------------------------------------------

/// Given the raw `event_name` and a `serde_json::Value` representing the payload,
/// this function deserializes the value into the strongly-typed wrapper `EventPayload`.
///
/// # Errors
/// * Returns `serde_json::Error` if the event name is unknown OR the payload cannot
///   be deserialized into the expected structure.
impl EventPayload {
    // TODO: canister_id is used

    pub async fn send_notification(&self, app_state: &AppState) {
        match self {
            EventPayload::VideoUploadSuccessful(payload) => {
                let title = "Video Uploaded";
                let body = "Your video has been uploaded successfully";
                let publisher_user_id = payload.publisher_user_id;
                let canister_id = app_state
                    .get_individual_canister_by_user_principal(publisher_user_id)
                    .await
                    .unwrap();
                let notif_payload = SendNotificationReq {
                    notification: Some(NotificationPayload {
                        title: Some(title.to_string()),
                        body: Some(body.to_string()),
                        image: Some(
                            "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                        ),
                    }),
                    data: Some(json!({
                        "payload": serde_json::to_string(self).unwrap()
                    })),
                    android: Some(AndroidConfig {
                        notification: Some(AndroidNotification {
                            icon: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            image: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    webpush: Some(WebpushConfig {
                        fcm_options: Some(WebpushFcmOptions {
                            link: Some(format!(
                                "https://yral.com/hot-or-not/{}/{}",
                                payload.canister_id.to_text(),
                                payload.post_id
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    apns: Some(ApnsConfig {
                        fcm_options: Some(ApnsFcmOptions {
                            image: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            ..Default::default()
                        }),
                        payload: Some(json!({
                            "aps": {
                                "alert": {
                                    "title": title.to_string(),
                                    "body": body.to_string(),
                                },
                                "sound": "default",
                            },
                            "url": format!("https://yral.com/hot-or-not/{}/{}", canister_id.to_text(), payload.post_id)
                        })),
                        ..Default::default()
                    }),
                    ..Default::default()
                };

                app_state
                    .notification_client
                    .send_notification(notif_payload, payload.publisher_user_id)
                    .await;
            }
            EventPayload::LikeVideo(payload) => {
                let title = "Video Liked";
                let body = format!("{} liked your video", payload.user_id.to_text());
                let publisher_user_id = payload.publisher_user_id;
                let canister_id = app_state
                    .get_individual_canister_by_user_principal(publisher_user_id)
                    .await
                    .unwrap();
                let notif_payload = SendNotificationReq {
                    notification: Some(NotificationPayload {
                        title: Some(title.to_string()),
                        body: Some(body.to_string()),
                        image: Some(
                            "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                        ),
                    }),
                    data: Some(json!({
                        "payload": serde_json::to_string(self).unwrap()
                    })),
                    android: Some(AndroidConfig {
                        notification: Some(AndroidNotification {
                            icon: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            image: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    webpush: Some(WebpushConfig {
                        fcm_options: Some(WebpushFcmOptions {
                            link: Some(format!(
                                "https://yral.com/hot-or-not/{}/{}",
                                canister_id.to_text(),
                                payload.post_id
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    apns: Some(ApnsConfig {
                        fcm_options: Some(ApnsFcmOptions {
                            image: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            ..Default::default()
                        }),
                        payload: Some(json!({
                            "aps": {
                                "alert": {
                                    "title": title.to_string(),
                                    "body": body.to_string(),
                                },
                                "sound": "default",
                            },
                            "url": format!("https://yral.com/hot-or-not/{}/{}", canister_id.to_text(), payload.post_id)
                        })),
                        ..Default::default()
                    }),
                    ..Default::default()
                };

                app_state
                    .notification_client
                    .send_notification(notif_payload, payload.publisher_user_id)
                    .await;
            }

            EventPayload::TournamentStarted(payload) => {
                // Tournament start notifications would be sent to all users
                // This should be handled by a batch process, not individual notifications
                // For now, log the event
                log::info!(
                    "Tournament started: {} with prize pool {} {}",
                    payload.tournament_id,
                    payload.prize_pool,
                    payload.prize_token
                );
            }

            EventPayload::TournamentEndedWinner(payload) => {
                let title = format!("Congratulations! You won rank #{}!", payload.rank);
                let body = format!(
                    "You ranked #{}! You’ve won {} {} in the tournament. Check the leaderboard now!",
                    payload.rank, payload.prize_amount, payload.prize_token
                );

                let notif_payload = SendNotificationReq {
                    notification: Some(NotificationPayload {
                        title: Some(title.to_string()),
                        body: Some(body.to_string()),
                        image: Some(
                            "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                        ),
                    }),
                    data: Some(json!({
                        "payload": serde_json::to_string(self).unwrap()
                    })),
                    android: None,
                    webpush: Some(WebpushConfig {
                        fcm_options: Some(WebpushFcmOptions {
                            link: Some(format!(
                                "https://yral.com/leaderboard/results/{}",
                                payload.tournament_id
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    apns: None,
                    ..Default::default()
                };

                app_state
                    .notification_client
                    .send_notification(notif_payload, payload.user_id)
                    .await;
            }

            EventPayload::RewardEarned(payload) => {
                let (title, body) = match payload.reward_token {
                    RewardTokenType::Dolr => (
                        "DOLR Credited",
                        "Congrats! Your video views have earned you DOLR. See your balance in the wallet."
                    ),
                    RewardTokenType::Btc => (
                        "Bitcoin Credited",
                        "Congrats! Your video views have earned you Bitcoin. See your balance in the wallet."
                    ),
                };

                let notif_payload = SendNotificationReq {
                    notification: Some(NotificationPayload {
                        title: Some(title.to_string()),
                        body: Some(body.to_string()),
                        image: Some(
                            "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                        ),
                    }),
                    data: Some(json!({
                        "payload": serde_json::to_string(self).unwrap()
                    })),
                    android: Some(AndroidConfig {
                        notification: Some(AndroidNotification {
                            icon: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            image: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    webpush: Some(WebpushConfig {
                        fcm_options: Some(WebpushFcmOptions {
                            link: Some("https://link.yral.com/dJqgFEnM6Wb".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    apns: Some(ApnsConfig {
                        fcm_options: Some(ApnsFcmOptions {
                            image: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            ..Default::default()
                        }),
                        payload: Some(json!({
                            "aps": {
                                "alert": {
                                    "title": title.to_string(),
                                    "body": body.to_string(),
                                },
                                "sound": "default",
                            },
                            "url": "https://link.yral.com/dJqgFEnM6Wb"
                        })),
                        ..Default::default()
                    }),
                    ..Default::default()
                };

                app_state
                    .notification_client
                    .send_notification(notif_payload, payload.creator_id)
                    .await;
            }

            EventPayload::FollowUser(payload) => {
                let title = "New Follower";
                let body = match &payload.follower_username {
                    Some(username) => format!("{} started following you", username),
                    None => "Someone started following you".to_string(),
                };
                let followee_principal_id = payload.followee_principal_id;

                let profile_url = format!(
                    "https://yral.com/profile/{}/posts",
                    payload.follower_principal_id.to_text()
                );

                log::debug!(
                    "Sending follow notification to user {} from {}",
                    followee_principal_id,
                    payload
                        .follower_username
                        .as_deref()
                        .unwrap_or(&payload.follower_principal_id.to_text())
                );

                let notif_payload = SendNotificationReq {
                    notification: Some(NotificationPayload {
                        title: Some(title.to_string()),
                        body: Some(body.clone()),
                        image: Some(
                            "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                        ),
                    }),
                    data: Some(json!({
                        "payload": serde_json::to_string(self).unwrap()
                    })),
                    android: Some(AndroidConfig {
                        notification: Some(AndroidNotification {
                            icon: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            image: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    webpush: Some(WebpushConfig {
                        fcm_options: Some(WebpushFcmOptions {
                            link: Some(profile_url.clone()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    apns: Some(ApnsConfig {
                        fcm_options: Some(ApnsFcmOptions {
                            image: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            ..Default::default()
                        }),
                        payload: Some(json!({
                            "aps": {
                                "alert": {
                                    "title": title.to_string(),
                                    "body": body,
                                },
                                "sound": "default",
                            },
                            "url": profile_url
                        })),
                        ..Default::default()
                    }),
                    ..Default::default()
                };

                app_state
                    .notification_client
                    .send_notification(notif_payload, followee_principal_id)
                    .await;
            }

            EventPayload::VideoApproved(payload) => {
                let title = "Video Approved";
                let body = "Your video has been approved and is now live!";

                let video_url = payload
                    .canister_id
                    .as_ref()
                    .map(|cid| format!("https://yral.com/hot-or-not/{}/{}", cid, payload.post_id))
                    .unwrap_or_else(|| "https://yral.com".to_string());

                let notif_payload = SendNotificationReq {
                    notification: Some(NotificationPayload {
                        title: Some(title.to_string()),
                        body: Some(body.to_string()),
                        image: Some(
                            "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                        ),
                    }),
                    data: Some(json!({
                        "type": "video_approved",
                        "video_id": payload.video_id,
                        "post_id": payload.post_id
                    })),
                    android: Some(AndroidConfig {
                        notification: Some(AndroidNotification {
                            icon: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            image: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    webpush: Some(WebpushConfig {
                        fcm_options: Some(WebpushFcmOptions {
                            link: Some(video_url.clone()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    apns: Some(ApnsConfig {
                        fcm_options: Some(ApnsFcmOptions {
                            image: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            ..Default::default()
                        }),
                        payload: Some(json!({
                            "aps": {
                                "alert": {
                                    "title": title.to_string(),
                                    "body": body.to_string(),
                                },
                                "sound": "default",
                            },
                            "url": video_url
                        })),
                        ..Default::default()
                    }),
                    ..Default::default()
                };

                app_state
                    .notification_client
                    .send_notification(notif_payload, payload.user_id)
                    .await;
            }

            EventPayload::VideoDisapproved(payload) => {
                let title = "Video Not Approved";
                let body = "Your video was not approved for publication.";

                let notif_payload = SendNotificationReq {
                    notification: Some(NotificationPayload {
                        title: Some(title.to_string()),
                        body: Some(body.to_string()),
                        image: Some(
                            "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                        ),
                    }),
                    data: Some(json!({
                        "type": "video_disapproved",
                        "video_id": payload.video_id,
                        "post_id": payload.post_id
                    })),
                    android: Some(AndroidConfig {
                        notification: Some(AndroidNotification {
                            icon: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            image: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    webpush: Some(WebpushConfig {
                        fcm_options: Some(WebpushFcmOptions {
                            link: Some("https://yral.com".to_string()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    apns: Some(ApnsConfig {
                        fcm_options: Some(ApnsFcmOptions {
                            image: Some(
                                "https://yral.com/img/yral/android-chrome-384x384.png".to_string(),
                            ),
                            ..Default::default()
                        }),
                        payload: Some(json!({
                            "aps": {
                                "alert": {
                                    "title": title.to_string(),
                                    "body": body.to_string(),
                                },
                                "sound": "default",
                            },
                            "url": "https://yral.com"
                        })),
                        ..Default::default()
                    }),
                    ..Default::default()
                };

                app_state
                    .notification_client
                    .send_notification(notif_payload, payload.user_id)
                    .await;
            }

            _ => {}
        }
    }
}

pub fn deserialize_event_payload(
    event_name: &str,
    value: Value,
) -> Result<EventPayload, serde_json::Error> {
    match event_name {
        "video_duration_watched" => Ok(EventPayload::VideoDurationWatched(serde_json::from_value(
            value,
        )?)),
        "video_viewed" => Ok(EventPayload::VideoViewed(serde_json::from_value(value)?)),
        "like_video" => Ok(EventPayload::LikeVideo(serde_json::from_value(value)?)),
        "share_video" => Ok(EventPayload::ShareVideo(serde_json::from_value(value)?)),
        "video_upload_initiated" => Ok(EventPayload::VideoUploadInitiated(serde_json::from_value(
            value,
        )?)),
        "video_upload_upload_button_clicked" => Ok(EventPayload::VideoUploadUploadButtonClicked(
            serde_json::from_value(value)?,
        )),
        "video_upload_video_selected" => Ok(EventPayload::VideoUploadVideoSelected(
            serde_json::from_value(value)?,
        )),
        "video_upload_unsuccessful" => Ok(EventPayload::VideoUploadUnsuccessful(
            serde_json::from_value(value)?,
        )),
        "video_upload_successful" => Ok(EventPayload::VideoUploadSuccessful(
            serde_json::from_value(value)?,
        )),
        "refer" => Ok(EventPayload::Refer(serde_json::from_value(value)?)),
        "refer_share_link" => Ok(EventPayload::ReferShareLink(serde_json::from_value(value)?)),
        "login_successful" => Ok(EventPayload::LoginSuccessful(serde_json::from_value(
            value,
        )?)),
        "login_method_selected" => Ok(EventPayload::LoginMethodSelected(serde_json::from_value(
            value,
        )?)),
        "login_join_overlay_viewed" => Ok(EventPayload::LoginJoinOverlayViewed(
            serde_json::from_value(value)?,
        )),
        "login_cta" => Ok(EventPayload::LoginCta(serde_json::from_value(value)?)),
        "logout_clicked" => Ok(EventPayload::LogoutClicked(serde_json::from_value(value)?)),
        "logout_confirmation" => Ok(EventPayload::LogoutConfirmation(serde_json::from_value(
            value,
        )?)),
        "error_event" => Ok(EventPayload::ErrorEvent(serde_json::from_value(value)?)),
        "profile_view_video" => Ok(EventPayload::ProfileViewVideo(serde_json::from_value(
            value,
        )?)),
        "token_creation_started" => Ok(EventPayload::TokenCreationStarted(serde_json::from_value(
            value,
        )?)),
        "tokens_transferred" => Ok(EventPayload::TokensTransferred(serde_json::from_value(
            value,
        )?)),
        "yral_page_visit" => Ok(EventPayload::PageVisit(serde_json::from_value(value)?)),
        "cents_added" => Ok(EventPayload::CentsAdded(serde_json::from_value(value)?)),
        "cents_withdrawn" => Ok(EventPayload::CentsWithdrawn(serde_json::from_value(value)?)),
        "sats_withdrawn" => Ok(EventPayload::SatsWithdrawn(serde_json::from_value(value)?)),
        "tournament_started" => Ok(EventPayload::TournamentStarted(serde_json::from_value(
            value,
        )?)),
        "tournament_ended_winner" => Ok(EventPayload::TournamentEndedWinner(
            serde_json::from_value(value)?,
        )),
        "reward_earned" => Ok(EventPayload::RewardEarned(serde_json::from_value(value)?)),
        "follow_user" => Ok(EventPayload::FollowUser(serde_json::from_value(value)?)),
        "video_approved" => Ok(EventPayload::VideoApproved(serde_json::from_value(value)?)),
        "video_disapproved" => Ok(EventPayload::VideoDisapproved(serde_json::from_value(
            value,
        )?)),
        _ => Err(serde_json::Error::unknown_field(event_name, &[])),
    }
}

#[test]
fn test_data_payload_serialization() {
    let payload = VideoUploadSuccessfulPayload {
        canister_id: Principal::from_text("mlj75-eyaaa-aaaaa-qbn5q-cai").unwrap(),
        post_id: "123".to_string(),
        publisher_user_id: Principal::from_text("mlj75-eyaaa-aaaaa-qbn5q-cai").unwrap(),
        user_id: Principal::from_text("mlj75-eyaaa-aaaaa-qbn5q-cai").unwrap(),
        display_name: None,
        creator_category: "test".to_string(),
        hashtag_count: 0,
        is_nsfw: false,
        is_hot_or_not: false,
        is_filter_used: false,
        video_id: "test".to_string(),
        country: None,
        internal_url: None,
    };

    let data = EventPayload::VideoUploadSuccessful(payload.clone());

    let notif_payload = SendNotificationReq {
        notification: Some(NotificationPayload {
            title: Some("test".to_string()),
            body: Some("test".to_string()),
            image: Some("https://yral.com/img/yral/android-chrome-384x384.png".to_string()),
        }),
        data: Some(json!({
            "payload": serde_json::to_string(&data).unwrap()
        })),
        android: Some(AndroidConfig {
            notification: Some(AndroidNotification {
                icon: Some("https://yral.com/img/yral/android-chrome-384x384.png".to_string()),
                image: Some("https://yral.com/img/yral/android-chrome-384x384.png".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        webpush: Some(WebpushConfig {
            fcm_options: Some(WebpushFcmOptions {
                link: Some(format!(
                    "https://yral.com/hot-or-not/{}/{}",
                    payload.canister_id.to_text(),
                    payload.post_id
                )),
                ..Default::default()
            }),
            ..Default::default()
        }),
        apns: Some(ApnsConfig {
            fcm_options: Some(ApnsFcmOptions {
                image: Some("https://yral.com/img/yral/android-chrome-384x384.png".to_string()),
                ..Default::default()
            }),
            payload: Some(json!({
                "aps": {
                    "alert": {
                        "title": "test".to_string(),
                        "body": "test".to_string(),
                    },
                    "sound": "default",
                },
                "url": format!("https://yral.com/hot-or-not/{}/{}", payload.canister_id.to_text(), payload.post_id)
            })),
            ..Default::default()
        }),
        ..Default::default()
    };

    let stringed_payload = serde_json::to_string(&notif_payload).unwrap();

    let deserialized_payload: SendNotificationReq =
        serde_json::from_str(&stringed_payload).unwrap();

    assert!(deserialized_payload.clone().data.unwrap()["payload"].is_string());

    println!("{:?}", deserialized_payload.data.unwrap()["payload"]);
}
