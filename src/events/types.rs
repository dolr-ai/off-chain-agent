use candid::Principal;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use utoipa::ToSchema;
use yral_metrics::metrics::{
    like_video::LikeVideo, sealed_metric::SealedMetric,
    video_duration_watched::VideoDurationWatched, video_watched::VideoWatched,
};

#[derive(Serialize, Clone, Debug, ToSchema)]
#[serde(tag = "event")]
pub enum AnalyticsEvent {
    VideoWatched(VideoWatched),
    VideoDurationWatched(VideoDurationWatched),
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
            Some("LikeVideo") => {
                let like_video: LikeVideo =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(AnalyticsEvent::LikeVideo(like_video))
            }
            Some(event_type) => Err(serde::de::Error::custom(format!(
                "Unknown event type: {}",
                event_type
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
            AnalyticsEvent::LikeVideo(event) => event.$method(),
        }
    };
    // Overload for methods that need serde_json::to_value
    ($self:ident, $method:ident, to_value) => {
        match $self {
            AnalyticsEvent::VideoWatched(event) => serde_json::to_value(event).unwrap(),
            AnalyticsEvent::VideoDurationWatched(event) => serde_json::to_value(event).unwrap(),
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
    #[serde(rename = "is_loggedIn", skip_serializing_if = "Option::is_none")]
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
    #[serde(rename = "is_NSFW", skip_serializing_if = "Option::is_none")]
    pub is_nsfw: Option<bool>,
    #[serde(rename = "is_hotorNot", skip_serializing_if = "Option::is_none")]
    pub is_hotor_not: Option<bool>,
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
    #[serde(rename = "post_id", skip_serializing_if = "Option::is_none")]
    pub post_id: Option<u64>,
    #[serde(
        rename = "publisher_canister_id",
        skip_serializing_if = "Option::is_none"
    )]
    pub publisher_canister_id: Option<Principal>,
    #[serde(rename = "nsfw_probability", skip_serializing_if = "Option::is_none")]
    pub nsfw_probability: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoViewedPayload {
    #[serde(rename = "publisher_user_id")]
    pub publisher_user_id: Option<Principal>,
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "is_loggedIn")]
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
    #[serde(rename = "is_NSFW", skip_serializing_if = "Option::is_none")]
    pub is_nsfw: Option<bool>,
    #[serde(rename = "is_hotorNot", skip_serializing_if = "Option::is_none")]
    pub is_hotor_not: Option<bool>,
    #[serde(rename = "feed_type")]
    pub feed_type: String,
    #[serde(rename = "view_count", skip_serializing_if = "Option::is_none")]
    pub view_count: Option<u64>,
    #[serde(rename = "like_count", skip_serializing_if = "Option::is_none")]
    pub like_count: Option<u64>,
    #[serde(rename = "share_count")]
    pub share_count: u64,
    #[serde(rename = "post_id", skip_serializing_if = "Option::is_none")]
    pub post_id: Option<u64>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LikeVideoPayload {
    #[serde(rename = "publisher_user_id")]
    pub publisher_user_id: Principal,
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "is_loggedIn")]
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
    #[serde(rename = "is_NSFW")]
    pub is_nsfw: bool,
    #[serde(rename = "is_hotorNot")]
    pub is_hotor_not: bool,
    #[serde(rename = "feed_type")]
    pub feed_type: String,
    #[serde(rename = "view_count")]
    pub view_count: u64,
    #[serde(rename = "like_count")]
    pub like_count: u64,
    #[serde(rename = "share_count")]
    pub share_count: u64,
    #[serde(rename = "post_id")]
    pub post_id: u64,
    #[serde(rename = "publisher_canister_id")]
    pub publisher_canister_id: Principal,
    #[serde(rename = "nsfw_probability", skip_serializing_if = "Option::is_none")]
    pub nsfw_probability: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareVideoPayload {
    #[serde(rename = "publisher_user_id")]
    pub publisher_user_id: Principal,
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "is_loggedIn")]
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
    #[serde(rename = "is_NSFW")]
    pub is_nsfw: bool,
    #[serde(rename = "is_hotorNot")]
    pub is_hotor_not: bool,
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
    #[serde(rename = "is_NSFW")]
    pub is_nsfw: bool,
    #[serde(rename = "is_hotorNot")]
    pub is_hotor_not: bool,
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
    #[serde(rename = "is_NSFW")]
    pub is_nsfw: bool,
    #[serde(rename = "is_hotorNot")]
    pub is_hotor_not: bool,
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
    #[serde(rename = "is_NSFW")]
    pub is_nsfw: bool,
    #[serde(rename = "is_hotorNot")]
    pub is_hotor_not: bool,
    #[serde(rename = "is_filter_used")]
    pub is_filter_used: bool,
    #[serde(rename = "video_id")]
    pub video_id: String,
    #[serde(rename = "post_id")]
    pub post_id: u64,
    #[serde(rename = "country", skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
}

// --------------------------------------------------
// Refer & Share link
// --------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferPayload {
    #[serde(rename = "user_id")]
    pub user_id: Principal,
    #[serde(rename = "is_loggedIn")]
    pub is_logged_in: bool,
    #[serde(rename = "display_name", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(rename = "canister_id")]
    pub canister_id: Principal,
    #[serde(rename = "refer_location", skip_serializing_if = "Option::is_none")]
    pub refer_location: Option<String>,
}

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
    #[serde(rename = "is_loggedIn")]
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
    #[serde(rename = "is_loggedIn")]
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
