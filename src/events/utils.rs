use crate::events::types::{
    LikeVideoPayloadV2, LikeVideoPayloadV3, VideoDurationWatchedPayloadV2,
    VideoDurationWatchedPayloadV3,
};
use candid::Principal;
use serde_json;

#[derive(Debug)]
pub struct SuccessHistoryParams {
    pub publisher_user_id: String,
    pub user_id: String,
    pub nsfw_probability: f64,
    pub post_id: u64,
    pub video_id: String,
    pub percent_watched: f64,
}

#[derive(Debug)]
pub struct SuccessHistoryParamsV2 {
    pub publisher_user_id: String,
    pub user_id: String,
    pub nsfw_probability: f64,
    pub post_id: String, // Changed from u64 to String
    pub video_id: String,
    pub percent_watched: f64,
}

pub fn parse_success_history_params(
    event_type: &str,
    params_str: &str,
) -> Result<Option<SuccessHistoryParams>, String> {
    match event_type {
        "video_duration_watched" => {
            let params: VideoDurationWatchedPayloadV2 = serde_json::from_str(params_str)
                .map_err(|e| format!("Failed to parse video_duration_watched params: {e:?}"))?;

            let percent_watched = params.percentage_watched;
            if percent_watched < 30.0 {
                return Ok(None);
            }

            Ok(Some(SuccessHistoryParams {
                publisher_user_id: params
                    .publisher_user_id
                    .map(|p| p.to_string())
                    .unwrap_or_default(),
                user_id: params.user_id.to_string(),
                nsfw_probability: params.nsfw_probability.unwrap_or_default(),
                post_id: params.post_id.unwrap_or_default(),
                video_id: params.video_id.unwrap_or_default(),
                percent_watched,
            }))
        }
        "like_video" => {
            let params: LikeVideoPayloadV2 = serde_json::from_str(params_str)
                .map_err(|e| format!("Failed to parse like_video params: {e:?}"))?;

            Ok(Some(SuccessHistoryParams {
                publisher_user_id: params.publisher_user_id.to_string(),
                user_id: params.user_id.to_string(),
                nsfw_probability: params.nsfw_probability.unwrap_or_default(),
                post_id: params.post_id,
                video_id: params.video_id,
                percent_watched: 0.0, // No percent_watched for likes
            }))
        }
        _ => Err(format!(
            "Unexpected event type in parse_success_history_params: {event_type}"
        )),
    }
}

pub fn parse_success_history_params_v2(
    event_type: &str,
    params_str: &str,
) -> Result<Option<SuccessHistoryParamsV2>, String> {
    match event_type {
        "video_duration_watched" => {
            // Try V3 first, fallback to V2
            if let Ok(params) = serde_json::from_str::<VideoDurationWatchedPayloadV3>(params_str) {
                let percent_watched = params.percentage_watched;
                if percent_watched < 30.0 {
                    return Ok(None);
                }

                Ok(Some(SuccessHistoryParamsV2 {
                    publisher_user_id: params
                        .publisher_user_id
                        .unwrap_or_else(|| Principal::anonymous())
                        .to_string(),
                    user_id: params.user_id.to_string(),
                    nsfw_probability: params.nsfw_probability.unwrap_or_default(),
                    post_id: params.post_id.unwrap_or_else(|| "0".to_string()),
                    video_id: params.video_id.unwrap_or_default(),
                    percent_watched,
                }))
            } else {
                // Fallback to V2 and convert
                let params: VideoDurationWatchedPayloadV2 = serde_json::from_str(params_str)
                    .map_err(|e| format!("Failed to parse video_duration_watched params: {e:?}"))?;

                let percent_watched = params.percentage_watched;
                if percent_watched < 30.0 {
                    return Ok(None);
                }

                Ok(Some(SuccessHistoryParamsV2 {
                    publisher_user_id: params
                        .publisher_user_id
                        .map(|p| p.to_string())
                        .unwrap_or_default(),
                    user_id: params.user_id.to_string(),
                    nsfw_probability: params.nsfw_probability.unwrap_or_default(),
                    post_id: params.post_id.unwrap_or_default().to_string(),
                    video_id: params.video_id.unwrap_or_default(),
                    percent_watched,
                }))
            }
        }
        "like_video" => {
            // Try V3 first, fallback to V2
            if let Ok(params) = serde_json::from_str::<LikeVideoPayloadV3>(params_str) {
                Ok(Some(SuccessHistoryParamsV2 {
                    publisher_user_id: params.publisher_user_id.to_string(),
                    user_id: params.user_id.to_string(),
                    nsfw_probability: params.nsfw_probability.unwrap_or_default(),
                    post_id: params.post_id,
                    video_id: params.video_id,
                    percent_watched: 0.0, // No percent_watched for likes
                }))
            } else {
                // Fallback to V2 and convert
                let params: LikeVideoPayloadV2 = serde_json::from_str(params_str)
                    .map_err(|e| format!("Failed to parse like_video params: {e:?}"))?;

                Ok(Some(SuccessHistoryParamsV2 {
                    publisher_user_id: params.publisher_user_id.to_string(),
                    user_id: params.user_id.to_string(),
                    nsfw_probability: params.nsfw_probability.unwrap_or_default(),
                    post_id: params.post_id.to_string(),
                    video_id: params.video_id,
                    percent_watched: 0.0, // No percent_watched for likes
                }))
            }
        }
        _ => Err(format!(
            "Unexpected event type in parse_success_history_params_v2: {event_type}"
        )),
    }
}
