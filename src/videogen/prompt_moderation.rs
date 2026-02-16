use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use videogen_common::{ImageData, VideoGenError};

const GEMINI_API_URL: &str =
    "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent";

const SYSTEM_PROMPT: &str = r#"You are a content moderation system for a video generation platform.
Analyze the given prompt (and image if provided) and determine if it contains NSFW (Not Safe For Work) content.

Flag as NSFW if the prompt or image contains:
- Sexually explicit or suggestive content
- Nudity or pornographic descriptions
- Extreme graphic violence or gore
- Hate speech, slurs, or discriminatory content
- Drug manufacturing or illegal activity instructions
- Child exploitation of any kind

Do NOT flag as NSFW:
- Artistic or cinematic violence (action scenes, fight choreography)
- Medical or educational content
- Mild language or innuendo
- Fantasy or sci-fi violence (battles, explosions)
- Swimwear, athletic wear, or fashion content"#;

#[derive(Serialize)]
struct GeminiRequest {
    system_instruction: GeminiContent,
    contents: Vec<GeminiContent>,
    generation_config: GenerationConfig,
}

#[derive(Serialize)]
struct GeminiContent {
    role: String,
    parts: Vec<GeminiPart>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum GeminiPart {
    Text { text: String },
    InlineData { inline_data: InlineData },
    FileData { file_data: FileData },
}

#[derive(Serialize)]
struct InlineData {
    mime_type: String,
    data: String,
}

#[derive(Serialize)]
struct FileData {
    mime_type: String,
    file_uri: String,
}

#[derive(Serialize)]
struct GenerationConfig {
    #[serde(rename = "responseMimeType")]
    response_mime_type: String,
    #[serde(rename = "responseSchema")]
    response_schema: serde_json::Value,
}

#[derive(Deserialize)]
struct GeminiResponse {
    candidates: Option<Vec<GeminiCandidate>>,
}

#[derive(Deserialize)]
struct GeminiCandidate {
    content: GeminiCandidateContent,
}

#[derive(Deserialize)]
struct GeminiCandidateContent {
    parts: Vec<GeminiResponsePart>,
}

#[derive(Deserialize)]
struct GeminiResponsePart {
    text: Option<String>,
}

#[derive(Deserialize, Debug)]
struct ModerationResult {
    is_nsfw: bool,
    reason: String,
}

/// Check if a prompt (and optional image) contains NSFW content using Gemini 2.5 Flash.
/// For images: if Gemini's safety filters block the request, we treat it as NSFW.
/// Fails open for non-safety errors: if the API call fails for other reasons, the prompt is allowed through.
pub async fn check_prompt_nsfw(
    prompt: &str,
    image: Option<&ImageData>,
) -> Result<(), (StatusCode, Json<VideoGenError>)> {
    // Skip if both prompt and image are empty
    if prompt.is_empty() && image.is_none() {
        return Ok(());
    }

    let api_key = match std::env::var("PROMPT_MODERATION_GEMINI_API_KEY") {
        Ok(key) => key,
        Err(_) => {
            log::warn!("PROMPT_MODERATION_GEMINI_API_KEY not set, skipping prompt moderation");
            return Ok(());
        }
    };

    // Build user parts: prompt text + optional image
    let mut parts = Vec::new();

    if !prompt.is_empty() {
        parts.push(GeminiPart::Text {
            text: prompt.to_string(),
        });
    }

    // Add image if provided
    if let Some(img) = image {
        match img {
            ImageData::Base64(input) => {
                parts.push(GeminiPart::InlineData {
                    inline_data: InlineData {
                        mime_type: input.mime_type.clone(),
                        data: input.data.clone(),
                    },
                });
            }
            ImageData::Url(url) => {
                let mime_type = guess_mime_type(url);
                parts.push(GeminiPart::FileData {
                    file_data: FileData {
                        mime_type,
                        file_uri: url.clone(),
                    },
                });
            }
        }
    }

    let request = GeminiRequest {
        system_instruction: GeminiContent {
            role: "system".to_string(),
            parts: vec![GeminiPart::Text {
                text: SYSTEM_PROMPT.to_string(),
            }],
        },
        contents: vec![GeminiContent {
            role: "user".to_string(),
            parts,
        }],
        generation_config: GenerationConfig {
            response_mime_type: "application/json".to_string(),
            response_schema: serde_json::json!({
                "type": "OBJECT",
                "properties": {
                    "is_nsfw": {
                        "type": "BOOLEAN",
                        "description": "Whether the prompt or image contains NSFW/explicit content"
                    },
                    "reason": {
                        "type": "STRING",
                        "description": "Brief reason if flagged, or 'clean' if not"
                    }
                },
                "required": ["is_nsfw", "reason"]
            }),
        },
    };

    let client = reqwest::Client::new();
    let url = format!("{GEMINI_API_URL}?key={api_key}");

    let response = match client.post(&url).json(&request).send().await {
        Ok(resp) => resp,
        Err(e) => {
            log::warn!("Gemini API call failed, allowing prompt through: {e}");
            return Ok(());
        }
    };

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();

        // If Gemini blocks due to safety (usually 400 with safety reason), treat as NSFW when image is present
        if image.is_some()
            && (status.as_u16() == 400 || status.as_u16() == 429)
            && body.contains("SAFETY")
        {
            log::info!("Gemini blocked request due to safety filters (image likely NSFW)");
            return Err((
                StatusCode::BAD_REQUEST,
                Json(VideoGenError::InvalidInput(
                    "Image rejected: content violates safety guidelines".to_string(),
                )),
            ));
        }

        log::warn!("Gemini API returned {status}, allowing prompt through: {body}");
        return Ok(());
    }

    let gemini_response: GeminiResponse = match response.json().await {
        Ok(resp) => resp,
        Err(e) => {
            log::warn!("Failed to parse Gemini response, allowing prompt through: {e}");
            return Ok(());
        }
    };

    // Extract the text from the response
    let text = gemini_response
        .candidates
        .as_ref()
        .and_then(|c| c.first())
        .and_then(|c| c.content.parts.first())
        .and_then(|p| p.text.as_ref());

    let Some(text) = text else {
        log::warn!("No text in Gemini response, allowing prompt through");
        return Ok(());
    };

    let moderation: ModerationResult = match serde_json::from_str(text) {
        Ok(result) => result,
        Err(e) => {
            log::warn!("Failed to parse moderation result, allowing prompt through: {e}");
            return Ok(());
        }
    };

    if moderation.is_nsfw {
        log::info!(
            "Prompt rejected by moderation: {prompt} (reason: {})",
            moderation.reason
        );
        return Err((
            StatusCode::BAD_REQUEST,
            Json(VideoGenError::InvalidInput(format!(
                "Prompt rejected: {}",
                moderation.reason
            ))),
        ));
    }

    Ok(())
}

/// Guess MIME type from URL extension
fn guess_mime_type(url: &str) -> String {
    let lower = url.to_lowercase();
    if lower.contains(".png") {
        "image/png"
    } else if lower.contains(".gif") {
        "image/gif"
    } else if lower.contains(".webp") {
        "image/webp"
    } else {
        "image/jpeg"
    }
    .to_string()
}
