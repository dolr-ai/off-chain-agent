use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use videogen_common::VideoGenError;

const GEMINI_API_URL: &str =
    "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent";

const SYSTEM_PROMPT: &str = r#"You are a content moderation system for a video generation platform.
Analyze the given prompt and determine if it contains NSFW (Not Safe For Work) content.

Flag as NSFW if the prompt contains:
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
    contents: Vec<GeminiContent>,
    generation_config: GenerationConfig,
}

#[derive(Serialize)]
struct GeminiContent {
    role: String,
    parts: Vec<GeminiPart>,
}

#[derive(Serialize)]
struct GeminiPart {
    text: String,
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

/// Check if a prompt contains NSFW content using Gemini 2.5 Flash structured outputs.
/// Fails open: if the API call fails, the prompt is allowed through with a warning log.
pub async fn check_prompt_nsfw(
    prompt: &str,
) -> Result<(), (StatusCode, Json<VideoGenError>)> {
    // Skip empty prompts (e.g. image-only generation)
    if prompt.is_empty() {
        return Ok(());
    }

    let api_key = match std::env::var("GEMINI_API_KEY") {
        Ok(key) => key,
        Err(_) => {
            log::warn!("GEMINI_API_KEY not set, skipping prompt moderation");
            return Ok(());
        }
    };

    let request = GeminiRequest {
        contents: vec![
            GeminiContent {
                role: "user".to_string(),
                parts: vec![GeminiPart {
                    text: format!(
                        "{SYSTEM_PROMPT}\n\nAnalyze this video generation prompt:\n\n{prompt}"
                    ),
                }],
            },
        ],
        generation_config: GenerationConfig {
            response_mime_type: "application/json".to_string(),
            response_schema: serde_json::json!({
                "type": "OBJECT",
                "properties": {
                    "is_nsfw": {
                        "type": "BOOLEAN",
                        "description": "Whether the prompt contains NSFW/explicit content"
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
            "Prompt rejected by moderation: {} (reason: {})",
            &prompt[..prompt.len().min(60)],
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
