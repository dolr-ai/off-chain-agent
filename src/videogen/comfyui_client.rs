use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use tracing::info;
use videogen_common::VideoGenError;

/// Configuration for a ComfyUI instance
#[derive(Clone)]
pub struct ComfyUIConfig {
    /// API wrapper URL (for /generate, /result endpoints)
    pub api_url: Url,
    /// Raw ComfyUI URL (for /view endpoint to access generated files)
    pub view_url: Url,
    pub api_token: String,
}

impl ComfyUIConfig {
    pub fn new(api_url: Url, view_url: Url, api_token: String) -> Self {
        Self {
            api_url,
            view_url,
            api_token,
        }
    }

    pub fn from_env() -> Option<Self> {
        let api_url = std::env::var("COMFYUI_API_URL").ok()?;
        let view_url = std::env::var("COMFYUI_VIEW_URL").ok()?;
        let api_token = std::env::var("COMFYUI_API_TOKEN").ok()?;

        if api_token.is_empty() {
            return None;
        }

        Some(Self::new(
            Url::parse(&api_url).ok()?,
            Url::parse(&view_url).ok()?,
            api_token,
        ))
    }
}

/// ComfyUI client for submitting workflows with webhook callbacks
#[derive(Clone)]
pub struct ComfyUIClient {
    pub config: ComfyUIConfig,
    http_client: reqwest::Client,
}

/// ComfyUI API wrapper generate request
#[derive(Serialize)]
struct GenerateRequest {
    input: GenerateInput,
}

#[derive(Serialize)]
struct GenerateInput {
    request_id: String,
    workflow_json: Value,
    webhook: Option<WebhookConfig>,
}

#[derive(Serialize)]
struct WebhookConfig {
    url: String,
    extra_params: Value,
}

/// ComfyUI API wrapper generate response
#[derive(Deserialize)]
pub struct GenerateResponse {
    pub id: String,
    pub status: String,
    pub message: Option<String>,
}

/// Video generation mode
#[derive(Debug, Clone)]
pub enum VideoGenMode {
    /// Text-to-video: only prompt provided
    TextToVideo { prompt: String },
    /// Image-to-video: image URL provided (prompt optional)
    ImageToVideo {
        image_url: String,
        prompt: Option<String>,
    },
    /// Image+Text-to-video: both image and prompt provided
    ImageTextToVideo { image_url: String, prompt: String },
}

/// Webhook payload received from ComfyUI API wrapper
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ComfyUIWebhookPayload {
    pub id: String,
    pub status: String,
    pub message: Option<String>,
    pub output: Option<Vec<ComfyUIOutput>>,
    // Extra params we sent
    #[serde(flatten)]
    pub extra: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ComfyUIOutput {
    pub filename: String,
    pub local_path: Option<String>,
    pub url: Option<String>,
    #[serde(rename = "type")]
    pub file_type: Option<String>,
    pub subfolder: Option<String>,
    pub node_id: Option<String>,
    pub output_type: Option<String>,
}

impl ComfyUIClient {
    pub fn new(config: ComfyUIConfig) -> Self {
        Self {
            config,
            http_client: reqwest::Client::new(),
        }
    }

    /// Build LTX-2 distilled workflow with 1080p upscale based on the generation mode
    ///
    /// Pipeline: generate at half res (540x960) → upscale 2x in latent space → refine → tiled VAE decode
    pub fn build_ltx2_workflow(&self, mode: &VideoGenMode) -> Value {
        let (prompt_text, image_url) = match mode {
            VideoGenMode::TextToVideo { prompt } => (prompt.as_str(), None),
            VideoGenMode::ImageToVideo { image_url, prompt } => {
                (prompt.as_deref().unwrap_or(""), Some(image_url.as_str()))
            }
            VideoGenMode::ImageTextToVideo { image_url, prompt } => {
                (prompt.as_str(), Some(image_url.as_str()))
            }
        };

        let mut workflow = serde_json::json!({
            // === Model loading ===
            "1": {
                "inputs": { "ckpt_name": "ltx-2-19b-distilled.safetensors" },
                "class_type": "CheckpointLoaderSimple"
            },
            "1b": {
                "inputs": { "model": ["1", 0], "backend": "inductor" },
                "class_type": "TorchCompileModel"
            },
            "2": {
                "inputs": {
                    "gemma_path": "gemma-3-12b-it-qat-q4_0-unquantized/model-00001-of-00005.safetensors",
                    "ltxv_path": "ltx-2-19b-distilled.safetensors",
                    "max_length": 1024
                },
                "class_type": "LTXVGemmaCLIPModelLoader"
            },
            "3": {
                "inputs": { "ckpt_name": "ltx-2-19b-distilled.safetensors" },
                "class_type": "LTXVAudioVAELoader"
            },
            "16": {
                "inputs": { "model_name": "ltx-2-spatial-upscaler-x2-1.0.safetensors" },
                "class_type": "LatentUpscaleModelLoader"
            },

            // === Audio latent ===
            "5": {
                "inputs": { "frames_number": 121, "frame_rate": 24, "batch_size": 1, "audio_vae": ["3", 0] },
                "class_type": "LTXVEmptyLatentAudio"
            },

            // === Text conditioning ===
            "7": {
                "inputs": { "text": prompt_text, "clip": ["2", 0] },
                "class_type": "CLIPTextEncode"
            },
            "8": {
                "inputs": { "text": "blurry, low quality, silent, distorted", "clip": ["2", 0] },
                "class_type": "CLIPTextEncode"
            },

            // === First pass: generate at half res (540x960), 8 steps euler_ancestral ===
            "10": {
                "inputs": {
                    "seed": 0, "steps": 8, "cfg": 2.0,
                    "sampler_name": "euler_ancestral", "scheduler": "ddim_uniform", "denoise": 1.0,
                    "model": ["1b", 0], "positive": ["9b", 0], "negative": ["9b", 1],
                    "latent_image": ["6", 0]
                },
                "class_type": "KSampler"
            },

            // === Separate AV latent from first pass ===
            "11": {
                "inputs": { "av_latent": ["10", 0] },
                "class_type": "LTXVSeparateAVLatent"
            },

            // === Upscale video latent 2x ===
            "17": {
                "inputs": { "samples": ["11", 0], "upscale_model": ["16", 0], "vae": ["1", 2] },
                "class_type": "LTXVLatentUpsampler"
            },

            // === Concat upscaled video + original audio for refinement ===
            "18": {
                "inputs": { "video_latent": ["17", 0], "audio_latent": ["11", 1] },
                "class_type": "LTXVConcatAVLatent"
            },

            // === Second pass: refine at full res, 3 steps euler_ancestral ===
            "19": {
                "inputs": {
                    "seed": 0, "steps": 3, "cfg": 2.0,
                    "sampler_name": "euler_ancestral", "scheduler": "ddim_uniform", "denoise": 0.5,
                    "model": ["1b", 0], "positive": ["9b", 0], "negative": ["9b", 1],
                    "latent_image": ["18", 0]
                },
                "class_type": "KSampler"
            },

            // === Separate refined AV latent ===
            "20": {
                "inputs": { "av_latent": ["19", 0] },
                "class_type": "LTXVSeparateAVLatent"
            },

            // === Tiled VAE decode (memory-efficient for 1080p) ===
            "12": {
                "inputs": {
                    "samples": ["20", 0], "vae": ["1", 2],
                    "tile_size": 512, "overlap": 64, "temporal_size": 64, "temporal_overlap": 8
                },
                "class_type": "VAEDecodeTiled"
            },
            "12b": {
                "inputs": { "images": ["12", 0], "factor": 0.8 },
                "class_type": "AdjustContrast"
            },

            // === Audio decode ===
            "13": {
                "inputs": { "samples": ["20", 1], "audio_vae": ["3", 0] },
                "class_type": "LTXVAudioVAEDecode"
            },

            // === Output ===
            "14": {
                "inputs": { "fps": 24, "images": ["12b", 0], "audio": ["13", 0] },
                "class_type": "CreateVideo"
            },
            "15": {
                "inputs": { "filename_prefix": "ltx2-video", "format": "mp4", "codec": "h264", "video": ["14", 0] },
                "class_type": "SaveVideo"
            }
        });

        // Add mode-specific nodes (half res: 540x960 portrait)
        if let Some(img_url) = image_url {
            workflow["4"] = serde_json::json!({
                "inputs": { "image": img_url },
                "class_type": "LoadImage"
            });
            workflow["9"] = serde_json::json!({
                "inputs": {
                    "positive": ["7", 0], "negative": ["8", 0], "vae": ["1", 2],
                    "image": ["4", 0], "width": 540, "height": 960, "length": 121,
                    "batch_size": 1, "strength": 1.0
                },
                "class_type": "LTXVImgToVideo"
            });
            workflow["9b"] = serde_json::json!({
                "inputs": { "positive": ["9", 0], "negative": ["9", 1], "frame_rate": 24 },
                "class_type": "LTXVConditioning"
            });
            workflow["6"] = serde_json::json!({
                "inputs": { "video_latent": ["9", 2], "audio_latent": ["5", 0] },
                "class_type": "LTXVConcatAVLatent"
            });
        } else {
            workflow["9"] = serde_json::json!({
                "inputs": { "width": 540, "height": 960, "length": 121, "batch_size": 1 },
                "class_type": "EmptyLTXVLatentVideo"
            });
            workflow["9b"] = serde_json::json!({
                "inputs": { "positive": ["7", 0], "negative": ["8", 0], "frame_rate": 24 },
                "class_type": "LTXVConditioning"
            });
            workflow["6"] = serde_json::json!({
                "inputs": { "video_latent": ["9", 0], "audio_latent": ["5", 0] },
                "class_type": "LTXVConcatAVLatent"
            });
        }

        workflow
    }

    /// Submit a video generation job with webhook callback (async, returns immediately)
    pub async fn submit_video_generation(
        &self,
        mode: VideoGenMode,
        webhook_url: &str,
        extra_params: Value,
    ) -> Result<GenerateResponse, VideoGenError> {
        let workflow = self.build_ltx2_workflow(&mode);
        self.submit_workflow_with_webhook(workflow, webhook_url, extra_params)
            .await
    }

    /// Submit a workflow with webhook callback
    async fn submit_workflow_with_webhook(
        &self,
        workflow: Value,
        webhook_url: &str,
        extra_params: Value,
    ) -> Result<GenerateResponse, VideoGenError> {
        let request_id = uuid::Uuid::new_v4().to_string();

        let request = GenerateRequest {
            input: GenerateInput {
                request_id: request_id.clone(),
                workflow_json: workflow,
                webhook: Some(WebhookConfig {
                    url: webhook_url.to_string(),
                    extra_params,
                }),
            },
        };

        let submit_url = format!(
            "{}/generate",
            self.config.api_url.as_str().trim_end_matches('/')
        );
        info!(
            "ComfyUI: Submitting workflow to {} with webhook",
            submit_url
        );

        let response = self
            .http_client
            .post(&submit_url)
            .bearer_auth(&self.config.api_token)
            .header("Content-Type", "application/json")
            .json(&request)
            .timeout(Duration::from_secs(60))
            .send()
            .await
            .map_err(|e| VideoGenError::NetworkError(format!("Failed to submit workflow: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(VideoGenError::ProviderError(format!(
                "ComfyUI API error ({}): {}",
                status, error_text
            )));
        }

        let generate_response: GenerateResponse = response.json().await.map_err(|e| {
            VideoGenError::ProviderError(format!("Failed to parse generate response: {e}"))
        })?;

        info!(
            "ComfyUI: Workflow submitted with ID: {} (webhook will be called on completion)",
            generate_response.id
        );

        Ok(generate_response)
    }

    /// Check if the ComfyUI instance is healthy
    pub async fn health_check(&self) -> bool {
        let url = format!(
            "{}/health",
            self.config.api_url.as_str().trim_end_matches('/')
        );

        match self
            .http_client
            .get(&url)
            .bearer_auth(&self.config.api_token)
            .timeout(Duration::from_secs(10))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Get the base URL for constructing video URLs
    pub fn get_video_url(&self, filename: &str, subfolder: Option<&str>) -> String {
        let base_url = self.config.api_url.as_str().trim_end_matches('/');
        match subfolder {
            Some(sf) if !sf.is_empty() => {
                format!(
                    "{}/view?filename={}&subfolder={}&type=output",
                    base_url, filename, sf
                )
            }
            _ => {
                format!("{}/view?filename={}&type=output", base_url, filename)
            }
        }
    }
}

/// Extract video URL from ComfyUI webhook payload
pub fn extract_video_url_from_webhook(
    payload: &ComfyUIWebhookPayload,
    api_url: &str,
) -> Option<String> {
    let outputs = payload.output.as_ref()?;

    // Find the first video output (look for .mp4 files or video output type)
    for output in outputs {
        // Check if it's a video file
        let is_video = output.filename.ends_with(".mp4")
            || output.filename.ends_with(".webm")
            || output.filename.ends_with(".mov")
            || output.output_type.as_deref() == Some("videos");

        if is_video {
            // Prefer S3 URL if available
            if let Some(url) = &output.url {
                return Some(url.clone());
            }

            // Fall back to local URL using /view endpoint
            let base_url = api_url.trim_end_matches('/');

            // Try to extract subfolder from local_path if subfolder field is empty
            // local_path format: /workspace/ComfyUI/output/{request_id}/{filename}
            let subfolder = if let Some(ref lp) = output.local_path {
                // Extract the parent folder name from local_path
                std::path::Path::new(lp)
                    .parent()
                    .and_then(|p| p.file_name())
                    .and_then(|n| n.to_str())
                    .filter(|s| *s != "output") // Don't use "output" as subfolder
                    .map(|s| s.to_string())
            } else {
                output.subfolder.clone()
            };

            let subfolder = subfolder.as_deref().unwrap_or("");
            if subfolder.is_empty() {
                return Some(format!(
                    "{}/view?filename={}&type=output",
                    base_url, output.filename
                ));
            } else {
                return Some(format!(
                    "{}/view?filename={}&subfolder={}&type=output",
                    base_url, output.filename, subfolder
                ));
            }
        }
    }

    None
}
