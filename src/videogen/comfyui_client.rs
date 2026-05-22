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
        let api_token = std::env::var("COMFYUI_API_TOKEN").ok()?;

        if api_token.is_empty() {
            return None;
        }

        let url = crate::consts::COMFYUI_URL.as_str();
        Some(Self::new(
            Url::parse(url).ok()?,
            Url::parse(url).ok()?,
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

    /// Upload an image to ComfyUI's input directory via /upload/image endpoint.
    /// Returns the filename to use in LoadImage node.
    pub async fn upload_image(
        &self,
        image_bytes: Vec<u8>,
        filename: &str,
    ) -> Result<String, VideoGenError> {
        let upload_url = format!(
            "{}/upload/image",
            self.config.view_url.as_str().trim_end_matches('/')
        );

        let part = reqwest::multipart::Part::bytes(image_bytes)
            .file_name(filename.to_string())
            .mime_str("image/png")
            .map_err(|e| VideoGenError::NetworkError(format!("Failed to create multipart: {e}")))?;

        let form = reqwest::multipart::Form::new()
            .part("image", part)
            .text("overwrite", "true");

        let response = self
            .http_client
            .post(&upload_url)
            .bearer_auth(&self.config.api_token)
            .multipart(form)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| {
                VideoGenError::NetworkError(format!("Failed to upload image to ComfyUI: {e}"))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            log::error!("ComfyUI image upload failed: {status}, {body}");
            return Err(VideoGenError::ProviderError(format!(
                "ComfyUI image upload failed: {status}, {body}"
            )));
        }

        #[derive(Deserialize)]
        struct UploadResponse {
            name: String,
        }

        let upload_resp: UploadResponse = response.json().await.map_err(|e| {
            VideoGenError::ProviderError(format!("Failed to parse upload response: {e}"))
        })?;

        info!("Uploaded image to ComfyUI: {}", upload_resp.name);
        Ok(upload_resp.name)
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

        let is_t2v = image_url.is_none();
        let image_input = image_url.unwrap_or("example.png");

        // LTX-2.3 two-pass workflow (verified working, exported from ComfyUI).
        // Pass 1: half-res (640x360) with euler_ancestral_cfg_pp.
        // Pass 2: upscale 2x then refine with euler_cfg_pp.
        // T2V/I2V toggled via bypass on LTXVImgToVideoInplace nodes.
        serde_json::json!({
            // === Model loaders ===
            "267:236": {
                "inputs": { "ckpt_name": "ltx-2.3-22b-dev-fp8.safetensors" },
                "class_type": "CheckpointLoaderSimple"
            },
            "267:243": {
                "inputs": {
                    "text_encoder": "gemma_3_12B_it_fp4_mixed.safetensors",
                    "ckpt_name": "ltx-2.3-22b-dev-fp8.safetensors",
                    "device": "default"
                },
                "class_type": "LTXAVTextEncoderLoader"
            },
            "267:221": {
                "inputs": { "ckpt_name": "ltx-2.3-22b-dev-fp8.safetensors" },
                "class_type": "LTXVAudioVAELoader"
            },
            "267:232": {
                "inputs": {
                    "lora_name": "ltx-2.3-22b-distilled-lora-384.safetensors",
                    "strength_model": 0.5,
                    "model": ["267:236", 0]
                },
                "class_type": "LoraLoaderModelOnly"
            },
            "267:233": {
                "inputs": { "model_name": "ltx-2.3-spatial-upscaler-x2-1.1.safetensors" },
                "class_type": "LatentUpscaleModelLoader"
            },

            // === Video parameters ===
            "267:201": { "inputs": { "value": is_t2v }, "class_type": "PrimitiveBoolean" },
            "267:260": { "inputs": { "value": 25 }, "class_type": "PrimitiveInt" },
            "267:225": { "inputs": { "value": 5 }, "class_type": "PrimitiveInt" },
            "267:257": { "inputs": { "value": 1280 }, "class_type": "PrimitiveInt" },
            "267:258": { "inputs": { "value": 720 }, "class_type": "PrimitiveInt" },
            "267:261": { "inputs": { "expression": "a", "values.a": ["267:260", 0] }, "class_type": "ComfyMathExpression" },
            "267:277": { "inputs": { "expression": "a * b + 1", "values.a": ["267:225", 0], "values.b": ["267:260", 0] }, "class_type": "ComfyMathExpression" },
            "267:256": { "inputs": { "expression": "a/2", "values.a": ["267:257", 0] }, "class_type": "ComfyMathExpression" },
            "267:259": { "inputs": { "expression": "a/2", "values.a": ["267:258", 0] }, "class_type": "ComfyMathExpression" },

            // === Prompts ===
            "267:266": { "inputs": { "value": prompt_text }, "class_type": "PrimitiveStringMultiline" },
            "267:240": { "inputs": { "text": ["267:266", 0], "clip": ["267:243", 0] }, "class_type": "CLIPTextEncode" },
            "267:247": { "inputs": { "text": "pc game, console game, video game, cartoon, childish, ugly", "clip": ["267:243", 0] }, "class_type": "CLIPTextEncode" },
            "267:239": {
                "inputs": {
                    "frame_rate": ["267:261", 0],
                    "positive": ["267:240", 0],
                    "negative": ["267:247", 0]
                },
                "class_type": "LTXVConditioning"
            },

            // === Audio latent ===
            "267:214": {
                "inputs": {
                    "frames_number": ["267:277", 1],
                    "frame_rate": ["267:261", 1],
                    "batch_size": 1,
                    "audio_vae": ["267:221", 0]
                },
                "class_type": "LTXVEmptyLatentAudio"
            },

            // === Image input (bypassed for T2V via 267:201) ===
            "267:276": { "inputs": { "image": image_input }, "class_type": "LoadImage" },
            "267:238": {
                "inputs": {
                    "resize_type": "scale dimensions",
                    "resize_type.width": ["267:257", 0],
                    "resize_type.height": ["267:258", 0],
                    "resize_type.crop": "center",
                    "scale_method": "lanczos",
                    "input": ["267:276", 0]
                },
                "class_type": "ResizeImageMaskNode"
            },
            "267:235": { "inputs": { "longer_edge": 1536, "images": ["267:238", 0] }, "class_type": "ResizeImagesByLongerEdge" },
            "267:248": { "inputs": { "img_compression": 18, "image": ["267:235", 0] }, "class_type": "LTXVPreprocess" },

            // === Pass 1: half-res latent ===
            "267:228": {
                "inputs": {
                    "width": ["267:256", 1],
                    "height": ["267:259", 1],
                    "length": ["267:277", 1],
                    "batch_size": 1
                },
                "class_type": "EmptyLTXVLatentVideo"
            },
            "267:249": {
                "inputs": {
                    "strength": 0.7,
                    "bypass": ["267:201", 0],
                    "vae": ["267:236", 2],
                    "image": ["267:248", 0],
                    "latent": ["267:228", 0]
                },
                "class_type": "LTXVImgToVideoInplace"
            },
            "267:222": { "inputs": { "video_latent": ["267:249", 0], "audio_latent": ["267:214", 0] }, "class_type": "LTXVConcatAVLatent" },
            "267:237": { "inputs": { "noise_seed": 0 }, "class_type": "RandomNoise" },
            "267:209": { "inputs": { "sampler_name": "euler_ancestral_cfg_pp" }, "class_type": "KSamplerSelect" },
            "267:252": { "inputs": { "sigmas": "1.0, 0.99375, 0.9875, 0.98125, 0.975, 0.909375, 0.725, 0.421875, 0.0" }, "class_type": "ManualSigmas" },
            "267:231": {
                "inputs": { "cfg": 1, "model": ["267:232", 0], "positive": ["267:239", 0], "negative": ["267:239", 1] },
                "class_type": "CFGGuider"
            },
            "267:215": {
                "inputs": {
                    "noise": ["267:237", 0], "guider": ["267:231", 0],
                    "sampler": ["267:209", 0], "sigmas": ["267:252", 0],
                    "latent_image": ["267:222", 0]
                },
                "class_type": "SamplerCustomAdvanced"
            },
            "267:217": { "inputs": { "av_latent": ["267:215", 0] }, "class_type": "LTXVSeparateAVLatent" },

            // === Upscale 2x ===
            "267:253": {
                "inputs": { "samples": ["267:217", 0], "upscale_model": ["267:233", 0], "vae": ["267:236", 2] },
                "class_type": "LTXVLatentUpsampler"
            },

            // === Pass 2: full-res refinement ===
            "267:230": {
                "inputs": {
                    "strength": 1.0,
                    "bypass": ["267:201", 0],
                    "vae": ["267:236", 2],
                    "image": ["267:248", 0],
                    "latent": ["267:253", 0]
                },
                "class_type": "LTXVImgToVideoInplace"
            },
            "267:229": { "inputs": { "video_latent": ["267:230", 0], "audio_latent": ["267:217", 1] }, "class_type": "LTXVConcatAVLatent" },
            "267:212": {
                "inputs": { "positive": ["267:239", 0], "negative": ["267:239", 1], "latent": ["267:217", 0] },
                "class_type": "LTXVCropGuides"
            },
            "267:216": { "inputs": { "noise_seed": 0 }, "class_type": "RandomNoise" },
            "267:246": { "inputs": { "sampler_name": "euler_cfg_pp" }, "class_type": "KSamplerSelect" },
            "267:211": { "inputs": { "sigmas": "0.85, 0.7250, 0.4219, 0.0" }, "class_type": "ManualSigmas" },
            "267:213": {
                "inputs": { "cfg": 1, "model": ["267:232", 0], "positive": ["267:212", 0], "negative": ["267:212", 1] },
                "class_type": "CFGGuider"
            },
            "267:219": {
                "inputs": {
                    "noise": ["267:216", 0], "guider": ["267:213", 0],
                    "sampler": ["267:246", 0], "sigmas": ["267:211", 0],
                    "latent_image": ["267:229", 0]
                },
                "class_type": "SamplerCustomAdvanced"
            },
            "267:218": { "inputs": { "av_latent": ["267:219", 0] }, "class_type": "LTXVSeparateAVLatent" },

            // === Decode ===
            "267:251": {
                "inputs": {
                    "tile_size": 768, "overlap": 64, "temporal_size": 4096, "temporal_overlap": 4,
                    "samples": ["267:218", 0], "vae": ["267:236", 2]
                },
                "class_type": "VAEDecodeTiled"
            },
            "267:220": { "inputs": { "samples": ["267:218", 1], "audio_vae": ["267:221", 0] }, "class_type": "LTXVAudioVAEDecode" },

            // === Output ===
            "267:242": { "inputs": { "fps": ["267:261", 0], "images": ["267:251", 0], "audio": ["267:220", 0] }, "class_type": "CreateVideo" },
            "75": {
                "inputs": { "filename_prefix": "video/ltx2-3", "format": "auto", "codec": "auto", "video-preview": "", "video": ["267:242", 0] },
                "class_type": "SaveVideo"
            }
        })
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
            log::error!("ComfyUI API error ({}): {}", status, error_text);
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
