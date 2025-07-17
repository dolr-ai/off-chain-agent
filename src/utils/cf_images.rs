use base64::{engine::general_purpose, Engine as _};
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct CloudflareResponse {
    pub result: CloudflareResult,
    pub success: bool,
    pub errors: Vec<String>,
    pub messages: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CloudflareResult {
    pub id: String,
    pub filename: String,
    pub uploaded: String,
    #[serde(rename = "requireSignedURLs")]
    pub require_signed_urls: bool,
    pub variants: Vec<String>,
}

pub async fn upload_base64_image(
    account_id: &str,
    api_token: &str,
    base64_image_without_prefix: &str,
    filename: &str,
) -> Result<CloudflareResponse, anyhow::Error> {
    let client = reqwest::Client::new();

    // Decode base64 string to bytes
    let image_data = general_purpose::STANDARD.decode(base64_image_without_prefix)?;

    let form = Form::new().part(
        "file",
        Part::bytes(image_data).file_name(filename.to_string()),
    );

    let response = client
        .post(format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/images/v1",
            account_id
        ))
        .header("Authorization", format!("Bearer {api_token}"))
        .multipart(form)
        .send()
        .await?;

    let cloudflare_response: CloudflareResponse = response.json().await?;
    Ok(cloudflare_response)
}
