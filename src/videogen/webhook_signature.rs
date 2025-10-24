use axum::http::StatusCode;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// Error types for webhook signature verification
#[derive(Debug)]
pub enum WebhookError {
    InvalidSignature,
    TimestampOutOfRange,
    MissingHeaders,
    InvalidTimestamp,
    InvalidFormat,
}

impl From<WebhookError> for (StatusCode, String) {
    fn from(error: WebhookError) -> Self {
        match error {
            WebhookError::InvalidSignature => (StatusCode::UNAUTHORIZED, "Invalid signature".to_string()),
            WebhookError::TimestampOutOfRange => (StatusCode::UNAUTHORIZED, "Timestamp outside acceptable range".to_string()),
            WebhookError::MissingHeaders => (StatusCode::BAD_REQUEST, "Missing required headers".to_string()),
            WebhookError::InvalidTimestamp => (StatusCode::BAD_REQUEST, "Invalid timestamp format".to_string()),
            WebhookError::InvalidFormat => (StatusCode::BAD_REQUEST, "Invalid webhook format".to_string()),
        }
    }
}

/// Replicate webhook headers
pub struct WebhookHeaders {
    pub id: String,
    pub timestamp: String,
    pub signature: String,
}

impl WebhookHeaders {
    /// Extract webhook headers from HTTP headers
    pub fn from_http_headers(headers: &axum::http::HeaderMap) -> Result<Self, WebhookError> {
        let id = headers
            .get("webhook-id")
            .and_then(|v| v.to_str().ok())
            .ok_or(WebhookError::MissingHeaders)?
            .to_string();
        
        let timestamp = headers
            .get("webhook-timestamp")
            .and_then(|v| v.to_str().ok())
            .ok_or(WebhookError::MissingHeaders)?
            .to_string();
        
        let signature = headers
            .get("webhook-signature")
            .and_then(|v| v.to_str().ok())
            .ok_or(WebhookError::MissingHeaders)?
            .to_string();

        Ok(WebhookHeaders {
            id,
            timestamp,
            signature,
        })
    }
}

/// Verify Replicate webhook signature
pub fn verify_webhook_signature(
    headers: &WebhookHeaders,
    payload: &[u8],
    signing_secret: &str,
) -> Result<(), WebhookError> {
    // Validate timestamp (within 5 minutes)
    validate_timestamp(&headers.timestamp)?;

    // Construct signed content: webhook_id.webhook_timestamp.payload
    let signed_content = format!("{}.{}.{}", headers.id, headers.timestamp, String::from_utf8_lossy(payload));

    // Generate HMAC signature
    let mut mac = HmacSha256::new_from_slice(signing_secret.as_bytes())
        .map_err(|_| WebhookError::InvalidFormat)?;
    mac.update(signed_content.as_bytes());
    let generated_signature = hex::encode(mac.finalize().into_bytes());

    // Parse expected signature (remove "v1=" prefix if present)
    let expected_signature = headers.signature
        .strip_prefix("v1=")
        .unwrap_or(&headers.signature);

    // Compare signatures in constant time
    if !constant_time_eq(expected_signature.as_bytes(), generated_signature.as_bytes()) {
        return Err(WebhookError::InvalidSignature);
    }

    Ok(())
}

/// Validate that the timestamp is within acceptable range (5 minutes)
fn validate_timestamp(timestamp_str: &str) -> Result<(), WebhookError> {
    let timestamp = timestamp_str
        .parse::<u64>()
        .map_err(|_| WebhookError::InvalidTimestamp)?;

    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WebhookError::InvalidTimestamp)?
        .as_secs();

    const FIVE_MINUTES: u64 = 5 * 60; // 5 minutes in seconds

    if timestamp.abs_diff(current_time) > FIVE_MINUTES {
        return Err(WebhookError::TimestampOutOfRange);
    }

    Ok(())
}

/// Constant time string comparison to prevent timing attacks
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut result = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    result == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use axum::http::{HeaderMap, HeaderName, HeaderValue};

    #[test]
    fn test_webhook_headers_extraction() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("webhook-id"),
            HeaderValue::from_static("test-id"),
        );
        headers.insert(
            HeaderName::from_static("webhook-timestamp"),
            HeaderValue::from_static("1234567890"),
        );
        headers.insert(
            HeaderName::from_static("webhook-signature"),
            HeaderValue::from_static("v1=abcd1234"),
        );

        let webhook_headers = WebhookHeaders::from_http_headers(&headers).unwrap();
        assert_eq!(webhook_headers.id, "test-id");
        assert_eq!(webhook_headers.timestamp, "1234567890");
        assert_eq!(webhook_headers.signature, "v1=abcd1234");
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"world"));
        assert!(!constant_time_eq(b"hello", b"hell"));
    }
}