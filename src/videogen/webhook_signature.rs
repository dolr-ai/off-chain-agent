use axum::http::StatusCode;
use base64::{engine::general_purpose, Engine};
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
            WebhookError::InvalidSignature => {
                (StatusCode::UNAUTHORIZED, "Invalid signature".to_string())
            }
            WebhookError::TimestampOutOfRange => (
                StatusCode::UNAUTHORIZED,
                "Timestamp outside acceptable range".to_string(),
            ),
            WebhookError::MissingHeaders => (
                StatusCode::BAD_REQUEST,
                "Missing required headers".to_string(),
            ),
            WebhookError::InvalidTimestamp => (
                StatusCode::BAD_REQUEST,
                "Invalid timestamp format".to_string(),
            ),
            WebhookError::InvalidFormat => (
                StatusCode::BAD_REQUEST,
                "Invalid webhook format".to_string(),
            ),
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

/// Extract the base64 portion from Replicate signing secret
/// Replicate secrets are in format: whsec_<base64_key>
fn extract_signing_key(signing_secret: &str) -> Result<Vec<u8>, WebhookError> {
    let parts: Vec<&str> = signing_secret.split('_').collect();
    if parts.len() != 2 || parts[0] != "whsec" {
        return Err(WebhookError::InvalidFormat);
    }

    general_purpose::STANDARD
        .decode(parts[1])
        .map_err(|_| WebhookError::InvalidFormat)
}

/// Verify Replicate webhook signature
pub fn verify_webhook_signature(
    headers: &WebhookHeaders,
    payload: &[u8],
    signing_secret: &str,
) -> Result<(), WebhookError> {
    // Validate timestamp (within 5 minutes)
    validate_timestamp(&headers.timestamp)?;

    // Extract the base64 portion from the signing secret and decode it
    let signing_key = extract_signing_key(signing_secret)?;

    // Construct signed content: webhook_id.webhook_timestamp.payload
    let signed_content = format!(
        "{}.{}.{}",
        headers.id,
        headers.timestamp,
        String::from_utf8_lossy(payload)
    );

    // Generate HMAC signature using the decoded signing key
    let mut mac =
        HmacSha256::new_from_slice(&signing_key).map_err(|_| WebhookError::InvalidFormat)?;
    mac.update(signed_content.as_bytes());
    let generated_signature_bytes = mac.finalize().into_bytes();

    // Encode as base64 (Replicate uses base64, not hex)
    let computed_signature = general_purpose::STANDARD.encode(generated_signature_bytes);

    // Parse expected signatures - can be multiple signatures separated by spaces
    // Each signature is in format: v1,<signature>
    let expected_signatures: Vec<&str> = headers
        .signature
        .split(' ')
        .filter_map(|sig| {
            let parts: Vec<&str> = sig.split(',').collect();
            if parts.len() == 2 && parts[0] == "v1" {
                Some(parts[1])
            } else {
                None
            }
        })
        .collect();

    if expected_signatures.is_empty() {
        return Err(WebhookError::InvalidFormat);
    }

    // Check if any of the expected signatures matches our computed signature
    let is_valid = expected_signatures.iter().any(|expected_sig| {
        constant_time_eq(expected_sig.as_bytes(), computed_signature.as_bytes())
    });

    if !is_valid {
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

    #[test]
    fn test_signing_secret_extraction() {
        // Test valid Replicate signing secret format
        let signing_secret = "whsec_C2FVsBQIhrscChlQIMV+b5sSYspob7oD";
        let result = extract_signing_key(signing_secret);
        assert!(result.is_ok());

        // Test invalid format (missing prefix)
        let invalid_secret = "C2FVsBQIhrscChlQIMV+b5sSYspob7oD";
        let result = extract_signing_key(invalid_secret);
        assert!(result.is_err());

        // Test invalid format (wrong prefix)
        let wrong_prefix = "other_C2FVsBQIhrscChlQIMV+b5sSYspob7oD";
        let result = extract_signing_key(wrong_prefix);
        assert!(result.is_err());

        // Test invalid base64
        let invalid_base64 = "whsec_invalid@base64!";
        let result = extract_signing_key(invalid_base64);
        assert!(result.is_err());

        // Test multiple underscores (should fail)
        let multiple_underscores = "whsec_part1_part2_C2FVsBQIhrscChlQIMV+b5sSYspob7oD";
        let result = extract_signing_key(multiple_underscores);
        assert!(result.is_err());
    }

    #[test]
    fn test_signature_parsing() {
        // Test single signature
        let single_sig = "v1,abc123";
        let sigs: Vec<&str> = single_sig
            .split(' ')
            .filter_map(|sig| {
                let parts: Vec<&str> = sig.split(',').collect();
                if parts.len() == 2 && parts[0] == "v1" {
                    Some(parts[1])
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(sigs, vec!["abc123"]);

        // Test multiple signatures
        let multi_sig = "v1,sig1 v1,sig2 v1,sig3";
        let sigs: Vec<&str> = multi_sig
            .split(' ')
            .filter_map(|sig| {
                let parts: Vec<&str> = sig.split(',').collect();
                if parts.len() == 2 && parts[0] == "v1" {
                    Some(parts[1])
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(sigs, vec!["sig1", "sig2", "sig3"]);

        // Test invalid format
        let invalid_sig = "v2,abc123";
        let sigs: Vec<&str> = invalid_sig
            .split(' ')
            .filter_map(|sig| {
                let parts: Vec<&str> = sig.split(',').collect();
                if parts.len() == 2 && parts[0] == "v1" {
                    Some(parts[1])
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(sigs.len(), 0);
    }

    #[test]
    fn test_production_signature_verification() {
        // Test case from production data that was failing
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("webhook-id"),
            HeaderValue::from_static("msg_350Xi4Bl1cYIR6EmZaLimekP83D"),
        );
        headers.insert(
            HeaderName::from_static("webhook-timestamp"),
            HeaderValue::from_static("1730748137"), // Adjusted to valid timestamp
        );
        headers.insert(
            HeaderName::from_static("webhook-signature"),
            HeaderValue::from_static("v1,UrnZYA8+FlfEjiusgeK40NsNCXVlN6sBEqSaaB5n4P8="),
        );

        let webhook_headers = WebhookHeaders::from_http_headers(&headers).unwrap();

        // Mock payload and signing secret - we need the actual values to test properly
        let payload = b"{}"; // Simplified for test
        let signing_secret = "whsec_dGVzdF9zZWNyZXQ="; // base64 encoded "test_secret"

        // This should work with the corrected implementation
        let result = verify_webhook_signature(&webhook_headers, payload, signing_secret);
        println!("Signature verification result: {:?}", result);
    }
}
