use sentry::protocol::{Event, Request as SentryRequest};
use std::sync::Arc;

/// List of field names that contain sensitive data and should be scrubbed
const SENSITIVE_FIELDS: &[&str] = &[
    "delegated_identity_wire",
    "authorization",
    "bearer",
    "token",
    "api_key",
    "secret",
    "password",
    "private_key",
    "access_token",
    "refresh_token",
    "session_token",
    "auth_token",
];

/// Checks if a field name contains sensitive data
fn is_sensitive_field(field_name: &str) -> bool {
    let field_lower = field_name.to_lowercase();
    SENSITIVE_FIELDS
        .iter()
        .any(|sensitive| field_lower.contains(sensitive))
}

/// Scrubs sensitive data from Sentry events before sending
pub fn scrub_sensitive_data(mut event: Event<'static>) -> Option<Event<'static>> {
    // Scrub request data
    if let Some(request) = &mut event.request {
        scrub_request(request);
    }

    // Scrub extra data
    let keys_to_remove: Vec<String> = event.extra
        .keys()
        .filter(|k| is_sensitive_field(k))
        .cloned()
        .collect();

    for key in keys_to_remove {
        event.extra.remove(&key);
    }

    // Scrub contexts
    for (_key, context) in event.contexts.iter_mut() {
        if let sentry::protocol::Context::Other(map) = context {
            let keys_to_remove: Vec<String> = map
                .keys()
                .filter(|k| is_sensitive_field(k))
                .cloned()
                .collect();

            for key in keys_to_remove {
                map.remove(&key);
            }
        }
    }

    // Scrub breadcrumbs
    for breadcrumb in event.breadcrumbs.values.iter_mut() {
        let keys_to_remove: Vec<String> = breadcrumb.data
            .keys()
            .filter(|k| is_sensitive_field(k))
            .cloned()
            .collect();

        for key in keys_to_remove {
            breadcrumb.data.remove(&key);
        }
    }

    Some(event)
}

/// Scrubs sensitive data from Sentry transaction events
pub fn scrub_transaction_data(event: Event<'static>) -> Option<Event<'static>> {
    // Use the same scrubbing logic for transactions
    scrub_sensitive_data(event)
}

/// Scrubs sensitive headers and data from Sentry request objects
fn scrub_request(request: &mut SentryRequest) {
    // Scrub sensitive headers
    let keys_to_scrub: Vec<String> = request.headers
        .keys()
        .filter(|k| is_sensitive_field(k))
        .cloned()
        .collect();

    for key in keys_to_scrub {
        request.headers.insert(key, "[REDACTED]".to_string());
    }

    // Scrub query string if it contains sensitive data
    if let Some(query_string) = &request.query_string {
        if SENSITIVE_FIELDS
            .iter()
            .any(|field| query_string.to_lowercase().contains(field))
        {
            request.query_string = Some("[REDACTED]".to_string());
        }
    }

    // Scrub request data - request.data is a String in Sentry protocol
    // We'll check if it contains sensitive field names
    if let Some(data_str) = &request.data {
        if SENSITIVE_FIELDS
            .iter()
            .any(|field| data_str.to_lowercase().contains(field))
        {
            request.data = Some("[REDACTED - Contains sensitive data]".to_string());
        }
    }
}

/// Creates Arc-wrapped scrubbing functions for Sentry ClientOptions
pub fn create_before_send() -> Arc<dyn Fn(Event<'static>) -> Option<Event<'static>> + Send + Sync> {
    Arc::new(scrub_sensitive_data)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_sensitive_field() {
        assert!(is_sensitive_field("delegated_identity_wire"));
        assert!(is_sensitive_field("Authorization"));
        assert!(is_sensitive_field("bearer_token"));
        assert!(is_sensitive_field("api_key"));
        assert!(!is_sensitive_field("user_principal"));
        assert!(!is_sensitive_field("request_id"));
    }
}
