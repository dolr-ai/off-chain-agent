use candid::Principal;

/// Set Sentry user context from a user principal
///
/// This should be called after authentication to associate errors and events
/// with a specific user. The user_principal is safely added to Sentry context.
///
/// # Example
/// ```ignore
/// let user_info = get_user_info_from_delegated_identity_wire(&state, wire).await?;
/// set_user_context(user_info.user_principal);
/// ```
pub fn set_user_context(user_principal: Principal) {
    sentry::configure_scope(|scope| {
        scope.set_user(Some(sentry::User {
            id: Some(user_principal.to_string()),
            ..Default::default()
        }));
    });
}

/// Set Sentry user context with additional metadata
///
/// Allows setting user context with optional username and additional data.
#[allow(dead_code)]
pub fn set_user_context_with_metadata(
    user_principal: Principal,
    user_canister: Option<Principal>,
    username: Option<String>,
) {
    sentry::configure_scope(|scope| {
        let mut user = sentry::User {
            id: Some(user_principal.to_string()),
            username,
            ..Default::default()
        };

        // Add user_canister as extra data if provided
        if let Some(canister) = user_canister {
            user.other
                .insert("user_canister".to_string(), canister.to_string().into());
        }

        scope.set_user(Some(user));
    });
}

/// Clear Sentry user context
///
/// Useful for clearing user context after request completion or in error cases.
#[allow(dead_code)]
pub fn clear_user_context() {
    sentry::configure_scope(|scope| {
        scope.set_user(None);
    });
}

/// Add custom tag to Sentry scope
///
/// Useful for adding additional context like endpoint names, request types, etc.
#[allow(dead_code)]
pub fn add_tag(key: &str, value: &str) {
    sentry::configure_scope(|scope| {
        scope.set_tag(key, value);
    });
}

/// Add custom context to Sentry scope
///
/// Useful for adding structured data to Sentry events.
#[allow(dead_code)]
pub fn add_context(key: &str, value: serde_json::Value) {
    sentry::configure_scope(|scope| {
        if let Ok(sentry_value) = serde_json::from_value::<sentry::protocol::Value>(value) {
            let context = sentry::protocol::Context::Other(
                [(key.to_string(), sentry_value)].into_iter().collect(),
            );
            scope.set_context(key, context);
        }
    });
}
