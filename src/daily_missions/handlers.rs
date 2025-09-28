
#[utopia::path(
    post,
    tag = "daily-missions",
    path = "/daily-missions/claim/{user_id}",
    summary = "Fulfill a daily mission claim",
    description = "Fulfill a daily mission claim for a user",
    responses = {
        200 => { description = "Claim fulfilled" },
        400 => { description = "Invalid user ID" },
        404 => { description = "User not found" },
        500 => { description = "Internal server error" },
    }
)]
pub async fn fulfill_claim(state: Arc<AppState>, user_id: String) -> impl IntoResponse {
    let principal = match Principal::from_text(&user_id) {
        Ok(p) => p,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Invalid principal ID"
                })),
            )
                .into_response();
        }
    };
    
    
    
    Ok(Response::new().json("Claim fulfilled"))
}
