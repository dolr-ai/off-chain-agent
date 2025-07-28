use candid::Principal;
use videogen_common::{VideoGenError, VideoGenRequest, VideoGenRequestWithSignature};

/// Create a message for videogen request signing
pub fn videogen_request_msg(request: VideoGenRequest) -> yral_identity::msg_builder::Message {
    yral_identity::msg_builder::Message::default()
        .method_name("videogen_generate".into())
        .args((request,))
        .expect("VideoGen request should serialize")
}

/// Verify signature for videogen request
pub fn verify_videogen_request(
    sender: Principal,
    req: &VideoGenRequestWithSignature,
) -> Result<(), VideoGenError> {
    let msg = videogen_request_msg(req.request.clone());

    req.get_signature()
        .clone()
        .verify_identity(sender, msg)
        .map_err(|_| VideoGenError::InvalidSignature)?;

    Ok(())
}