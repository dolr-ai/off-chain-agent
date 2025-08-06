// Re-export all types from videogen_common
// Since videogen_common now includes ToSchema derives, we can use them directly
pub use videogen_common::{
    ImageInput, Veo3AspectRatio, VideoGenError, VideoGenInput, VideoGenRequest, VideoGenResponse,
};
