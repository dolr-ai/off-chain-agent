/// All possible steps in our processing pipeline
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Deduplication,
    ExtractFrames,
    GcsUpload,
    NsfwDetection,
    NsfwDetectionV2,
    StorjIngest,
}

impl std::fmt::Display for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Step::Deduplication => "deduplication",
            Step::ExtractFrames => "extract_frames",
            Step::GcsUpload => "gcs_upload",
            Step::NsfwDetection => "nsfw_detection",
            Step::NsfwDetectionV2 => "nsfw_detection_v2",
            Step::StorjIngest => "storj_ingest",
        };

        f.write_str(text)
    }
}

#[macro_export]
macro_rules! setup_context {
    ($video_id:expr, $step:expr, {
        $($key:literal: $value:expr),+ $(,)?
    }) => {
        sentry::configure_scope(|scope| {
            use std::collections::BTreeMap;
            use sentry::protocol::Context;
            scope.set_tag("pipeline.video_id", $video_id);
            scope.set_tag("pipeline.step", $step);
            let map = BTreeMap::from([
                $(
                  ($key.into(), serde_json::to_value($value).expect("value for context to be json serializable")),
                )*
            ]);
            scope.set_context("context", Context::Other(map));
        })
    };
    ($video_id:expr, $step:expr) => {
        sentry::configure_scope(|scope| {
            scope.set_tag("pipeline.video_id", $video_id);
            scope.set_tag("pipeline.step", $step);
        })
    };
}
