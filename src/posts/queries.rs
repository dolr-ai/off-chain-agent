#[allow(dead_code)]
pub fn get_duplicate_children_query(videohash: String, parent_video_id: String) -> String {
    format!(
        "
    SELECT
        video_id
    FROM
        `hot-or-not-feed-intelligence`.`yral_ds`.`videohash_original`
    WHERE
        videohash = '{videohash}'
        AND video_id NOT IN (
        SELECT
            video_id
        FROM
            `hot-or-not-feed-intelligence`.`yral_ds`.`video_deleted` )
        AND video_id != '{parent_video_id}';
    "
    )
}
