#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
pub fn get_icpump_insert_query(
    canister_id: String,
    description: String,
    host: String,
    link: String,
    logo: String,
    token_name: String,
    token_symbol: String,
    user_id: String,
    is_nsfw: bool,
) -> String {
    format!("
    INSERT INTO `hot-or-not-feed-intelligence.icpumpfun.token_metadata_v1` (canister_id, description, host, link, logo, token_name, token_symbol, user_id, is_nsfw, created_at, token_name_embedding, token_description_embedding)
    WITH token_description_embedding AS (
      SELECT
          ARRAY(
          SELECT CAST(JSON_VALUE(value, '$') AS FLOAT64)
          FROM UNNEST(JSON_EXTRACT_ARRAY(ml_generate_embedding_result.predictions[0].embeddings.values)) AS value
          ) AS embedding
      FROM
          ML.GENERATE_EMBEDDING(
          MODEL `hot-or-not-feed-intelligence.icpumpfun.text_embed`,
          (
              SELECT \"{description}\" AS content
          ),
          STRUCT(FALSE AS flatten_json_output, 'RETRIEVAL_QUERY' AS task_type, 256 AS output_dimensionality)
          )
    ),
    token_name_embedding AS (
        SELECT
            ARRAY(
            SELECT CAST(JSON_VALUE(value, '$') AS FLOAT64)
            FROM UNNEST(JSON_EXTRACT_ARRAY(ml_generate_embedding_result.predictions[0].embeddings.values)) AS value
            ) AS embedding
        FROM
            ML.GENERATE_EMBEDDING(
            MODEL `hot-or-not-feed-intelligence.icpumpfun.text_embed`,
            (
                SELECT \"{token_name}\" AS content
            ),
            STRUCT(FALSE AS flatten_json_output, 'RETRIEVAL_QUERY' AS task_type, 256 AS output_dimensionality)
            )
    )

    SELECT
    \"{canister_id}\",
    \"{description}\",
    \"{host}\",
    \"{link}\",
    \"{logo}\",
    \"{token_name}\",
    \"{token_symbol}\",
    \"{user_id}\",
    {is_nsfw},
    CURRENT_TIMESTAMP(),
    token_name_embedding.embedding,
    token_description_embedding.embedding
    FROM `token_name_embedding`, `token_description_embedding`;
    ")
}

// used for backfilling data
#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
pub fn get_icpump_insert_query_created_at(
    canister_id: String,
    description: String,
    host: String,
    link: String,
    logo: String,
    token_name: String,
    token_symbol: String,
    user_id: String,
    created_at: String,
) -> String {
    format!("
    INSERT INTO `hot-or-not-feed-intelligence.icpumpfun.token_metadata_v1` (canister_id, description, host, link, logo, token_name, token_symbol, user_id, created_at, token_name_embedding, token_description_embedding)
    WITH token_description_embedding AS (
      SELECT
          ARRAY(
          SELECT CAST(JSON_VALUE(value, '$') AS FLOAT64)
          FROM UNNEST(JSON_EXTRACT_ARRAY(ml_generate_embedding_result.predictions[0].embeddings.values)) AS value
          ) AS embedding
      FROM
          ML.GENERATE_EMBEDDING(
          MODEL `hot-or-not-feed-intelligence.icpumpfun.text_embed`,
          (
              SELECT \"{description}\" AS content
          ),
          STRUCT(FALSE AS flatten_json_output, 'RETRIEVAL_QUERY' AS task_type, 256 AS output_dimensionality)
          )
    ),
    token_name_embedding AS (
        SELECT
            ARRAY(
            SELECT CAST(JSON_VALUE(value, '$') AS FLOAT64)
            FROM UNNEST(JSON_EXTRACT_ARRAY(ml_generate_embedding_result.predictions[0].embeddings.values)) AS value
            ) AS embedding
        FROM
            ML.GENERATE_EMBEDDING(
            MODEL `hot-or-not-feed-intelligence.icpumpfun.text_embed`,
            (
                SELECT \"{token_name}\" AS content
            ),
            STRUCT(FALSE AS flatten_json_output, 'RETRIEVAL_QUERY' AS task_type, 256 AS output_dimensionality)
            )
    )

    SELECT
    \"{canister_id}\",
    \"{description}\",
    \"{host}\",
    \"{link}\",
    \"{logo}\",
    \"{token_name}\",
    \"{token_symbol}\",
    \"{user_id}\",
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S%Ez', '{created_at}'),
    token_name_embedding.embedding,
    token_description_embedding.embedding
    FROM `token_name_embedding`, `token_description_embedding`;
    ")
}
