pub mod utils;

use anyhow::{Context, Result};
use milvus::client::{Client as MilvusClient, ClientBuilder};
use milvus::collection::SearchOption;
use milvus::data::FieldColumn;
use milvus::index::{IndexParams, IndexType, MetricType};
use milvus::options::CreateCollectionOptions;
use milvus::schema::{CollectionSchemaBuilder, FieldSchema};
use milvus::value::Value;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;

// Re-export MilvusClient for other modules to use
pub use milvus::client::Client;

const COLLECTION_NAME: &str = "video_phash";
const PHASH_DIM: i64 = 640;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoHashRecord {
    pub video_id: String,
    pub phash_vector: Vec<u8>,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub video_id: String,
    pub hamming_distance: u32,
}

/// Initialize Milvus client with connection to server
/// Supports format: https://username:password@host:port or https://host:port
pub async fn create_milvus_client(url: String) -> Result<MilvusClient> {
    log::info!("Connecting to Milvus");

    let client = if url.contains("@") {
        // Parse URL with authentication: https://username:password@host:port
        let parts: Vec<&str> = url.split("@").collect();
        if parts.len() != 2 {
            anyhow::bail!("Invalid Milvus URL format with authentication");
        }

        let auth_part = parts[0];
        let host_part = parts[1];

        // Extract scheme, username, and password
        let scheme_split: Vec<&str> = auth_part.split("://").collect();
        if scheme_split.len() != 2 {
            anyhow::bail!("Invalid URL scheme");
        }

        let scheme = scheme_split[0];
        let credentials = scheme_split[1];

        let cred_parts: Vec<&str> = credentials.split(":").collect();
        if cred_parts.len() != 2 {
            anyhow::bail!("Invalid credentials format");
        }

        let username = cred_parts[0];
        let password = cred_parts[1];
        let endpoint = format!("{}://{}", scheme, host_part);

        log::info!(
            "Connecting to Milvus at {} with user: {}",
            host_part,
            username
        );

        ClientBuilder::new(endpoint)
            .username(username)
            .password(password)
            .build()
            .await
            .context("Failed to connect to Milvus with authentication")?
    } else {
        log::info!("Connecting to Milvus at {}", url);
        // Leak the string to get a 'static reference - acceptable since this is called once at startup
        let static_url: &'static str = Box::leak(url.into_boxed_str());
        MilvusClient::new(static_url)
            .await
            .context("Failed to connect to Milvus")?
    };

    log::info!("Successfully connected to Milvus");
    Ok(client)
}

/// Check if collection exists and create it if not
pub async fn init_collection(client: &MilvusClient) -> Result<()> {
    log::info!("Initializing Milvus collection: {}", COLLECTION_NAME);

    // Check if collection exists
    let has_collection = client
        .has_collection(COLLECTION_NAME)
        .await
        .context("Failed to check if collection exists")?;

    if has_collection {
        log::info!("Collection {} already exists", COLLECTION_NAME);
        return Ok(());
    }

    log::info!("Creating new collection: {}", COLLECTION_NAME);

    // Create collection schema
    let mut schema_builder =
        CollectionSchemaBuilder::new(COLLECTION_NAME, "Video phash deduplication collection");

    schema_builder.add_field(FieldSchema::new_primary_varchar(
        "video_id",
        "Unique video identifier",
        false, // auto_id
        256,   // max_length
    ));

    schema_builder.add_field(FieldSchema::new_binary_vector(
        "phash_vector",
        "640-bit perceptual hash as binary vector",
        PHASH_DIM,
    ));

    schema_builder.add_field(FieldSchema::new_int64(
        "created_at",
        "Timestamp when hash was ingested",
    ));

    let schema = schema_builder
        .build()
        .context("Failed to build collection schema")?;

    // Create the collection
    client
        .create_collection(schema, Some(CreateCollectionOptions::default()))
        .await
        .context("Failed to create collection")?;

    log::info!("Collection {} created successfully", COLLECTION_NAME);

    // Create index on the binary vector field
    create_hamming_index(client).await?;

    // Load collection into memory
    load_collection(client).await?;

    Ok(())
}

/// Create HAMMING index on phash_vector field
async fn create_hamming_index(client: &MilvusClient) -> Result<()> {
    log::info!("Creating HAMMING index on phash_vector field");

    // Get the collection
    let collection = client
        .get_collection(COLLECTION_NAME)
        .await
        .context("Failed to get collection")?;

    let index_params = IndexParams::new(
        "phash_index".to_string(),
        IndexType::BinFlat,
        MetricType::HAMMING,
        HashMap::new(),
    );

    collection
        .create_index("phash_vector", index_params)
        .await
        .context("Failed to create index")?;

    log::info!("Index created successfully");
    Ok(())
}

/// Load collection into memory for searching
async fn load_collection(client: &MilvusClient) -> Result<()> {
    log::info!("Loading collection {} into memory", COLLECTION_NAME);

    let collection = client
        .get_collection(COLLECTION_NAME)
        .await
        .context("Failed to get collection")?;

    collection
        .load(1) // replica_number = 1
        .await
        .context("Failed to load collection")?;

    log::info!("Collection loaded successfully");
    Ok(())
}

/// Search for similar videos by phash with Hamming distance threshold
pub async fn search_similar_videos(
    client: &MilvusClient,
    phash: &str,
    distance_threshold: u32,
) -> Result<Vec<SearchResult>> {
    log::debug!(
        "Searching for similar videos with threshold {}",
        distance_threshold
    );

    // Get the collection
    let collection = client
        .get_collection(COLLECTION_NAME)
        .await
        .context("Failed to get collection")?;

    // Check if collection is loaded
    if !collection
        .is_loaded()
        .await
        .context("Failed to check if collection is loaded")?
    {
        log::warn!("Collection is not loaded, loading now...");
        collection
            .load(1)
            .await
            .context("Failed to load collection")?;
    }

    // Convert phash to binary vector
    let query_vector = utils::phash_to_binary_vector(phash)?;
    let query_vectors = vec![Value::Binary(Cow::Owned(query_vector))];

    // Prepare search parameters
    let mut search_option = SearchOption::new();
    search_option.add_param("nprobe", serde_json::json!(10));

    // Search for top 10 nearest neighbors (we'll filter by threshold later)
    let results = collection
        .search(
            query_vectors,
            "phash_vector",
            10, // top_k
            MetricType::HAMMING,
            vec!["video_id".to_string()],
            &search_option,
        )
        .await
        .context("Failed to search in Milvus")?;

    // Parse results and filter by distance threshold
    let mut similar_videos = Vec::new();

    for result_set in results {
        for i in 0..result_set.size as usize {
            let hamming_dist = result_set.score[i] as u32;

            // Skip if distance exceeds threshold or is the exact same video (distance 0)
            if hamming_dist == 0 || hamming_dist > distance_threshold {
                continue;
            }

            // Extract video_id from result
            if let Some(Value::String(video_id)) = result_set.id.get(i) {
                similar_videos.push(SearchResult {
                    video_id: video_id.to_string(),
                    hamming_distance: hamming_dist,
                });
            }
        }
    }

    // Sort by distance (closest first)
    similar_videos.sort_by_key(|r| r.hamming_distance);

    log::debug!(
        "Found {} similar videos within threshold {}",
        similar_videos.len(),
        distance_threshold
    );

    Ok(similar_videos)
}

/// Insert a single video hash into Milvus
pub async fn insert_video_hash(
    client: &MilvusClient,
    video_id: &str,
    phash: &str,
    created_at: i64,
) -> Result<()> {
    log::debug!("Inserting video hash for video_id: {}", video_id);

    // Get the collection
    let collection = client
        .get_collection(COLLECTION_NAME)
        .await
        .context("Failed to get collection")?;

    let schema = collection.schema();

    // Convert phash to binary vector
    let phash_vector = utils::phash_to_binary_vector(phash)?;

    // Prepare data columns
    let video_id_field = schema
        .get_field("video_id")
        .context("video_id field not found")?;
    let phash_field = schema
        .get_field("phash_vector")
        .context("phash_vector field not found")?;
    let timestamp_field = schema
        .get_field("created_at")
        .context("created_at field not found")?;

    let video_ids = FieldColumn::new(video_id_field, vec![video_id.to_string()]);
    // For binary vectors, FieldColumn expects Vec<u8> (flat array of all vectors concatenated)
    let phash_vectors = FieldColumn::new(phash_field, phash_vector);
    let timestamps = FieldColumn::new(timestamp_field, vec![created_at]);

    // Insert into collection
    collection
        .insert(vec![video_ids, phash_vectors, timestamps], None)
        .await
        .context("Failed to insert into Milvus")?;

    log::debug!(
        "Successfully inserted video hash for video_id: {}",
        video_id
    );
    Ok(())
}

/// Batch insert multiple video hashes into Milvus
#[allow(dead_code)]
pub async fn insert_batch_video_hashes(
    client: &MilvusClient,
    records: Vec<VideoHashRecord>,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    log::info!("Inserting batch of {} video hashes", records.len());

    // Get the collection
    let collection = client
        .get_collection(COLLECTION_NAME)
        .await
        .context("Failed to get collection")?;

    let schema = collection.schema();

    let mut video_ids = Vec::with_capacity(records.len());
    let mut phash_vectors_flat = Vec::with_capacity(records.len() * 80); // 80 bytes per vector
    let mut timestamps = Vec::with_capacity(records.len());

    for record in records {
        video_ids.push(record.video_id);
        // Concatenate all binary vectors into a single flat Vec<u8>
        phash_vectors_flat.extend_from_slice(&record.phash_vector);
        timestamps.push(record.created_at);
    }

    let video_id_field = schema
        .get_field("video_id")
        .context("video_id field not found")?;
    let phash_field = schema
        .get_field("phash_vector")
        .context("phash_vector field not found")?;
    let timestamp_field = schema
        .get_field("created_at")
        .context("created_at field not found")?;

    let video_id_column = FieldColumn::new(video_id_field, video_ids);
    // Binary vectors are stored as a flat Vec<u8> containing all vectors concatenated
    let phash_column = FieldColumn::new(phash_field, phash_vectors_flat);
    let timestamp_column = FieldColumn::new(timestamp_field, timestamps);

    collection
        .insert(vec![video_id_column, phash_column, timestamp_column], None)
        .await
        .context("Failed to batch insert into Milvus")?;

    log::info!("Successfully inserted batch");
    Ok(())
}

/// Drop collection (for testing/cleanup)
#[allow(dead_code)]
pub async fn drop_collection(client: &MilvusClient) -> Result<()> {
    log::warn!("Dropping collection: {}", COLLECTION_NAME);

    client
        .drop_collection(COLLECTION_NAME)
        .await
        .context("Failed to drop collection")?;

    log::info!("Collection dropped successfully");
    Ok(())
}
