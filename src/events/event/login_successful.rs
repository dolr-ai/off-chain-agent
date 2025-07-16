use candid::Principal;
use google_cloud_bigquery::{
    client::Client,
    http::{
        job::query::QueryRequest,
        tabledata::insert_all::{InsertAllRequest, Row},
    },
    query::row::Row as QueryRow,
};
use serde::{Deserialize, Serialize};
use tracing::instrument;

#[derive(Serialize, Deserialize, Clone)]
pub struct UserCanisterPrincipal {
    pub canister_id: String,
    pub user_principal_id: String,
}

// Note : temporary function to insert user canister principal into bigquery
// for backfill
// also Note : this is not taking care of canister fungibility
#[instrument(skip(bq_client))]
pub async fn handle_login_successful(
    bq_client: Client,
    canister_id: Principal,
    user_id: Principal,
) -> Result<(), anyhow::Error> {
    // check if its unique
    let request = QueryRequest {
        query: format!(
            "SELECT * FROM `hot-or-not-feed-intelligence.yral_ds.canister_user_principal` WHERE canister_id = '{}' AND user_principal_id = '{}'",
            canister_id,
            user_id
        ),
        ..Default::default()
    };
    let mut response = bq_client
        .query::<QueryRow>("hot-or-not-feed-intelligence", request)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to query canister_user_principal: {}", e))?;
    let mut res_list = Vec::new();
    while let Some(row) = response.next().await? {
        res_list.push(row);
    }
    // if already exists, return
    if !res_list.is_empty() {
        return Ok(());
    }

    let login_successful_request = UserCanisterPrincipal {
        canister_id: canister_id.to_string(),
        user_principal_id: user_id.to_string(),
    };

    // insert into bigquery
    let request = InsertAllRequest {
        rows: vec![Row {
            insert_id: None,
            json: login_successful_request,
        }],
        ..Default::default()
    };

    let res = bq_client
        .tabledata()
        .insert(
            "hot-or-not-feed-intelligence",
            "yral_ds",
            "canister_user_principal",
            &request,
        )
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to insert canister_user_principal row to bigquery: {}",
                e
            )
        })?;

    if let Some(errors) = res.insert_errors {
        if !errors.is_empty() {
            log::error!("canister_user_principal insert response : {:?}", errors);
            return Err(anyhow::anyhow!(
                "Failed to insert canister_user_principal row to bigquery"
            ));
        }
    }

    Ok(())
}
