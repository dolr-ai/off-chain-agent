mod verify;

use std::{str::FromStr, sync::Arc, time::Duration};

use axum::{
    extract::{Path, State},
    middleware::{self},
    response::Response,
    routing::post,
    Json, Router,
};
use candid::{Decode, Encode, Nat, Principal};
use hotornot_job::{start_hotornot_job, start_hotornot_job_v2};
use http::StatusCode;
use ic_agent::{identity::DelegatedIdentity, Identity};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use serde_bytes::ByteBuf;
use tower::ServiceBuilder;
use tracing::instrument;
use verify::verify_qstash_message;
use yral_canisters_client::{
    individual_user_template::{DeployedCdaoCanisters, IndividualUserTemplate},
    sns_governance::{
        Account, Amount, Command, Command1, Disburse, DissolveState, ListNeurons, ManageNeuron,
        SnsGovernance,
    },
    sns_ledger::{Account as LedgerAccount, SnsLedger, TransferArg, TransferResult},
    sns_swap::{self, NewSaleTicketRequest, RefreshBuyerTokensRequest, SnsSwap},
};
use yral_qstash_types::{ClaimTokensRequest, ParticipateInSwapRequest};

use crate::qstash::duplicate::VideoPublisherData;
use crate::{
    app_state::AppState,
    canister::{
        delete::handle_delete_and_reclaim_canisters,
        snapshot::{
            // alert::snapshot_alert_job,
            alert::snapshot_alert_job,
            snapshot_v2::{backup_canisters_job_v2, backup_user_canister},
        },
        upgrade_user_token_sns_canister::{
            setup_sns_canisters_of_a_user_canister_for_upgrade,
            upgrade_user_token_sns_canister_for_entire_network_impl,
            upgrade_user_token_sns_canister_impl, verify_if_proposal_executed_successfully_impl,
            SnsCanisters, VerifyUpgradeProposalRequest,
        },
    },
    consts::ICP_LEDGER_CANISTER_ID,
    events::{
        event::{storj::storj_ingest, upload_video_gcs},
        nsfw::{extract_frames_and_upload, nsfw_job, nsfw_job_v2},
    },
    posts::report_post::qstash_report_post,
};

pub mod client;
pub mod duplicate;
pub mod hotornot_job;

#[derive(Clone)]
pub struct QStashState {
    decoding_key: Arc<DecodingKey>,
    validation: Arc<Validation>,
}

impl QStashState {
    pub fn init(verification_key: String) -> Self {
        let decoding_key = DecodingKey::from_secret(verification_key.as_bytes());
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&["Upstash"]);
        validation.set_audience(&[""]);
        Self {
            decoding_key: Arc::new(decoding_key),
            validation: Arc::new(validation),
        }
    }
}

async fn verify_token_root(
    agent: &ic_agent::Agent,
    user_canister: Principal,
    token_root: Principal,
) -> Result<DeployedCdaoCanisters, StatusCode> {
    let individual_user = IndividualUserTemplate(user_canister, agent);
    let tokens = individual_user
        .deployed_cdao_canisters()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    tokens
        .into_iter()
        .find(|t| t.root == token_root)
        .ok_or(StatusCode::BAD_REQUEST)
}

async fn get_user_canister(
    metadata: &yral_metadata_client::MetadataClient<true>,
    user_principal: Principal,
) -> Result<Principal, StatusCode> {
    let meta = metadata
        .get_user_metadata(user_principal)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::UNAUTHORIZED)?;

    Ok(meta.user_canister_id)
}

fn principal_to_subaccount(principal: Principal) -> ByteBuf {
    let mut subaccount = [0u8; 32];
    let principal = principal.as_slice();
    subaccount[0] = principal.len().try_into().unwrap();
    subaccount[1..1 + principal.len()].copy_from_slice(principal);

    subaccount.to_vec().into()
}

async fn participate_in_swap(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ParticipateInSwapRequest>,
) -> Result<Response, StatusCode> {
    let user_canister = get_user_canister(&state.yral_metadata_client, req.user_principal).await?;
    let cdao_cans = verify_token_root(&state.agent, user_canister, req.token_root).await?;

    let agent = &state.agent;
    let swap = SnsSwap(cdao_cans.swap, agent);

    let new_sale_ticket = swap
        .new_sale_ticket(NewSaleTicketRequest {
            amount_icp_e8s: 100_000,
            subaccount: None,
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match new_sale_ticket.result {
        Some(sns_swap::Result2::Ok(_)) => (),
        Some(sns_swap::Result2::Err(sns_swap::Err2 { error_type: 1, .. })) => {
            let resp = Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .header("Retry-After", "100")
                .body("Swap is not available".into())
                .unwrap();
            return Ok(resp);
        }
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }

    // transfer icp
    let admin_principal = agent.get_principal().unwrap();
    let subaccount = principal_to_subaccount(admin_principal);
    let transfer_args = TransferArg {
        memo: Some(vec![0].into()),
        amount: Nat::from(1000000_u64),
        fee: None,
        from_subaccount: None,
        to: LedgerAccount {
            owner: cdao_cans.swap,
            subaccount: Some(subaccount),
        },
        created_at_time: None,
    };
    let res: Vec<u8> = agent
        .update(
            &Principal::from_str(ICP_LEDGER_CANISTER_ID).unwrap(),
            "icrc1_transfer",
        )
        .with_arg(Encode!(&transfer_args).unwrap())
        .call_and_wait()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let transfer_result: TransferResult = Decode!(&res, TransferResult).unwrap();
    if let TransferResult::Err(_) = transfer_result {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    swap.refresh_buyer_tokens(RefreshBuyerTokensRequest {
        buyer: admin_principal.to_string(),
        confirmation_text: None,
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let res = Response::builder()
        .status(StatusCode::OK)
        .body("Participated in swap".into())
        .unwrap();
    Ok(res)
}

async fn claim_tokens_from_first_neuron(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ClaimTokensRequest>,
) -> Result<Response, StatusCode> {
    let identity: DelegatedIdentity = req
        .identity
        .try_into()
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let user_principal = identity
        .sender()
        .expect("Delegated identity without principal?!");

    let mut agent = state.agent.clone();
    // we need to set identity for disburse and icrc-1 transfer
    agent.set_identity(identity);

    let user_canister = get_user_canister(&state.yral_metadata_client, user_principal).await?;
    let cdao_cans = verify_token_root(&agent, user_canister, req.token_root).await?;
    let governance_principal = cdao_cans.governance;
    let ledger_principal = cdao_cans.ledger;

    let governance = SnsGovernance(governance_principal, &agent);
    let neurons = governance
        .list_neurons(ListNeurons {
            of_principal: Some(user_principal),
            limit: 10,
            start_page_at: None,
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .neurons;

    if neurons.len() < 2 || neurons[1].cached_neuron_stake_e8s == 0 {
        let res = Response::builder()
            .status(StatusCode::OK)
            .body("Claiming not required".into())
            .unwrap();
        return Ok(res);
    }
    let ix = if matches!(
        neurons[1].dissolve_state.as_ref(),
        Some(DissolveState::DissolveDelaySeconds(0))
    ) {
        1
    } else {
        0
    };

    let amount = neurons[ix].cached_neuron_stake_e8s;
    if amount == 0 {
        let res = Response::builder()
            .status(StatusCode::OK)
            .body("Claiming not required".into())
            .unwrap();
        return Ok(res);
    }
    let neuron_id = &neurons[ix].id.as_ref().ok_or(StatusCode::BAD_REQUEST)?.id;

    let mut tries = 0;
    loop {
        if tries > 10 {
            return Err(StatusCode::LOOP_DETECTED);
        }
        tries += 1;

        let manage_neuron_arg = ManageNeuron {
            subaccount: neuron_id.clone(),
            command: Some(Command::Disburse(Disburse {
                to_account: Some(Account {
                    owner: Some(user_principal),
                    subaccount: None,
                }),
                amount: Some(Amount { e8s: amount }),
            })),
        };
        let manage_neuron = governance
            .manage_neuron(manage_neuron_arg)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        match manage_neuron.command {
            Some(Command1::Disburse(_)) => break,
            Some(Command1::Error(e)) => {
                if e.error_message.contains("PreInitializationSwap") {
                    log::debug!("Governance {governance_principal} is not ready. Retrying...");
                    tokio::time::sleep(Duration::from_secs(8)).await;
                    continue;
                }
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
            _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        }
    }

    // Transfer to canister
    let ledger = SnsLedger(ledger_principal, &agent);
    // User has 50% of the overall amount
    // 20% of this 50% is 10% of the overall amount
    // 10% of the overall amount is reserveed for the canister
    let distribution_amt = Nat::from(amount) * 20u32 / 100u32;
    let transfer_resp = ledger
        .icrc_1_transfer(TransferArg {
            to: LedgerAccount {
                owner: user_canister,
                subaccount: None,
            },
            fee: None,
            memo: None,
            from_subaccount: None,
            amount: distribution_amt,
            created_at_time: None,
        })
        .await;

    match transfer_resp {
        Ok(TransferResult::Err(e)) => {
            log::error!("Token is in invalid state, user_canister: {user_canister}, governance: {governance_principal}, irrecoverable {e:?}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        Err(e) => {
            log::error!("Token is in invalid state, user_canister: {user_canister}, governance: {governance_principal}, irrecoverable {e}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
        _ => (),
    }

    let res = Response::builder()
        .status(StatusCode::OK)
        .body("Tokens claimed".into())
        .unwrap();

    Ok(res)
}

async fn upgrade_sns_creator_dao_canister(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SnsCanisters>,
) -> Result<Response, StatusCode> {
    let result =
        upgrade_user_token_sns_canister_impl(&state.agent, &state.qstash_client, req).await;

    match result {
        Ok(()) => {
            let response = Response::builder()
                .status(StatusCode::OK)
                .body(format!("upgrade proposal id submitted").into())
                .unwrap();

            Ok(response)
        }
        Err(e) => {
            log::error!(
                "Error submitting upgrade proposal to governance canister: {:?}. Error: {}",
                req.governance,
                e.to_string()
            );
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn verify_sns_canister_upgrade_proposal(
    State(state): State<Arc<AppState>>,
    Json(verify_sns_canister_proposal_request): Json<VerifyUpgradeProposalRequest>,
) -> Result<Response, StatusCode> {
    let result = verify_if_proposal_executed_successfully_impl(
        &state.agent,
        &state.qstash_client,
        verify_sns_canister_proposal_request,
    )
    .await;

    match result {
        Ok(executed) if executed => Ok(Response::builder()
            .status(StatusCode::OK)
            .body("Proposal executed successfully".into())
            .unwrap()),
        Ok(_) => Err(StatusCode::BAD_REQUEST),

        Err(e) => Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(e.to_string().into())
            .unwrap()),
    }
}

async fn upgrade_all_sns_canisters_for_a_user_canister(
    Path(individual_user_canister_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Response, StatusCode> {
    let result = setup_sns_canisters_of_a_user_canister_for_upgrade(
        &state.agent,
        &state.qstash_client,
        individual_user_canister_id,
    )
    .await;

    let res = match result {
        Ok(_) => Response::builder()
            .status(StatusCode::OK)
            .body("setup for upgrade complete".into())
            .unwrap(),
        Err(e) => Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(e.to_string().into())
            .unwrap(),
    };

    Ok(res)
}

async fn upgrade_user_token_sns_canister_for_entire_network(
    State(state): State<Arc<AppState>>,
) -> Response {
    let result =
        upgrade_user_token_sns_canister_for_entire_network_impl(&state.agent, &state.qstash_client)
            .await;

    match result {
        Ok(()) => Response::builder()
            .status(StatusCode::OK)
            .body("Upgrade Started ".into())
            .unwrap(),
        Err(e) => Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(e.to_string().into())
            .unwrap(),
    }
}

#[derive(Debug, Deserialize)]
struct VideoHashIndexingRequest {
    video_id: String,
    video_url: String,
    publisher_data: VideoPublisherData,
}

#[instrument(skip(state))]
async fn video_deduplication_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<VideoHashIndexingRequest>,
) -> Result<Response, StatusCode> {
    log::info!(
        "Processing video deduplication for video ID: {}",
        req.video_id
    );

    let publisher_data = VideoPublisherData {
        canister_id: req.publisher_data.canister_id.clone(),
        publisher_principal: req.publisher_data.publisher_principal.clone(),
        post_id: req.publisher_data.post_id,
    };

    let duplication_handler = duplicate::VideoHashDuplication::new(
        &state.qstash_client.client,
        &state.qstash_client.base_url,
    );

    let qstash_client = state.qstash_client.clone();

    if let Err(e) = duplication_handler
        .process_video_deduplication(
            &state.dedup_index_ctx,
            &state.bigquery_client,
            &req.video_id,
            &req.video_url,
            publisher_data,
            move |vid_id, canister_id, post_id, timestamp, publisher_user_id| {
                // Clone the values to ensure they have 'static lifetime
                let vid_id = vid_id.to_string();
                let canister_id = canister_id.to_string();
                let publisher_user_id = publisher_user_id.to_string();

                // Use the cloned qstash_client instead of accessing through state
                let qstash_client = qstash_client.clone();

                Box::pin(async move {
                    qstash_client
                        .publish_video(
                            &vid_id,
                            &canister_id,
                            post_id,
                            timestamp,
                            &publisher_user_id,
                        )
                        .await
                })
            },
        )
        .await
    {
        log::error!("Video deduplication failed: {}", e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let response = Response::builder()
        .status(StatusCode::OK)
        .body("Video deduplication check completed".into())
        .unwrap();

    Ok(response)
}

#[instrument(skip(app_state))]
// QStash router remains the same but without the admin route
pub fn qstash_router<S>(app_state: Arc<AppState>) -> Router<S> {
    Router::new()
        .route("/claim_tokens", post(claim_tokens_from_first_neuron))
        .route("/participate_in_swap", post(participate_in_swap))
        .route(
            "/upgrade_sns_creator_dao_canister",
            post(upgrade_sns_creator_dao_canister),
        )
        .route("/video_deduplication", post(video_deduplication_handler))
        .route("/upload_video_gcs", post(upload_video_gcs))
        .route("/enqueue_video_frames", post(extract_frames_and_upload))
        .route("/enqueue_video_nsfw_detection", post(nsfw_job))
        .route("/enqueue_video_nsfw_detection_v2", post(nsfw_job_v2))
        .route(
            "/verify_sns_canister_upgrade_proposal",
            post(verify_sns_canister_upgrade_proposal),
        )
        .route(
            "/upgrade_all_sns_canisters_for_a_user_canister/{individual_user_canister_id}",
            post(upgrade_all_sns_canisters_for_a_user_canister),
        )
        .route(
            "/upgrade_user_token_sns_canister_for_entire_network",
            post(upgrade_user_token_sns_canister_for_entire_network),
        )
        .route("/report_post", post(qstash_report_post))
        .route("/storj_ingest", post(storj_ingest))
        .route(
            "/start_backup_canisters_job_v2",
            post(backup_canisters_job_v2),
        )
        .route("/backup_user_canister", post(backup_user_canister))
        .route("/snapshot_alert_job", post(snapshot_alert_job))
        .route("/start_hotornot_job", post(start_hotornot_job))
        .route("/start_hotornot_job_v2", post(start_hotornot_job_v2))
        .route(
            "/delete_and_reclaim_canisters",
            post(handle_delete_and_reclaim_canisters),
        )
        .layer(ServiceBuilder::new().layer(middleware::from_fn_with_state(
            app_state.qstash.clone(),
            verify_qstash_message,
        )))
        .with_state(app_state)
}
