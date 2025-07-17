use axum::{
    extract::{Path, State},
    Json,
};
use candid::{CandidType, Principal};
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use hex::ToHex;
use ic_agent::Agent;
use ic_sns_governance::init::GovernanceCanisterInitPayloadBuilder;
use serde::{Deserialize, Serialize};
use std::{error::Error, sync::Arc, time::Duration, vec};
use yral_canisters_client::{
    individual_user_template::{DeployedCdaoCanisters, IndividualUserTemplate},
    platform_orchestrator::{PlatformOrchestrator},
    sns_governance::{
        self, Action, Command1, Configure, Follow, GetProposal, GetRunningSnsVersionArg,
        IncreaseDissolveDelay, ListNeurons, ManageNeuron, NeuronId, Operation, Proposal,
        ProposalId, SnsGovernance, Version,
    },
    sns_root::{GetSnsCanistersSummaryRequest, SnsRoot},
    user_index::UserIndex,
};

use ic_utils::interfaces::management_canister::{
        builders::InstallMode,
        ManagementCanister,
    };

use crate::{consts::PLATFORM_ORCHESTRATOR_ID, qstash::client::QStashClient};

use crate::app_state::AppState;
use crate::utils::api_response::ApiResponse;

#[derive(Clone, Debug, CandidType, Deserialize)]
pub enum IndexArg {
    Init(InitArg),
    Upgrade(UpgradeArg),
}

#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct InitArg {
    pub ledger_id: Principal,
    pub retrieve_blocks_from_ledger_interval_seconds: Option<u64>,
}

#[derive(Clone, Debug, CandidType, Deserialize)]
pub struct UpgradeArg {
    pub ledger_id: Option<Principal>,
    pub retrieve_blocks_from_ledger_interval_seconds: Option<u64>,
}

pub const SNS_TOKEN_GOVERNANCE_MODULE_HASH: &str =
    "51fd3d1a529f3f7bad808b19074e761ce3538282ac8189bd7067b4156360c279";
pub const SNS_TOKEN_LEDGER_MODULE_HASH: &str =
    "3d808fa63a3d8ebd4510c0400aa078e99a31afaa0515f0b68778f929ce4b2a46";
pub const SNS_TOKEN_ROOT_MODULE_HASH: &str =
    "431cb333feb3f762f742b0dea58745633a2a2ca41075e9933183d850b4ddb259";
pub const SNS_TOKEN_SWAP_MODULE_HASH: &str =
    "8313ac22d2ef0a0c1290a85b47f235cfa24ca2c96d095b8dbed5502483b9cd18";
pub const SNS_TOKEN_INDEX_MODULE_HASH: &str =
    "67b5f0bf128e801adf4a959ea26c3c9ca0cd399940e169a26a2eb237899a94dd";
pub const SNS_TOKEN_ARCHIVE_MODULE_HASH: &str =
    "317771544f0e828a60ad6efc97694c425c169c4d75d911ba592546912dba3116";

const MINIMUM_RECHARGE_AMOUNT_TO_RUN_SNS_UPGRADE: u128 = 1_000_000_000_000; //1T
const INITIAL_RECHARGE_AMOUNT: u128 = 300_000_000_000; //0.3T

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct VerifyUpgradeProposalRequest {
    pub sns_canisters: SnsCanisters,
    pub proposal_id: u64,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct SnsCanisters {
    pub governance: Principal,
    pub index: Principal,
    pub swap: Principal,
    pub root: Principal,
    pub ledger: Principal,
}

impl From<DeployedCdaoCanisters> for SnsCanisters {
    fn from(value: DeployedCdaoCanisters) -> Self {
        Self {
            governance: value.governance,
            index: value.index,
            swap: value.swap,
            root: value.root,
            ledger: value.ledger,
        }
    }
}

pub async fn upgrade_user_token_sns_canister_for_entire_network(
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<()>> {
    let result = state
        .qstash_client
        .upgrade_user_token_sns_canister_for_entire_network()
        .await
        .map_err(|e| e.into());

    Json(ApiResponse::from(result))
}

pub async fn upgrade_user_token_sns_canister_for_entire_network_impl(
    agent: &Agent,
    qstash_client: &QStashClient,
) -> Result<(), Box<dyn Error>> {
    let platform_orchestrator = Principal::from_text(PLATFORM_ORCHESTRATOR_ID).unwrap();
    let mut individual_canister_ids: Vec<Principal> = vec![];

    let platform_orchestrator = PlatformOrchestrator(platform_orchestrator, agent);

    let subnet_orchestrators = platform_orchestrator.get_all_subnet_orchestrators().await?;

    for subnet_orchestrator in subnet_orchestrators {
        let subnet_orchestrator = UserIndex(subnet_orchestrator, agent);
        individual_canister_ids.extend(subnet_orchestrator.get_user_canister_list().await?);
    }

    let upgrade_governance_canister_tasks =
        individual_canister_ids
            .into_iter()
            .map(|individual_canister| async move {
                let individual_canister_template =
                    IndividualUserTemplate(individual_canister, agent);

                let deployed_cdao_canisters_res =
                    individual_canister_template.deployed_cdao_canisters().await;

                let deployed_cdao_canisters_len = deployed_cdao_canisters_res
                    .map(|res| res.len())
                    .unwrap_or(0);

                if deployed_cdao_canisters_len > 0 {
                    qstash_client
                        .upgrade_all_sns_canisters_for_a_user_canister(
                            individual_canister.to_text(),
                        )
                        .await
                } else {
                    Ok(())
                }
            });

    let stream = futures::stream::iter(upgrade_governance_canister_tasks)
        .boxed()
        .buffer_unordered(100);

    let _upgrade_creator_dao_governance_canister_tasks =
        stream.collect::<Vec<Result<(), anyhow::Error>>>().await;

    Ok(())
}

pub async fn upgrade_user_token_sns_canister_handler(
    Path(user_canister_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Json<ApiResponse<()>> {
    let setup_for_upgrade_result = setup_sns_canisters_of_a_user_canister_for_upgrade(
        &state.agent,
        &state.qstash_client,
        user_canister_id,
    )
    .await;

    Json(ApiResponse::from(setup_for_upgrade_result))
}

pub async fn setup_sns_canisters_of_a_user_canister_for_upgrade(
    agent: &Agent,
    qstash_client: &QStashClient,
    individual_canister_id: String,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let individual_canister_principal =
        Principal::from_text(individual_canister_id).map_err(|e| e.to_string())?;

    let individual_user_template = IndividualUserTemplate(individual_canister_principal, agent);

    let deployed_canisters = individual_user_template
        .deployed_cdao_canisters()
        .await
        .map_err(|e| e.to_string())?;

    let sns_canisters: Vec<SnsCanisters> = deployed_canisters
        .into_iter()
        .map(SnsCanisters::from)
        .collect();

    sns_canisters
        .into_iter()
        .map(|sns_canisters| async move {
            recharge_canisters(agent, sns_canisters).await?;
            setup_neurons_for_admin_principal(agent, sns_canisters).await?;
            qstash_client
                .upgrade_sns_creator_dao_canister(sns_canisters)
                .await
                .map_err(<anyhow::Error as Into<Box<dyn Error + Send + Sync>>>::into)?;

            Ok::<(), Box<dyn Error + Send + Sync>>(())
        })
        .collect::<FuturesUnordered<_>>()
        .try_collect::<()>()
        .await?;

    Ok(())
}

pub async fn verify_if_proposal_executed_successfully_impl(
    agent: &Agent,
    qstash_client: &QStashClient,
    verify_proposal_request: VerifyUpgradeProposalRequest,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let sns_governance = SnsGovernance(verify_proposal_request.sns_canisters.governance, agent);

    let proposal_executed_successfully = check_if_the_proposal_executed_successfully(
        &sns_governance,
        verify_proposal_request.proposal_id,
    )
    .await?;

    if proposal_executed_successfully {
        qstash_client
            .upgrade_sns_creator_dao_canister(verify_proposal_request.sns_canisters)
            .await?;
    }

    Ok(proposal_executed_successfully)
}

async fn upgrade_sns_governance_canister_with_custom_wasm(
    agent: &Agent,
    governance_canister_id: Principal,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    recharge_for_upgrade_using_platform_orchestrator(agent, governance_canister_id).await?;

    let management_canister = ManagementCanister::create(agent);

    let governance_init_payload = GovernanceCanisterInitPayloadBuilder::new().build();

    management_canister
        .stop_canister(&governance_canister_id)
        .await?;

    let custom_governance_wasm = include_bytes!("./wasms/custom-governance-canister.wasm.gz");

    let upgrade_result = management_canister
        .install_code(&governance_canister_id, custom_governance_wasm)
        .with_mode(InstallMode::Upgrade(None))
        .with_arg(governance_init_payload)
        .build()?
        .await
        .map_err(|e| e.into());

    management_canister
        .start_canister(&governance_canister_id)
        .await?;

    //wait for the canister to startup
    tokio::time::sleep(Duration::from_secs(5)).await;

    upgrade_result
}

async fn install_wasm_in_index_canister_if_not_present(
    agent: &Agent,
    sns_canisters: SnsCanisters,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let management_canister = ManagementCanister::create(agent);

    let index_cansier_info = management_canister
        .canister_status(&sns_canisters.index)
        .await
        .map(|res| res.0.module_hash)?;

    if index_cansier_info.is_some() {
        return Ok(());
    }

    recharge_for_upgrade_using_platform_orchestrator(agent, sns_canisters.index).await?;

    let index_ng_init_args = Some(IndexArg::Init(InitArg {
        ledger_id: sns_canisters.ledger,
        retrieve_blocks_from_ledger_interval_seconds: None,
    }));

    management_canister
        .stop_canister(&sns_canisters.index)
        .await?;

    let index_canister_wasm = include_bytes!("./wasms/index.wasm.gz");

    let upgrade_result = management_canister
        .install_code(&sns_canisters.index, index_canister_wasm)
        .with_mode(InstallMode::Install)
        .with_arg(index_ng_init_args)
        .build()?
        .await
        .map_err(|e| e.into());

    management_canister
        .start_canister(&sns_canisters.index)
        .await?;

    //wait for the canister to startup
    tokio::time::sleep(Duration::from_secs(5)).await;

    upgrade_result
}

async fn setup_neurons_for_admin_principal(
    agent: &Agent,
    sns_canisters: SnsCanisters,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let governance_canister_id = sns_canisters.governance;
    let sns_governance = SnsGovernance(governance_canister_id, agent);

    let sns_version_res = sns_governance
        .get_running_sns_version(GetRunningSnsVersionArg {})
        .await?;

    if sns_version_res.deployed_version.is_none() {
        upgrade_sns_governance_canister_with_custom_wasm(agent, governance_canister_id).await?;
    }

    install_wasm_in_index_canister_if_not_present(agent, sns_canisters).await?;

    let neuron_list = sns_governance
        .list_neurons(ListNeurons {
            of_principal: Some(agent.get_principal().unwrap()),
            limit: 10,
            start_page_at: None,
        })
        .await
        .map_err(|e| e.to_string())?
        .neurons;

    let first_neuron = neuron_list.first()
        .ok_or("first neuron not found")?
        .id
        .as_ref()
        .ok_or("first neuronId not found")?;

    let second_neuron = neuron_list
        .get(1)
        .ok_or("second neuron not found")?
        .id
        .as_ref()
        .ok_or("second neuronId not found")?;

    let _set_dissolve_delay = sns_governance
        .manage_neuron(ManageNeuron {
            subaccount: first_neuron.id.clone(),
            command: Some(sns_governance::Command::Configure(Configure {
                operation: Some(Operation::IncreaseDissolveDelay(IncreaseDissolveDelay {
                    additional_dissolve_delay_seconds: 172800,
                })),
            })),
        })
        .await
        .map_err(|e| format!("{e:?}"))?;

    let _set_dissolve_delay = sns_governance
        .manage_neuron(ManageNeuron {
            subaccount: second_neuron.id.clone(),
            command: Some(sns_governance::Command::Configure(Configure {
                operation: Some(Operation::IncreaseDissolveDelay(IncreaseDissolveDelay {
                    additional_dissolve_delay_seconds: 172800,
                })),
            })),
        })
        .await
        .map_err(|e| format!("{e:?}"))?;

    let function_id_for_upgrading_sns_to_next_version = sns_governance
        .list_nervous_system_functions()
        .await
        .map_err(|e| e.to_string())?
        .functions
        .iter()
        .find(|function| function.name.contains("Upgrade SNS to next version"))
        .map(|function| function.id)
        .ok_or("function id for upgrade sns to next version not found")?;

    let _second_neuron_follow_first_neuron_result = sns_governance
        .manage_neuron(ManageNeuron {
            subaccount: second_neuron.id.clone(),
            command: Some(sns_governance::Command::Follow(Follow {
                function_id: function_id_for_upgrading_sns_to_next_version,
                followees: vec![NeuronId {
                    id: first_neuron.id.clone(),
                }],
            })),
        })
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

async fn recharge_canister_using_platform_orchestrator(
    agent: &Agent,
    canister_id: Principal,
    amount: u128,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let platform_orchestrator_principal = Principal::from_text(PLATFORM_ORCHESTRATOR_ID).unwrap();
    let platform_orchestrator = PlatformOrchestrator(platform_orchestrator_principal, agent);
    platform_orchestrator
        .deposit_cycles_to_canister(canister_id, candid::Nat::from(amount))
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

async fn recharge_for_upgrade_using_platform_orchestrator(
    agent: &Agent,
    canister_id: Principal,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    recharge_canister_using_platform_orchestrator(
        agent,
        canister_id,
        500_000_000_000_u128, // 0.5T
    )
    .await
    .map_err(|e| e.to_string())?;

    Ok(())
}

fn check_if_version_matches_deployed_canister_version(deployed_version: Version) -> bool {
    let governance_hash = deployed_version
        .governance_wasm_hash
        .to_vec()
        .encode_hex::<String>();

    let index_hash = deployed_version
        .index_wasm_hash
        .to_vec()
        .encode_hex::<String>();

    let swap_hash = deployed_version
        .swap_wasm_hash
        .to_vec()
        .encode_hex::<String>();

    let ledger_hash = deployed_version
        .ledger_wasm_hash
        .to_vec()
        .encode_hex::<String>();

    let root_hash = deployed_version
        .root_wasm_hash
        .to_vec()
        .encode_hex::<String>();

    let archive_hash = deployed_version
        .archive_wasm_hash
        .to_vec()
        .encode_hex::<String>();

    let hashes = [governance_hash,
        index_hash,
        swap_hash,
        ledger_hash,
        root_hash,
        archive_hash];

    let final_hashes = [SNS_TOKEN_ARCHIVE_MODULE_HASH.to_owned(),
        SNS_TOKEN_GOVERNANCE_MODULE_HASH.to_owned(),
        SNS_TOKEN_INDEX_MODULE_HASH.to_owned(),
        SNS_TOKEN_LEDGER_MODULE_HASH.to_owned(),
        SNS_TOKEN_ROOT_MODULE_HASH.to_owned(),
        SNS_TOKEN_SWAP_MODULE_HASH.to_owned()];

    let result = hashes.iter().all(|val| final_hashes.contains(val));

    result
}

pub async fn is_upgrade_required(
    sns_governance: &SnsGovernance<'_>,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let deployed_version = sns_governance
        .get_running_sns_version(GetRunningSnsVersionArg {})
        .await?;

    let deployed_version = deployed_version
        .deployed_version
        .ok_or("deployed version not found")?;

    let result = !check_if_version_matches_deployed_canister_version(deployed_version);

    Ok(result)
}

pub async fn check_if_the_proposal_executed_successfully(
    sns_governance: &SnsGovernance<'_>,
    proposal_id: u64,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    let proposal_result = sns_governance
        .get_proposal(GetProposal {
            proposal_id: Some(ProposalId { id: proposal_id }),
        })
        .await
        .map_err(|e| e.to_string())?;

    if let Some(proposal_result) = proposal_result.result {
        match proposal_result {
            sns_governance::Result1::Proposal(res) => Ok(res.executed_timestamp_seconds != 0),
            sns_governance::Result1::Error(e) => Err(e.error_message.into()),
        }
    } else {
        Err("Proposal not found".to_owned().into())
    }
}

pub async fn recharge_canisters(
    agent: &Agent,
    deployed_canisters: SnsCanisters,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut recharge_canister_tasks = vec![];

    recharge_canister_tasks.push(recharge_canister_using_platform_orchestrator(
        agent,
        deployed_canisters.governance,
        INITIAL_RECHARGE_AMOUNT,
    ));

    recharge_canister_tasks.push(recharge_canister_using_platform_orchestrator(
        agent,
        deployed_canisters.index,
        INITIAL_RECHARGE_AMOUNT,
    ));
    recharge_canister_tasks.push(recharge_canister_using_platform_orchestrator(
        agent,
        deployed_canisters.ledger,
        INITIAL_RECHARGE_AMOUNT,
    ));
    recharge_canister_tasks.push(recharge_canister_using_platform_orchestrator(
        agent,
        deployed_canisters.root,
        INITIAL_RECHARGE_AMOUNT,
    ));
    recharge_canister_tasks.push(recharge_canister_using_platform_orchestrator(
        agent,
        deployed_canisters.swap,
        INITIAL_RECHARGE_AMOUNT,
    ));

    recharge_canister_tasks
        .into_iter()
        .collect::<FuturesUnordered<_>>()
        .try_collect::<()>()
        .await?;

    Ok(())
}

async fn recharge_if_sns_canister_threshold(
    agent: &Agent,
    canister_id: Principal,
    cycle_balance: u128,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if cycle_balance < MINIMUM_RECHARGE_AMOUNT_TO_RUN_SNS_UPGRADE {
        let amount = MINIMUM_RECHARGE_AMOUNT_TO_RUN_SNS_UPGRADE.saturating_sub(cycle_balance);
        recharge_canister_using_platform_orchestrator(agent, canister_id, amount).await?;
    }

    Ok(())
}

async fn check_and_recharge_canisters_for_sns_upgrade(
    agent: &Agent,
    sns_canisters: SnsCanisters,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let root_canister = SnsRoot(sns_canisters.root, agent);
    let sns_canister_summary = root_canister
        .get_sns_canisters_summary(GetSnsCanistersSummaryRequest {
            update_canister_list: None,
        })
        .await?;

    let root_canister_cycle_balance = sns_canister_summary
        .root
        .and_then(|canister_res| {
            canister_res
                .status
                .map(|canister_status| u128::try_from(canister_status.cycles.0).unwrap())
        });

    if let Some(root_canister_balance) = root_canister_cycle_balance {
        let _ =
            recharge_if_sns_canister_threshold(agent, sns_canisters.root, root_canister_balance)
                .await;
    }

    let swap_canister_cycle_balance = sns_canister_summary
        .swap
        .and_then(|canister_res| {
            canister_res
                .status
                .map(|canister_status| u128::try_from(canister_status.cycles.0).unwrap())
        });

    if let Some(swap_canister_balance) = swap_canister_cycle_balance {
        let _ =
            recharge_if_sns_canister_threshold(agent, sns_canisters.swap, swap_canister_balance)
                .await;
    }

    let ledger_canister_cycle_balance = sns_canister_summary
        .ledger
        .and_then(|canister_res| {
            canister_res
                .status
                .map(|canister_status| u128::try_from(canister_status.cycles.0).unwrap())
        });

    if let Some(ledger_canister_balance) = ledger_canister_cycle_balance {
        let _ = recharge_if_sns_canister_threshold(
            agent,
            sns_canisters.ledger,
            ledger_canister_balance,
        )
        .await;
    }

    let index_cansiter_cycle_balance = sns_canister_summary
        .index
        .and_then(|canister_res| {
            canister_res
                .status
                .map(|canister_status| u128::try_from(canister_status.cycles.0).unwrap())
        });

    if let Some(index_canister_balance) = index_cansiter_cycle_balance {
        let _ =
            recharge_if_sns_canister_threshold(agent, sns_canisters.index, index_canister_balance)
                .await;
    }

    let governance_canister_cycle_balance = sns_canister_summary
        .governance
        .and_then(|canister_res| {
            canister_res
                .status
                .map(|canister_status| u128::try_from(canister_status.cycles.0).unwrap())
        });

    if let Some(governance_canister_balance) = governance_canister_cycle_balance {
        let _ = recharge_if_sns_canister_threshold(
            agent,
            sns_canisters.governance,
            governance_canister_balance,
        )
        .await;
    }

    Ok(())
}

pub async fn upgrade_user_token_sns_canister_impl(
    agent: &Agent,
    qstash_client: &QStashClient,
    sns_canisters: SnsCanisters,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let sns_governance = SnsGovernance(sns_canisters.governance, agent);

    let is_upgrade_required = is_upgrade_required(&sns_governance).await?;

    if !is_upgrade_required {
        return Ok(());
    }

    check_and_recharge_canisters_for_sns_upgrade(agent, sns_canisters).await?;

    let neuron_list = sns_governance
        .list_neurons(ListNeurons {
            of_principal: Some(agent.get_principal().unwrap()),
            limit: 10,
            start_page_at: None,
        })
        .await
        .map_err(|e| e.to_string())?
        .neurons;

    let first_neuron = neuron_list.first()
        .ok_or("first neuron not found")?
        .id
        .as_ref()
        .ok_or("first neuronId not found")?;

    let proposal_id = sns_governance
        .manage_neuron(ManageNeuron {
            subaccount: first_neuron.id.clone(),
            command: Some(sns_governance::Command::MakeProposal(Proposal {
                url: "yral.com".to_owned(),
                title: "Upgrade SNS for token".into(),
                action: Some(Action::UpgradeSnsToNextVersion {}),
                summary: "Upgrading canisters".to_owned(),
            })),
        })
        .await?
        .command
        .unwrap();

    if let Command1::MakeProposal(proposal_id) = proposal_id {
        let proposal_id_u64 = proposal_id.proposal_id.ok_or("proposal id not found")?.id;

        let verify_request = VerifyUpgradeProposalRequest {
            sns_canisters,
            proposal_id: proposal_id_u64,
        };

        qstash_client
            .verify_sns_canister_upgrade_proposal(verify_request)
            .await?;
        Ok(())
    } else {
        Err(format!("{proposal_id:?}").into())
    }
}
