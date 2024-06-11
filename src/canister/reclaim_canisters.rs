use std::{borrow::Borrow, env, sync::Arc, time::SystemTime};

use axum::{
    extract::State,
    response::{Html, Response},
};
use candid::{encode_args, Principal};
use futures::prelude::*;
use http::StatusCode;
use ic_agent::Agent;
use serde::Serialize;

use crate::{auth::AuthBearer, consts::RECYCLE_THRESHOLD_SECS, types::SessionType, AppState};

use super::utils::get_user_and_canisters_list;

pub async fn reclaim_canisters_handler(
    State(state): State<Arc<AppState>>,
    AuthBearer(token): AuthBearer,
) -> Html<&'static str> {
    if token != "reclaim_token_test_tmp" {
        return Html("Unauthorized");
    }

    let state = Arc::clone(&state);

    tokio::spawn(async move {
        let pk = env::var("RECLAIM_CANISTER_PEM").expect("$RECLAIM_CANISTER_PEM is not set");

        let identity = match ic_agent::identity::BasicIdentity::from_pem(
            stringreader::StringReader::new(pk.as_str()),
        ) {
            Ok(identity) => identity,
            Err(err) => {
                println!("Unable to create identity, error: {:?}", err);
                return Html("Unable to create identity");
            }
        };

        // let identity = match ic_agent::identity::Secp256k1Identity::from_pem_file(
        //     "/Users/komalsai/Downloads/generated-id.pem",
        // ) {
        //     Ok(identity) => identity,
        //     Err(err) => {
        //         println!("Unable to create identity, error: {:?}", err);
        //         return Html("Unable to create identity");
        //     }
        // };

        let agent = match Agent::builder()
            .with_url("https://a4gq6-oaaaa-aaaab-qaa4q-cai.raw.ic0.app/") // local : http://127.0.0.1:4943
            .with_identity(identity)
            .build()
        {
            Ok(agent) => agent,
            Err(err) => {
                println!("Unable to create agent, error: {:?}", err);
                return Html("Unable to create agent");
            }
        };
        // ‼️‼️comment below line in mainnet‼️‼️
        // agent.fetch_root_key().await.unwrap();

        let user_canisters_map = match get_user_and_canisters_list(&agent).await {
            Ok(user_canisters_map) => user_canisters_map,
            Err(err) => {
                println!("Unable to get user canisters map, error: {:?}", err);
                return Html("Unable to get user canisters map");
            }
        };

        for (subnet_orchestrator_id, user_canisters_list) in user_canisters_map.iter() {
            let futures = user_canisters_list
                .iter()
                .map(|(user_id, canister_id)| async {
                    filter_canister(&agent.clone(), user_id, canister_id).await
                });

            let stream = futures::stream::iter(futures).boxed().buffer_unordered(100);

            let results = stream
                .collect::<Vec<Option<(Principal, Principal)>>>()
                .await;

            let shortlisted_canisters = results
                .into_iter()
                .filter_map(|x| x)
                .collect::<Vec<(Principal, Principal)>>();

            let canister_ids = shortlisted_canisters
                .iter()
                .map(|(_, canister_id)| *canister_id)
                .collect::<Vec<Principal>>();

            // test
            log::info!(
                "Reclaiming canisters for subnet orchestrator: {:?}, canister_ids: {:?}",
                subnet_orchestrator_id,
                canister_ids
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>()
            );
            log::info!("Num {}/{}", canister_ids.len(), user_canisters_list.len());

            // call subnet orchestrator to reclaim canisters

            // let response = match agent
            //     .update(subnet_orchestrator_id, "reset_user_individual_canisters")
            //     .with_arg(encode_args((canister_ids,)).unwrap())
            //     .call_and_wait()
            //     .await
            // {
            //     Ok(response) => response,
            //     Err(err) => {
            //         println!(
            //                 "Unable to call the method recycle_canisters, error: {:?}, subnet_orchestrator_id {:?}",
            //                 err,
            //                 subnet_orchestrator_id.to_string()
            //             );
            //         return Html("Unable to call the method recycle_canisters");
            //     }
            // };

            // let res = match candid::decode_one(&response) {
            //     Ok(result) => {
            //         let result: Result<String, String> = result;
            //         match result {
            //             Ok(result) => result,
            //             Err(err) => {
            //                 println!(
            //                     "Error in decoding the response recycle_canisters, error: {:?}, subnet_orchestrator_id {:?}",
            //                     err,
            //                     subnet_orchestrator_id.to_string()
            //                 );
            //                 return Html("Error in decoding the response recycle_canisters");
            //             }
            //         }
            //     }
            //     Err(err) => {
            //         println!(
            //             "Error in decoding the response recycle_canisters, error: {:?}, subnet_orchestrator_id {:?}",
            //             err,
            //             subnet_orchestrator_id.to_string()
            //         );
            //         return Html("Error in decoding the response recycle_canisters");
            //     }
            // };
            // println!("Response from subnet orchestrator: {:?}", res);

            // call yral-metadata to delete keys

            // let yral_metadata_client = state.yral_metadata_client.clone();
            // match yral_metadata_client
            //     .delete_metadata_bulk(user_ids)
            //     .await
            // {
            //     Ok(_) => {}
            //     Err(err) => {
            //         log::error!("Error calling delete_metadata_bulk, error: {:?}", err);
            //         return Html("Error calling the method delete_metadata_bulk");
            //     }
            // }
        }
        Html("Reclaim canisters - OK")
    });

    Html("Reclaim canisters - OK")
}

async fn filter_canister(
    agent: &Agent,
    user_id: &Principal,
    canister_id: &Principal,
) -> Option<(Principal, Principal)> {
    // Call get_session_type
    let response = match agent
        .query(canister_id, "get_session_type")
        .with_arg(encode_args(()).unwrap())
        .call()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            log::error!(
                "Unable to call the method get_session_type, error: {:?}, canister_id {:?}",
                err,
                canister_id.to_string()
            );
            return None;
        }
    };

    let session_type = match candid::decode_one(&response) {
        Ok(result) => {
            let result: Result<SessionType, String> = result;
            match result {
                Ok(result) => result,
                Err(err) => {
                    log::error!(
                        "Error in decoding the response get_session_type, error: {:?}, canister_id {:?}",
                        err,
                        canister_id.to_string()
                    );
                    return None;
                }
            }
        }
        Err(err) => {
            log::error!(
                "Error in decoding the response get_session_type, error: {:?}, canister_id {:?}",
                err,
                canister_id.to_string()
            );
            return None;
        }
    };
    if session_type == SessionType::RegisteredSession {
        return None;
    }

    // Call get_last_canister_functionality_access_time
    let response = match agent
        .query(canister_id, "get_last_canister_functionality_access_time")
        .with_arg(encode_args(()).unwrap())
        .call()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            log::error!(
                "Unable to call the method get_last_canister_functionality_access_time, error: {:?}, canister_id {:?}",
                err,
                canister_id.to_string()
            );
            return None;
        }
    };

    let response_decoded = match candid::decode_one(&response) {
        Ok(result) => {
            let result: Result<SystemTime, String> = result;
            match result {
                Ok(result) => result,
                Err(err) => {
                    log::error!(
                        "Error in decoding the response get_last_canister_functionality_access_time, error: {:?}, canister_id {:?}",
                        err,
                        canister_id.to_string()
                    );
                    return None;
                }
            }
        }
        Err(err) => {
            log::error!(
                "Unable to decode the response get_last_canister_functionality_access_time, error: {:?}, canister_id {:?}",
                err,
                canister_id.to_string()
            );
            return None;
        }
    };

    // If the last access time is more than RECYCLE_THRESHOLD_SECS, return the canister_id
    if response_decoded.elapsed().unwrap().as_secs() > RECYCLE_THRESHOLD_SECS {
        return Some((user_id.clone(), canister_id.clone()));
    }

    None
}

// pub async fn key_test(
//     State(state): State<Arc<AppState>>,
//     AuthBearer(token): AuthBearer,
// ) -> Html<&'static str> {
//     if token != "token_test_tmp" {
//         return Html("Unauthorized");
//     }

//     let state = Arc::clone(&state);

//     let yral_metadata_client = state.yral_metadata_client.clone();
//     match yral_metadata_client
//         .delete_metadata_bulk(vec![Principal::from_text(
//             "62p2r-n2ihk-mcjc7-hxjki-stvna-73xka-hazwu-il3fw-5e4p7-wuay5-mae",
//         )
//         .unwrap()])
//         .await
//     {
//         Ok(_) => {}
//         Err(err) => {
//             log::error!("Error calling delete_metadata_bulk, error: {:?}", err);
//             return Html("Error calling the method delete_metadata_bulk");
//         }
//     }

//     Html("Key test - OK")
// }
