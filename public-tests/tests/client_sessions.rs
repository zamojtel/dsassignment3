use ntest::timeout;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::{Duration, Instant};

use module_system::System;

use assignment_3_solution::*;
use assignment_3_test_utils::*;

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn client_sessions_give_exactly_once_semantics() {
    let mut system = System::new().await;
    let (apply_sender, mut apply_receiver) = unbounded_channel();
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();
    let leader = Raft::new(
        &mut system,
        make_config(
            leader_id,
            boot,
            Duration::from_millis(100),
            &processes.clone(),
        ),
        Box::new(SpyMachine { apply_sender }),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, boot, Duration::from_millis(300), &processes),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    sender.insert(leader_id, Box::new(leader.clone())).await;
    sender.insert(follower_id, Box::new(follower.clone())).await;
    let (result_sender, mut result_receiver) = unbounded_channel();

    sleep_ms(150).await;

    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    let mut responses = vec![];
    responses.push(result_receiver.recv().await.unwrap());
    responses.push(result_receiver.recv().await.unwrap());
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    responses.push(result_receiver.recv().await.unwrap());

    for response in responses {
        assert_eq!(
            response,
            ClientRequestResponse::CommandResponse(CommandResponseArgs {
                client_id,
                sequence_num: 0,
                content: CommandResponseContent::CommandApplied {
                    output: vec![1, 2, 3, 4]
                },
            })
        );
    }
    assert_eq!(apply_receiver.recv().await.unwrap(), vec![1, 2, 3, 4]);
    assert!(apply_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(200)]
async fn client_sessions_are_expired() {
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let (_sender, [leader, _follower], _) = make_standard_rafts::<IdentityMachine, RamStorage, _>(
        &mut system,
        [leader_id, follower_id],
        [100, 300],
        &processes,
    )
    .await;
    let (result_sender, mut result_receiver) = unbounded_channel();

    sleep_ms(150).await;

    let client_id_1 = register_client(&leader, &result_sender, &mut result_receiver).await;
    let client_id_2 = register_client(&leader, &result_sender, &mut result_receiver).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id: client_id_1,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    tokio::time::sleep(SESSION_EXPIRATION + Duration::from_millis(200)).await;
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id: client_id_2,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![9, 10, 11, 12],
                client_id: client_id_1,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id: client_id_1,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![1, 2, 3, 4]
            }
        })
    );
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id: client_id_2,
            sequence_num: 0,
            content: CommandResponseContent::SessionExpired
        })
    );
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id: client_id_1,
            sequence_num: 1,
            content: CommandResponseContent::SessionExpired
        })
    );
    assert!(result_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(200)]
async fn acknowledged_outputs_are_discarded() {
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let (_sender, [leader, _follower], _) = make_standard_rafts::<IdentityMachine, RamStorage, _>(
        &mut system,
        [leader_id, follower_id],
        [100, 300],
        &processes,
    )
    .await;
    let (result_sender, mut result_receiver) = unbounded_channel();

    sleep_ms(150).await;

    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    let response_1 = result_receiver.recv().await.unwrap();
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 1,
            },
        })
        .await;
    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    let response_2 = result_receiver.recv().await.unwrap();
    let response_3 = result_receiver.recv().await.unwrap();

    assert_eq!(
        response_1,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![1, 2, 3, 4]
            }
        })
    );
    assert_eq!(
        response_2,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::CommandApplied {
                output: vec![5, 6, 7, 8]
            }
        })
    );
    assert_eq!(
        response_3,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::SessionExpired
        })
    );
    assert!(result_receiver.is_empty());

    system.shutdown().await;
}
