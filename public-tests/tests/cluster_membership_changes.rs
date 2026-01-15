use std::time::Duration;
use tokio::time::Instant;

use tokio::sync::mpsc::unbounded_channel;

use ntest::timeout;
use uuid::Uuid;

use module_system::System;

use assignment_3_solution::*;
use assignment_3_test_utils::*;

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(100)]
async fn added_server_participates_in_consensus() {
    // given
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let (sender, [leader, _follower], boot) =
        make_standard_rafts::<IdentityMachine, RamStorage, _>(
            &mut system,
            [leader_id, follower_id],
            [100, 300],
            &processes,
        )
        .await;
    let (result_sender, mut result_receiver) = unbounded_channel();
    sleep_ms(150).await;

    // when
    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    let added_id = Uuid::new_v4();
    let [_added] = add_standard_rafts::<IdentityMachine, RamStorage, _>(
        &mut system,
        [added_id],
        [300],
        boot,
        sender.clone(),
        &processes,
    )
    .await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::AddServer {
                new_server: added_id,
            },
        })
        .await;
    let add_result = result_receiver.recv().await.unwrap();
    sender.break_link(follower_id, leader_id).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    let command_result = result_receiver.recv().await.unwrap();

    // then
    assert_eq!(
        add_result,
        ClientRequestResponse::AddServerResponse(AddServerResponseArgs {
            new_server: added_id,
            content: AddServerResponseContent::ServerAdded,
        })
    );
    assert_eq!(
        command_result,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![1, 2, 3, 4]
            }
        })
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(100)]
async fn removed_server_does_not_participate_in_consensus() {
    // given
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id, removed_id] = processes;
    let (sender, [leader, _follower, _removed], _) =
        make_standard_rafts::<IdentityMachine, RamStorage, _>(
            &mut system,
            [leader_id, follower_id, removed_id],
            [100, 300, 300],
            &processes,
        )
        .await;
    let (result_sender, mut result_receiver) = unbounded_channel();
    sleep_ms(150).await;

    // when
    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::RemoveServer {
                old_server: removed_id,
            },
        })
        .await;
    let remove_result = result_receiver.recv().await.unwrap();
    for process_id in processes {
        sender.break_link(removed_id, process_id).await;
    }
    sender.break_link(follower_id, leader_id).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    sleep_ms(700).await;

    // then
    assert_eq!(
        remove_result,
        ClientRequestResponse::RemoveServerResponse(RemoveServerResponseArgs {
            old_server: removed_id,
            content: RemoveServerResponseContent::ServerRemoved,
        })
    );
    assert!(result_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn messages_outside_cluster_configuration_are_accepted() {
    // given
    let mut system = System::new().await;
    let outside_ids = make_idents::<2>();
    let follower_id = Uuid::new_v4();
    let processes = vec![follower_id];
    let (tx, mut rx) = unbounded_channel();
    let boot = Instant::now();
    let follower = Raft::new(
        &mut system,
        make_config(follower_id, boot, Duration::from_millis(100), &processes),
        Box::new(IdentityMachine),
        Box::<RamStorage>::default(),
        Box::new(RamSender { tx }),
    )
    .await;

    // when
    follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                source: outside_ids[0],
                term: 1,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 0,
                last_log_term: 0,
            }),
        })
        .await;
    let client_id = Uuid::from_u128(1);
    follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                source: outside_ids[1],
                term: 2,
            },
            content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        term: 2,
                        timestamp: boot.elapsed(),
                        content: LogEntryContent::RegisterClient,
                    },
                    LogEntry {
                        term: 2,
                        timestamp: boot.elapsed(),
                        content: LogEntryContent::Command {
                            data: vec![1, 2, 3, 4],
                            client_id,
                            sequence_num: 0,
                            lowest_sequence_num_without_response: 0,
                        },
                    },
                ],
                leader_commit: 0,
            }),
        })
        .await;

    // then
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                source: follower_id,
                term: 1,
            },
            content: RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs {
                vote_granted: true
            })
        }
    );
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                source: follower_id,
                term: 2,
            },
            content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success: true,
                last_verified_log_index: 2,
            })
        }
    );

    system.shutdown().await;
}
