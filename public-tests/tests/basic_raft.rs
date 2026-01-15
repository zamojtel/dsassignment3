use assignment_3_solution::*;
use assignment_3_test_utils::*;
use module_system::System;
use ntest::timeout;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::Instant;
use uuid::Uuid;

#[tokio::test]
#[timeout(1000)]
async fn system_makes_progress_when_there_is_a_majority() {
    // given
    let mut system = System::new().await;

    let entry_data = vec![1, 2, 3, 4, 5];
    let processes = make_idents::<3>();
    let [ident_leader, ident_follower, _dead] = processes;
    let sender = ExecutorSender::default();
    let boot = Instant::now();
    let [raft_leader, _raft_follower] = make_rafts(
        &mut system,
        [ident_leader, ident_follower],
        [100, 300],
        boot,
        |_| Box::new(IdentityMachine),
        |_| Box::<RamStorage>::default(),
        sender.clone(),
        async |id, mref| sender.insert(id, mref).await,
        &processes,
    )
    .await;
    sleep_ms(200).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    // when
    let client_id = register_client(&raft_leader, &result_sender, &mut result_receiver).await;

    raft_leader
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: entry_data.clone(),
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // then
    assert_eq!(
        entry_data,
        *unwrap_output(&result_receiver.recv().await.unwrap())
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(150)]
async fn system_does_not_make_progress_without_majority() {
    // given
    let mut system = System::new().await;

    let entry_data = vec![1, 2, 3, 4, 5];
    let processes = make_idents::<3>();
    let [ident_leader, ident_follower, _dead] = processes;
    let (sender, [raft_leader, _raft_follower], _) =
        make_standard_rafts::<IdentityMachine, RamStorage, _>(
            &mut system,
            [ident_leader, ident_follower],
            [100, 300],
            &processes,
        )
        .await;
    sleep_ms(200).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    // when
    let client_id = register_client(&raft_leader, &result_sender, &mut result_receiver).await;

    sender.break_link(ident_follower, ident_leader).await;

    raft_leader
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: entry_data,
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    sleep_ms(1000).await;

    // then
    assert!(result_receiver.is_empty());

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(150)]
async fn follower_denies_vote_for_candidate_with_outdated_log() {
    // given
    let mut system = System::new().await;

    let (tx, mut rx) = unbounded_channel();
    let processes = make_idents::<3>();
    let [other_ident_1, other_ident_2, ident_follower] = processes;
    let boot = Instant::now();
    let [raft_follower] = make_rafts(
        &mut system,
        [ident_follower],
        [500],
        boot,
        |_| Box::new(DummyMachine),
        |_| Box::<RamStorage>::default(),
        RamSender { tx },
        async |_, _| {},
        &processes,
    )
    .await;
    let client_id = Uuid::from_u128(1);

    // when

    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 2,
                source: other_ident_1,
            },
            content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        content: LogEntryContent::RegisterClient,
                        term: 2,
                        timestamp: boot.elapsed(),
                    },
                    LogEntry {
                        content: LogEntryContent::Command {
                            data: vec![1],
                            client_id,
                            sequence_num: 0,
                            lowest_sequence_num_without_response: 0,
                        },
                        term: 2,
                        timestamp: boot.elapsed(),
                    },
                ],
                leader_commit: 0,
            }),
        })
        .await;

    // Wait longer than election timeout so that the follower does not ignore the vote request
    sleep_ms(600).await;

    // Older term of the last message.
    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 4,
                source: other_ident_2,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 2,
                last_log_term: 1,
            }),
        })
        .await;

    // Shorter log in candidate.
    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 5,
                source: other_ident_2,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 1,
                last_log_term: 2,
            }),
        })
        .await;

    // then
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                source: ident_follower,
                term: 2,
            },
            content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success: true,
                last_verified_log_index: 2,
            })
        }
    );
    for _ in 0..2 {
        rx.recv().await.unwrap();
    }
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                source: ident_follower,
                term: 4,
            },
            content: RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs {
                vote_granted: false,
            })
        }
    );
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                source: ident_follower,
                term: 5,
            },
            content: RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs {
                vote_granted: false,
            })
        }
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn follower_rejects_inconsistent_append_entry() {
    // given
    let mut system = System::new().await;

    let (tx, mut rx) = unbounded_channel();
    let processes = make_idents::<2>();
    let [other_ident, ident_follower] = processes;
    let boot = Instant::now();
    let [raft_follower] = make_rafts(
        &mut system,
        [ident_follower],
        [10_000],
        boot,
        |_| Box::new(DummyMachine),
        |_| Box::<RamStorage>::default(),
        RamSender { tx },
        async |_, _| {},
        &processes,
    )
    .await;

    // when
    let client_id = Uuid::from_u128(1);

    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 1,
                source: other_ident,
            },
            content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        content: LogEntryContent::RegisterClient,
                        term: 1,
                        timestamp: boot.elapsed(),
                    },
                    LogEntry {
                        content: LogEntryContent::Command {
                            data: vec![1, 2, 3, 4],
                            client_id,
                            sequence_num: 0,
                            lowest_sequence_num_without_response: 0,
                        },
                        term: 1,
                        timestamp: boot.elapsed(),
                    },
                ],
                leader_commit: 0,
            }),
        })
        .await;

    raft_follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                term: 2,
                source: other_ident,
            },
            content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                prev_log_index: 2,
                prev_log_term: 2,
                entries: vec![LogEntry {
                    content: LogEntryContent::Command {
                        data: vec![5, 6, 7, 8],
                        client_id,
                        sequence_num: 0,
                        lowest_sequence_num_without_response: 0,
                    },
                    term: 2,
                    timestamp: boot.elapsed(),
                }],
                leader_commit: 0,
            }),
        })
        .await;

    // then
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                term: 1,
                source: ident_follower,
            },
            content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success: true,
                last_verified_log_index: 2,
            })
        }
    );
    assert_eq!(
        rx.recv().await.unwrap(),
        RaftMessage {
            header: RaftMessageHeader {
                term: 2,
                source: ident_follower,
            },
            content: RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success: false,
                last_verified_log_index: 3,
            })
        }
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn follower_redirects_to_leader() {
    // given
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let (_sender, [leader, follower], _) = make_standard_rafts::<IdentityMachine, RamStorage, _>(
        &mut system,
        [leader_id, follower_id],
        [100, 300],
        &processes,
    )
    .await;
    sleep_ms(200).await;

    // when
    let (follower_result_sender, mut follower_result_receiver) = unbounded_channel();
    let (leader_result_sender, mut leader_result_receiver) = unbounded_channel();

    let client_id =
        register_client(&leader, &leader_result_sender, &mut leader_result_receiver).await;

    follower
        .send(ClientRequest {
            reply_to: follower_result_sender,
            content: ClientRequestContent::Command {
                command: vec![1, 2, 3, 4],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    leader
        .send(ClientRequest {
            reply_to: leader_result_sender,
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    // then
    assert_eq!(
        follower_result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::NotLeader {
                leader_hint: Some(leader_id)
            }
        })
    );
    assert_eq!(
        leader_result_receiver.recv().await.unwrap(),
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![5, 6, 7, 8]
            },
        })
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn leader_steps_down_without_heartbeat_responses_from_majority() {
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id] = processes;
    let (sender, [leader, _follower], _) = make_standard_rafts::<IdentityMachine, RamStorage, _>(
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

    sender.break_link(follower_id, leader_id).await;
    sleep_ms(200).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: vec![5, 6, 7, 8],
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    let response_2 = result_receiver.recv().await.unwrap();

    assert_eq!(
        response_1,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 0,
            content: CommandResponseContent::CommandApplied {
                output: vec![1, 2, 3, 4]
            },
        })
    );
    assert_eq!(
        response_2,
        ClientRequestResponse::CommandResponse(CommandResponseArgs {
            client_id,
            sequence_num: 1,
            content: CommandResponseContent::NotLeader { leader_hint: None }
        })
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(100)]
async fn follower_ignores_request_vote_within_election_timeout_of_leader_heartbeat() {
    // given
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id, spy_id] = processes;
    let (sender, [leader, follower], _) = make_standard_rafts::<IdentityMachine, RamStorage, _>(
        &mut system,
        [leader_id, follower_id],
        [100, 300],
        &processes,
    )
    .await;
    sleep_ms(150).await;

    // when
    let (spy_sender, mut spy_receiver) = unbounded_channel();
    sender
        .insert(
            spy_id,
            Box::new(RaftSpy::new(&mut system, None, spy_sender).await),
        )
        .await;

    follower
        .send(RaftMessage {
            header: RaftMessageHeader {
                source: spy_id,
                term: 2,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 0,
                last_log_term: 0,
            }),
        })
        .await;
    leader
        .send(RaftMessage {
            header: RaftMessageHeader {
                source: spy_id,
                term: 2,
            },
            content: RaftMessageContent::RequestVote(RequestVoteArgs {
                last_log_index: 0,
                last_log_term: 0,
            }),
        })
        .await;
    sleep_ms(500).await;

    // then
    while let Ok(msg) = spy_receiver.try_recv() {
        assert!(matches!(
            msg,
            RaftMessage {
                header: RaftMessageHeader {
                    source,
                    term: 1,
                },
                content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: _,
                    leader_commit: 1,
                })
            } if source == leader_id
        ));
        if let RaftMessageContent::AppendEntries(AppendEntriesArgs { entries, .. }) = msg.content {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].term, 1);
            assert_eq!(entries[0].content, LogEntryContent::NoOp);
        }
    }

    system.shutdown().await;
}
