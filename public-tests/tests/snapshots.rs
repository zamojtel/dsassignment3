use assignment_3_solution::*;
use assignment_3_test_utils::*;
use module_system::System;
use ntest::timeout;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::Instant;
use uuid::Uuid;

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn snapshot_is_sent_to_follower_correctly() {
    let mut system = System::new().await;
    let processes = make_idents();
    let [leader_id, follower_id, spy_id] = processes;
    let (sender, [leader, _follower], boot) = make_standard_rafts::<LogMachine, RamStorage, _>(
        &mut system,
        [leader_id, follower_id],
        [100, 300],
        &processes,
    )
    .await;
    let (result_sender, mut result_receiver) = unbounded_channel();

    sleep_ms(150).await;

    let client_id = register_client(&leader, &result_sender, &mut result_receiver).await;

    for i in 0..4 {
        leader
            .send(ClientRequest {
                reply_to: result_sender.clone(),
                content: ClientRequestContent::Command {
                    command: vec![i],
                    client_id,
                    sequence_num: i.into(),
                    lowest_sequence_num_without_response: 0,
                },
            })
            .await;
    }
    for _ in 0..4 {
        result_receiver.recv().await.unwrap();
    }
    sender.break_link(follower_id, leader_id).await;
    for i in 4..8 {
        leader
            .send(ClientRequest {
                reply_to: result_sender.clone(),
                content: ClientRequestContent::Command {
                    command: vec![i],
                    client_id,
                    sequence_num: i.into(),
                    lowest_sequence_num_without_response: 0,
                },
            })
            .await;
    }
    sleep_ms(50).await;

    leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Snapshot,
        })
        .await;
    assert_eq!(
        result_receiver.recv().await.unwrap(),
        ClientRequestResponse::SnapshotResponse(SnapshotResponseArgs {
            content: SnapshotResponseContent::SnapshotCreated {
                last_included_index: 6
            }
        })
    );

    let (spy_tx, mut spy_rx) = unbounded_channel();
    let spy = {
        let inner_raft = Raft::new(
            &mut system,
            make_config(spy_id, boot, Duration::from_millis(300), &processes),
            Box::<LogMachine>::default(),
            Box::<RamStorage>::default(),
            Box::new(sender.clone()),
        )
        .await;
        RaftSpy::new(&mut system, Some(inner_raft), spy_tx).await
    };
    sender.insert(spy_id, Box::new(spy.clone())).await;

    let mut expected_state_machine = LogMachine::default();
    for i in 0..4 {
        expected_state_machine.apply(&[i]).await;
    }
    let expected_snapshot = expected_state_machine.serialize().await;

    for (chunk_idx, chunk) in expected_snapshot.chunks(CHUNK_SIZE).enumerate() {
        loop {
            let msg = spy_rx.recv().await.unwrap();
            if let RaftMessageContent::InstallSnapshot(args @ InstallSnapshotArgs { offset, .. }) =
                msg.content
                && offset == chunk_idx * CHUNK_SIZE
            {
                assert_eq!(
                    msg.header,
                    RaftMessageHeader {
                        source: leader_id,
                        term: 1
                    }
                );
                if offset == 0 {
                    assert_eq!(
                        args.last_config.as_ref().unwrap(),
                        &processes.iter().copied().collect::<HashSet<Uuid>>()
                    );
                    assert_eq!(args.client_sessions.as_ref().unwrap().len(), 1);
                    let session = &args.client_sessions.as_ref().unwrap()[&client_id];
                    assert_eq!(
                        session.responses,
                        (0..4_u8).map(|i| (i.into(), vec![i])).collect()
                    );
                    assert_eq!(session.lowest_sequence_num_without_response, 0);
                }
                assert_eq!(args.last_included_index, 6);
                assert_eq!(args.last_included_term, 1);
                assert_eq!(args.data, chunk);
                assert_eq!(
                    args.done,
                    expected_snapshot.len() - chunk_idx * CHUNK_SIZE <= CHUNK_SIZE
                );
                break;
            }
        }
    }
    loop {
        let msg = spy_rx.recv().await.unwrap();
        if let RaftMessageContent::AppendEntries(args) = msg.content
            && args.prev_log_index == 6
        {
            assert_eq!(
                msg.header,
                RaftMessageHeader {
                    source: leader_id,
                    term: 1,
                }
            );
            assert_eq!(args.prev_log_term, 1);
            assert_eq!(args.entries.len(), 4);
            for (i, entry) in args.entries.iter().enumerate() {
                assert_eq!(entry.term, 1);
                assert_eq!(
                    entry.content,
                    LogEntryContent::Command {
                        data: vec![(i + 4).try_into().unwrap()],
                        client_id,
                        sequence_num: (i + 4).try_into().unwrap(),
                        lowest_sequence_num_without_response: 0,
                    }
                );
            }
            assert_eq!(args.leader_commit, 6);
            break;
        }
    }

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(50)]
async fn state_machine_is_initialized_with_snapshot() {
    // given
    let leader_id = Uuid::new_v4();
    let processes = [leader_id];
    let boot = Instant::now();
    let stable_storage_content = Arc::new(std::sync::Mutex::new(HashMap::new()));
    {
        let mut system = System::new().await;
        let sender = ExecutorSender::default();
        let (init_sender, _init_receiver) = unbounded_channel();
        let leader = Raft::new(
            &mut system,
            make_config(leader_id, boot, Duration::from_millis(100), &processes),
            Box::new(InitDetectorMachine {
                init_sender: init_sender.clone(),
            }),
            Box::new(SharedRamStorage {
                content: stable_storage_content.clone(),
            }),
            Box::new(sender.clone()),
        )
        .await;
        let (result_sender, mut result_receiver) = unbounded_channel();
        leader
            .send(ClientRequest {
                reply_to: result_sender,
                content: ClientRequestContent::Snapshot,
            })
            .await;
        result_receiver.recv().await.unwrap();
        system.shutdown().await;
    }

    // when
    let mut system = System::new().await;
    let sender = ExecutorSender::default();
    let (init_sender, mut init_receiver) = unbounded_channel();
    let _leader = Raft::new(
        &mut system,
        make_config(leader_id, boot, Duration::from_millis(100), &processes),
        Box::new(InitDetectorMachine { init_sender }),
        Box::new(SharedRamStorage {
            content: stable_storage_content.clone(),
        }),
        Box::new(sender.clone()),
    )
    .await;

    // then
    assert_eq!(
        init_receiver.recv().await.unwrap(),
        InitDetectorMachine::get_state()
    );

    system.shutdown().await;
}
