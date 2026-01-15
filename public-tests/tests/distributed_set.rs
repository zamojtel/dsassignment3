use assignment_3_solution::{
    ClientRequest, ClientRequestContent, ClientRequestResponse, CommandResponseArgs,
    CommandResponseContent, Raft, RegisterClientResponseArgs, RegisterClientResponseContent,
    ServerConfig, decode_from_slice, encode_to_vec,
};
use assignment_3_test_utils::distributed_set::{DistributedSet, SetOperation, SetResponse};
use assignment_3_test_utils::{ExecutorSender, RamStorage, singleton_range, sleep_ms};
use module_system::System;
use ntest::timeout;
use std::time::Duration;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::Instant;
use uuid::Uuid;

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(100)]
async fn can_remove_from_set() {
    // given
    let mut system = System::new().await;

    let ident_leader = Uuid::new_v4();
    let servers = vec![ident_leader];
    let sender = ExecutorSender::default();

    let boot = Instant::now();
    let raft_leader = Raft::new(
        &mut system,
        ServerConfig {
            self_id: ident_leader,
            election_timeout_range: singleton_range(Duration::from_millis(50)),
            heartbeat_timeout: Duration::from_millis(10),
            servers: servers.into_iter().collect(),
            append_entries_batch_size: 10,
            snapshot_chunk_size: 10,
            catch_up_rounds: 10,
            session_expiration: Duration::from_secs(20),
            system_boot_time: boot,
        },
        Box::new(DistributedSet::new()),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    sender
        .insert(ident_leader, Box::new(raft_leader.clone()))
        .await;
    // Raft leader election
    sleep_ms(200).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    // when
    raft_leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::RegisterClient,
        })
        .await;
    let ClientRequestResponse::RegisterClientResponse(RegisterClientResponseArgs {
        content: RegisterClientResponseContent::ClientRegistered { client_id },
    }) = result_receiver.recv().await.unwrap()
    else {
        panic!("Client registration failed");
    };

    raft_leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: encode_to_vec(&SetOperation::Add(7_i64)).unwrap(),
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    raft_leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: encode_to_vec(&SetOperation::Remove(7_i64)).unwrap(),
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;

    raft_leader
        .send(ClientRequest {
            reply_to: result_sender,
            content: ClientRequestContent::Command {
                command: encode_to_vec(&SetOperation::IsPresent(7_i64)).unwrap(),
                client_id,
                sequence_num: 2,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    result_receiver.recv().await.unwrap();
    result_receiver.recv().await.unwrap();
    let is_7_present_response = result_receiver.recv().await.unwrap();
    let ClientRequestResponse::CommandResponse(CommandResponseArgs {
        content:
            CommandResponseContent::CommandApplied {
                output: is_7_present_output,
            },
        ..
    }) = is_7_present_response
    else {
        panic!("Received wrong response: {is_7_present_response:?}");
    };

    // then
    assert_eq!(
        decode_from_slice::<SetResponse>(&is_7_present_output).unwrap(),
        SetResponse::IsPresent(false)
    );

    system.shutdown().await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[timeout(100)]
async fn single_node_adds_to_set() {
    // given
    let mut system = System::new().await;

    let ident_leader = Uuid::new_v4();
    let servers = vec![ident_leader];
    let sender = ExecutorSender::default();

    let boot = Instant::now();
    let raft_leader = Raft::new(
        &mut system,
        ServerConfig {
            self_id: ident_leader,
            election_timeout_range: singleton_range(Duration::from_millis(50)),
            heartbeat_timeout: Duration::from_millis(10),
            servers: servers.into_iter().collect(),
            append_entries_batch_size: 10,
            snapshot_chunk_size: 10,
            catch_up_rounds: 10,
            session_expiration: Duration::from_secs(20),
            system_boot_time: boot,
        },
        Box::new(DistributedSet::new()),
        Box::<RamStorage>::default(),
        Box::new(sender.clone()),
    )
    .await;
    sender
        .insert(ident_leader, Box::new(raft_leader.clone()))
        .await;
    // Raft leader election
    sleep_ms(200).await;

    let (result_sender, mut result_receiver) = unbounded_channel();

    // when
    raft_leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::RegisterClient,
        })
        .await;
    let ClientRequestResponse::RegisterClientResponse(RegisterClientResponseArgs {
        content: RegisterClientResponseContent::ClientRegistered { client_id },
    }) = result_receiver.recv().await.unwrap()
    else {
        panic!("Client registration failed");
    };
    raft_leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: encode_to_vec(&SetOperation::Add(7_i64)).unwrap(),
                client_id,
                sequence_num: 0,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    raft_leader
        .send(ClientRequest {
            reply_to: result_sender.clone(),
            content: ClientRequestContent::Command {
                command: encode_to_vec(&SetOperation::IsPresent(7_i64)).unwrap(),
                client_id,
                sequence_num: 1,
                lowest_sequence_num_without_response: 0,
            },
        })
        .await;
    result_receiver.recv().await.unwrap();
    let is_7_present_response = result_receiver.recv().await.unwrap();
    let ClientRequestResponse::CommandResponse(CommandResponseArgs {
        content:
            CommandResponseContent::CommandApplied {
                output: is_7_present_output,
            },
        ..
    }) = is_7_present_response
    else {
        panic!("Received wrong response: {is_7_present_response:?}");
    };

    // then
    assert_eq!(
        decode_from_slice::<SetResponse>(&is_7_present_output).unwrap(),
        SetResponse::IsPresent(true)
    );

    system.shutdown().await;
}
