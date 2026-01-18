use std::{collections::{HashMap, HashSet}};
use tokio::time::{Duration, Instant};
use uuid::Uuid;
use tokio::sync::mpsc::UnboundedSender;
use std::ops::RangeInclusive;
use rand::Rng;

use module_system::{Handler, ModuleRef, System};
use serde::{Serialize, Deserialize};

pub use domain::*;

mod domain;

#[derive(Serialize, Deserialize)]
pub struct PersistentState{
    current_term: u64,
    voted_for: Option<Uuid>,
    logs: Vec<LogEntry>,
}

// roles
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole{
    Follower,
    Candidate,
    Leader,
}

#[non_exhaustive]
pub struct Raft {
    // TODO you can add fields to this struct.
    // passed fields
    id: Uuid,
    role: RaftRole,
    state_machine: Box<dyn StateMachine>,
    stable_storage: Box<dyn StableStorage>,
    message_sender: Box<dyn RaftSender>,
    // Persistent
    current_term: u64,
    logs: Vec<LogEntry>,
    voted_for: Option<Uuid>,
    next_index: HashMap<Uuid,usize>,
    // max replicated index for a process
    match_index: HashMap<Uuid,usize>,
    last_applied: usize,
    append_entries_batch_size: usize,
    peers: HashSet<Uuid>,
    votes_received: std::collections::HashSet<Uuid>,
    // Volatile
    commit_index: usize,
    leader_id: Option<Uuid>,
    response_channels: HashMap<usize,UnboundedSender<ClientRequestResponse>>,

    system_boot_time: Instant,
    election_timeout_range: RangeInclusive<Duration>,
    heartbeat_timeout: Duration,
    election_deadline: Instant,
    heartbeat_deadline: Instant,
    last_leader_contact: Instant,
    last_heartbeat_response: HashMap<Uuid, Instant>,
}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        config: ServerConfig,
        state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {
        let initial_entry = LogEntry{
            term: 0,
            timestamp: Duration::from_secs(0),
            content: LogEntryContent::Configuration { 
                servers: config.servers.clone(),  
            }
        };
        let (current_term,voted_for,logs) = match stable_storage.get("persistent_state").await {
            Some(bytes) => {
                let state: PersistentState = decode_from_slice(&bytes).expect("could not read from stable storage!");
                (state.current_term,state.voted_for,state.logs)
            },
            None => {
                (0,None,vec![initial_entry])
            },
        };

        let mut raft_node = Self {
            current_term,
            logs,
            voted_for,

            role: RaftRole::Follower,
            leader_id: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            response_channels: HashMap::new(),
            
            votes_received: HashSet::new(), 
            last_applied: 0,
            commit_index: 0,
            id: config.self_id,
            peers: config.servers,
            
            append_entries_batch_size: config.append_entries_batch_size,
            stable_storage,
            state_machine,
            message_sender,
            
            election_timeout_range: config.election_timeout_range.clone(),
            heartbeat_timeout: config.heartbeat_timeout,
            system_boot_time: config.system_boot_time,
            election_deadline: Instant::now(),
            heartbeat_deadline: Instant::now(),
            last_leader_contact: Instant::now(),
            last_heartbeat_response: HashMap::new(),
        };

        raft_node.reset_election_timer();

       let self_ref = system.register_module(move |_ref| raft_node).await;

        let ticker_ref = self_ref.clone();
        tokio::spawn(
            async move {
                let interval = Duration::from_millis(10);
                loop {
                    tokio::time::sleep(interval).await;
                    let _ = ticker_ref.send(Timeout).await;
                }
            }
        );

        self_ref
    }

    async fn save_state(&mut self) {
        let state = PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            logs: self.logs.clone(),
        };

        if let Ok(bytes) = encode_to_vec(&state) {
            let _ = self.stable_storage.put("persistent_state", &bytes).await;
        }
    }

    async fn broadcast_append_entries(&mut self) {
        for (peer_id,&peer_next_index) in &self.next_index {
            if *peer_id == self.id {
                continue;
            }

            let prev_log_index = peer_next_index - 1;
            let prev_log_term = self.logs.get(prev_log_index)
            .map(|entry| entry.term )
            .unwrap_or(0);

            let entries_to_send: Vec<LogEntry> = self.logs.iter()
            .skip(peer_next_index)
            .take(self.append_entries_batch_size)
            .cloned()
            .collect();

            let response = RaftMessage {
                header: RaftMessageHeader { 
                    source: self.id,
                    term: self.current_term,
                },
                content: RaftMessageContent::AppendEntries(
                    AppendEntriesArgs {
                    prev_log_index,
                    prev_log_term,
                    entries: entries_to_send,
                    leader_commit: self.commit_index,
                })
            };

            self.message_sender.send(peer_id, response).await;
        }
    }

    async fn start_election(&mut self) {
        self.role = RaftRole::Candidate;

        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.votes_received.clear();
        self.votes_received.insert(self.id);

        let state = PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            logs: self.logs.clone(),
        };

        if let Ok(bytes) = encode_to_vec(&state) {
            let _ = self.stable_storage.put("persistent_state", &bytes).await;
        }

        // for safety 
        let last_log_index = self.logs.len().saturating_sub(1);
        let last_log_term = self.logs.last().map(|e| e.term).unwrap_or(0);

        for peer_id in &self.peers{
            if *peer_id == self.id {
                continue;
            }

            let msg = RaftMessage {
                header: RaftMessageHeader {
                    source: self.id,
                    term: self.current_term,
                },
                content: RaftMessageContent::RequestVote(RequestVoteArgs {
                    last_log_index,
                    last_log_term,
                }),
            };

            self.message_sender.send(peer_id, msg).await;
        }
    }

    async fn become_leader(&mut self) {
        let now = tokio::time::Instant::now();
        let entry_timestamp = now.duration_since(self.system_boot_time);
        
        let no_op_entry = LogEntry {
            term: self.current_term,
            timestamp: entry_timestamp,
            content: LogEntryContent::NoOp,
        };
        
        self.logs.push(no_op_entry);

        let next_idx = self.logs.len();

        self.next_index.clear();
        self.match_index.clear();

        for peer_id in &self.peers {
            self.next_index.insert(*peer_id, next_idx);
            self.match_index.insert(*peer_id, 0);
        }
    }

    fn reset_election_timer(&mut self) {
        let mut rng = rand::rng();
        
        let timeout = rng.random_range(self.election_timeout_range.clone());
        self.election_deadline = Instant::now() + timeout;
    }

    async fn apply_committed_entries(&mut self) {
        while self.commit_index > self.last_applied {
            self.last_applied += 1;
            let log_index = self.last_applied;
            
            let entry = self.logs[log_index].clone();

            match entry.content {
                LogEntryContent::Command { data, client_id, sequence_num, .. } => {

                    let output = self.state_machine.apply(&data).await;

                    if let Some(sender) = self.response_channels.remove(&log_index) {
                        let response = ClientRequestResponse::CommandResponse(
                            CommandResponseArgs {
                                client_id,
                                sequence_num,
                                content: CommandResponseContent::CommandApplied { output },
                            }
                        );
                        //error is ignored
                        let _ = sender.send(response);
                    }
                },
                LogEntryContent::RegisterClient => {
                    if let Some(sender) = self.response_channels.remove(&log_index) {
                        let response = ClientRequestResponse::RegisterClientResponse(
                            RegisterClientResponseArgs {
                                content: RegisterClientResponseContent::ClientRegistered {
                                    client_id: Uuid::from_u128(log_index as u128),
                                },
                            }
                        );
                        let _ = sender.send(response);
                    }
                },
                _ => {},
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
async fn handle(&mut self, msg: RaftMessage) {
        if let RaftMessageContent::RequestVote(_) = &msg.content {
            let min_election_timeout = *self.election_timeout_range.start();
            let is_leader_alive = self.last_leader_contact.elapsed() < min_election_timeout;
            
            if self.role == RaftRole::Leader || (self.leader_id.is_some() && is_leader_alive) {
                return; 
            }
        }

        if msg.header.term > self.current_term {
            self.current_term = msg.header.term;
            self.voted_for = None;
            self.role = RaftRole::Follower;
            self.leader_id = None; 
            self.save_state().await;
        };

        match msg.content {
            RaftMessageContent::RequestVoteResponse(args) => {
                if self.role == RaftRole::Candidate {
                    if args.vote_granted {
                        self.votes_received.insert(msg.header.source);

                        let majority = (self.peers.len() / 2) + 1;

                        if self.votes_received.len() >= majority {
                            self.role = RaftRole::Leader;
                            self.leader_id = Some(self.id);
     
                            self.become_leader().await; 
                            self.save_state().await;
                            self.broadcast_append_entries().await;
                        }
                    }
                }
            },
            RaftMessageContent::RequestVote(args) => {
                let candidate_id = msg.header.source;

                let my_last_log_idx = self.logs.len().saturating_sub(1);
                let my_last_log_term = self.logs.last().map(|e| e.term).unwrap_or(0);

                let log_is_ok = (args.last_log_term > my_last_log_term) ||
                                (args.last_log_term == my_last_log_term && args.last_log_index >= my_last_log_idx);

                let term_is_ok = msg.header.term == self.current_term;
                let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate_id);

                let vote_granted = term_is_ok && can_vote && log_is_ok;

                if vote_granted {
                    self.voted_for = Some(candidate_id);
                    self.reset_election_timer();
                    self.save_state().await;
                }

                let response = RaftMessage {
                    header: RaftMessageHeader {
                        term: self.current_term,
                        source: self.id,
                    },
                    content: RaftMessageContent::RequestVoteResponse(
                        RequestVoteResponseArgs {
                            vote_granted,
                        }
                    ),
                };
                self.message_sender.send(&candidate_id, response).await;
            },
            RaftMessageContent::AppendEntriesResponse(args) => {
                if self.role == RaftRole::Leader {
                    let follower_id = msg.header.source;
                    self.last_heartbeat_response.insert(follower_id, tokio::time::Instant::now());
                    
                    if args.success {
                        let current_match = *self.match_index.get(&follower_id).unwrap_or(&0);
                        let new_match = std::cmp::max(current_match, args.last_verified_log_index);
                        self.match_index.insert(follower_id, new_match);

                        let new_next = new_match + 1;
                        self.next_index.insert(follower_id, new_next);

                        let mut match_indexes: Vec<usize> = self.match_index.values().cloned().collect();
                        match_indexes.push(self.logs.len() - 1); 
                        match_indexes.sort_unstable();

                        let majority_index = match_indexes.len() / 2; 
                        let potential_commit_index = match_indexes[majority_index];

                        if potential_commit_index > self.commit_index {
                            let entry_term = self.logs.get(potential_commit_index).map(|e| e.term).unwrap_or(0);
                            if entry_term == self.current_term {
                                self.commit_index = potential_commit_index;
                                self.apply_committed_entries().await;
                            }
                        }
                    } else {
                        let current_next = *self.next_index.get(&follower_id).unwrap_or(&(self.logs.len()));
                        let new_next = std::cmp::max(1, current_next.saturating_sub(1));
                        self.next_index.insert(follower_id, new_next);
                        
                        self.broadcast_append_entries().await;
                    }
                }
            },
            RaftMessageContent::AppendEntries(args) => {
                if msg.header.term >= self.current_term {
                    self.reset_election_timer();
                    self.leader_id = Some(msg.header.source);
                    if self.role == RaftRole::Candidate {
                        self.role = RaftRole::Follower;
                    }
                    self.last_leader_contact = tokio::time::Instant::now();
                }

                let log_ok = self.logs.get(args.prev_log_index)
                    .map(|entry| entry.term == args.prev_log_term)
                    .unwrap_or(false);

                if !log_ok {
                    let response = RaftMessage {
                        header: RaftMessageHeader {
                            term: self.current_term,
                            source: self.id,
                        },
                        content: RaftMessageContent::AppendEntriesResponse(
                            AppendEntriesResponseArgs {
                                success: false,
                                last_verified_log_index: self.logs.len(), // hint
                            }
                        ),
                    };
                    self.message_sender.send(&msg.header.source, response).await;
                } else {
                    let entries_len = args.entries.len();
                    for (i, entry) in args.entries.into_iter().enumerate() {
                        let target_idx = args.prev_log_index + 1 + i;
                        if target_idx < self.logs.len() {
                            if self.logs[target_idx].term != entry.term {
                                self.logs.truncate(target_idx);
                                self.logs.push(entry);
                            }
                        } else {
                            self.logs.push(entry);
                        }
                    }

                    if entries_len > 0 {
                        self.save_state().await;
                    }
                    
                    let index_of_last_new_entry = args.prev_log_index + entries_len;
                    if args.leader_commit > self.commit_index {
                        self.commit_index = std::cmp::min(args.leader_commit as usize, index_of_last_new_entry as usize);
                        self.apply_committed_entries().await;
                    }

                    let response = RaftMessage {
                        header: RaftMessageHeader {
                            term: self.current_term,
                            source: self.id,
                        },
                        content: RaftMessageContent::AppendEntriesResponse(
                            AppendEntriesResponseArgs {
                                success: true,
                                last_verified_log_index: index_of_last_new_entry as usize,
                            }
                        ),
                    };
                    self.message_sender.send(&msg.header.source, response).await;
                }
            },
            _ => { },
        }
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, msg: ClientRequest) {
        let now = Instant::now();
        let entry_timestamp = now.duration_since(self.system_boot_time);

        match msg.content {
            ClientRequestContent::Command{command,client_id,sequence_num,lowest_sequence_num_without_response} => {
                if self.role == RaftRole::Leader {
                    let log_entry = LogEntry{
                        term: self.current_term,
                        timestamp: entry_timestamp,
                        content:
                        LogEntryContent::Command {
                            data: command,
                            client_id,
                            sequence_num,
                            lowest_sequence_num_without_response
                        },
                    };

                    self.logs.push(log_entry);
                    self.save_state().await;
                    self.response_channels.insert(self.logs.len()-1, msg.reply_to);   
                    self.broadcast_append_entries().await;
                }else{
                    // what happens when we are not a leader
                    let response = ClientRequestResponse::CommandResponse(
                        CommandResponseArgs { 
                            client_id,
                            sequence_num,
                            content: CommandResponseContent::NotLeader { leader_hint: self.leader_id },
                        }
                    );
                    let _ = msg.reply_to.send(response).expect("failed to send response");
                }
            },
            ClientRequestContent::RegisterClient => {
                if self.role == RaftRole::Leader {

                    let log_entry = LogEntry {
                        term:self.current_term,
                        timestamp: entry_timestamp,
                        content: LogEntryContent::RegisterClient
                    };

                    self.logs.push(log_entry);

                    self.save_state().await;

                    self.response_channels.insert(self.logs.len()-1, msg.reply_to);
                    self.broadcast_append_entries().await;
                }else{
                    let response = ClientRequestResponse::RegisterClientResponse(
                        RegisterClientResponseArgs {
                            content: RegisterClientResponseContent::NotLeader { leader_hint: self.leader_id },
                        }
                    );
                    let _ = msg.reply_to.send(response);
                }
            }
            _ => {},
        }
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.

// internal message for ticking timeouts
struct Timeout;

#[async_trait::async_trait]
impl Handler<Timeout> for Raft {
    async fn handle(&mut self, _: Timeout) {
        let now = Instant::now();

        match self.role {
            RaftRole::Leader => {
                // 1. Wysyłanie Heartbeatów (to już powinieneś mieć)
                if now >= self.heartbeat_deadline {
                    self.broadcast_append_entries().await;
                    self.heartbeat_deadline = now + self.heartbeat_timeout;
                }

                // 2. NOWOŚĆ: Sprawdź, czy załoga wciąż z nami jest!
                // Specyfikacja: "If election timeout elapses without successful round of heartbeats..."
                
                // Używamy np. dolnej granicy timeoutu wyborczego jako limitu cierpliwości
                let election_timeout = *self.election_timeout_range.start();
                
                // Liczymy głosy (siebie liczymy zawsze jako 1)
                let mut active_nodes = 1; 
                
                for peer_id in &self.peers {
                    if let Some(last_ack) = self.last_heartbeat_response.get(peer_id) {
                        if now.duration_since(*last_ack) < election_timeout {
                            active_nodes += 1;
                        }
                    }
                }

                let majority = (self.peers.len() / 2) + 1;
                
                if active_nodes < majority {
                    self.role = RaftRole::Follower;
                    self.leader_id = None;
                    self.save_state().await;
                }
            }
            RaftRole::Follower | RaftRole::Candidate => {
                if now >= self.election_deadline {
                    self.start_election().await;
                    self.reset_election_timer();
                }
            }
        }
    }
}