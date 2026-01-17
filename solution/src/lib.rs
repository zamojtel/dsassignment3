use std::{collections::HashMap, time::Duration};

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
    // Volatile
    commit_index: usize,
    leader_id: Option<Uuid>,
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
        let raft_node = Self{
            current_term,
            logs,
            voted_for,

            role: RaftRole::Follower,
            leader_id: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),

            last_applied: 0,
            commit_index: 0,
            id: config.self_id,

            stable_storage,
            state_machine,
            message_sender,
        };

        system.register_module(move |_ref| raft_node).await
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, msg: RaftMessage) {
        todo!()
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, msg: ClientRequest) {
        todo!()
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.
