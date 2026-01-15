use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use assignment_3_solution::{StateMachine, decode_from_slice, encode_to_vec};

#[derive(Default, Debug)]
pub struct DistributedSet {
    /// The current (committed) state of the set.
    integers: HashSet<i64>,
}

impl DistributedSet {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SetOperation {
    Add(i64),
    Remove(i64),
    IsPresent(i64),
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
pub enum SetResponse {
    InvalidOperation,
    IsPresent(bool),
}

#[async_trait::async_trait]
impl StateMachine for DistributedSet {
    async fn initialize(&mut self, state: &[u8]) {
        self.integers = decode_from_slice(state).unwrap();
    }

    async fn apply(&mut self, command: &[u8]) -> Vec<u8> {
        match decode_from_slice::<SetOperation>(command) {
            Ok(command) => match command {
                SetOperation::Add(num) => {
                    self.integers.insert(num);
                    vec![]
                }
                SetOperation::Remove(num) => {
                    self.integers.remove(&num);
                    vec![]
                }
                SetOperation::IsPresent(num) => {
                    encode_to_vec(&SetResponse::IsPresent(self.integers.contains(&num))).unwrap()
                }
            },
            Err(_) => encode_to_vec(&SetResponse::InvalidOperation).unwrap(),
        }
    }

    async fn serialize(&self) -> Vec<u8> {
        encode_to_vec(&self.integers).unwrap()
    }
}
