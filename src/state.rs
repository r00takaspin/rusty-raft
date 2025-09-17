use crate::messages::*;
use crate::state::NodeRole::{Candidate, Follower, Leader};
use crate::storage::Storage;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

pub type NodeId = String;
pub type Index = u64;

#[derive(Serialize, Deserialize, Debug)]
struct LogEntry {
    term: u64,
    command: String,
}

impl LogEntry {
    pub fn new(term: u64, command: String) -> Self {
        Self { term, command }
    }

    pub fn initial() -> Self {
        Self {
            term: 0,
            command: "initial".to_string(),
        }
    }
}

impl NodeRole {
    fn become_follower(&mut self) -> Result<(), anyhow::Error> {
        match self {
            Follower => Ok(()),
            Candidate => {
                *self = Follower;
                Ok(())
            }
            Leader => {
                *self = Follower;
                Ok(())
            }
        }
    }

    fn become_candidate(&mut self) -> Result<(), anyhow::Error> {
        match self {
            Follower => {
                *self = Candidate;
                Ok(())
            }
            Candidate => Ok(()),
            Leader => Err(anyhow!("unsupported transition {} -> {}", self, Candidate)),
        }
    }

    fn become_leader(&mut self) -> Result<(), anyhow::Error> {
        match self {
            Follower => Err(anyhow!("unsupported transition {} -> {}", self, Leader)),
            Candidate => {
                *self = Leader;
                Ok(())
            }
            Leader => Ok(()),
        }
    }
}

impl Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Follower => write!(f, "Follower"),
            Candidate => write!(f, "Candidate"),
            Leader => write!(f, "Leader"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeState {
    node_id: NodeId,
    role: NodeRole,
    peers: Vec<String>,
    term: u64,
    log: Vec<LogEntry>,
    voted_for: Option<NodeId>,
}

impl NodeState {
    pub fn default(node_id: NodeId, peers: Vec<String>) -> NodeState {
        // initialize node with initial replication log
        let mut log_entries = vec![];
        log_entries.push(LogEntry::initial());

        NodeState {
            node_id,
            peers,
            role: Follower,
            term: 0,
            voted_for: None,
            log: log_entries,
        }
    }

    pub fn request_vote(
        &mut self,
        node_id: NodeId,
        term: u64,
        last_log_index: Index,
        last_log_term: u64,
    ) -> (bool, u64) {
        // node cannot vote for itself
        if self.node_id == node_id {
            return (false, self.term);
        }

        // node has greater term than candidate, candidate must turn into follower
        if self.term > term {
            return (false, self.term);
        }

        // node already voted in this term
        if self.term == term && self.voted_for.is_some() {
            return (false, self.term);
        }

        // log always have messages
        let last_record = self.log.last().unwrap();

        // last log term greater than candidate
        if last_record.term > last_log_term {
            return (false, last_record.term);
        }

        // node has move actual log records
        if (self.log.len() - 1) as u64 > last_log_index {
            return (false, self.term);
        }

        // somehow new term less than current
        if !self.update_term(term) {
            return (false, self.term);
        }

        self.voted_for = Some(node_id);

        // successfully give vote to candidate
        (true, self.term)
    }

    fn update_term(&mut self, term: u64) -> bool {
        if term > self.term {
            self.term = term;

            return true;
        }

        false
    }

    fn add_log(&mut self, log: LogEntry) {
        self.log.push(log);
    }
}

pub struct Node {
    state: NodeState,
    storage: Arc<Box<dyn Storage>>,
    rx: Receiver<NodeMessage>,
}

impl Node {
    pub fn new(
        state: NodeState,
        rx: Receiver<NodeMessage>,
        storage: Arc<Box<dyn Storage>>,
    ) -> Self {
        Self { state, rx, storage }
    }

    pub async fn run(&mut self) {
        info!("starting as {}", self.state.role);

        while let Some(request) = self.rx.recv().await {
            let resp_rx = request.resp_tx.unwrap();

            let msg = match request.rpc_message {
                RpcMessage::SetRequest(req) => self.handle_set(req),
                RpcMessage::StateRequest(req) => self.handle_state(req),
                RpcMessage::RequestVote(req) => self.handle_request_vote(req),
                _ => RpcMessage::Error(ErrorResponse {
                    err_msg: "unsupported rpc request".to_string(),
                }),
            };

            if resp_rx.send(msg).is_err() {
                error!("response channel was closed");
            }
        }
    }

    pub fn handle_set(&mut self, _: SetRequest) -> RpcMessage {
        RpcMessage::SetResponse(SetResponse { ok: true })
    }

    pub fn handle_state(&mut self, _: StateRequest) -> RpcMessage {
        RpcMessage::StateResponse(StateResponse {
            node_id: self.state.node_id.clone(),
            role: self.state.role.clone(),
            term: self.state.term,
        })
    }

    pub fn handle_request_vote(&mut self, request: RequestVoteRequest) -> RpcMessage {
        let (success, new_term) = self.state.request_vote(
            request.node_id.clone(),
            request.term,
            request.last_log_index,
            request.last_log_term,
        );

        if success {
            if self.storage.save(&self.state).is_err() {
                error!("persistent save failed");

                return RpcMessage::Error(ErrorResponse {
                    err_msg: "persistent error".to_string(),
                });
            }

            info!("votes for {}", request.node_id.clone());
        }

        RpcMessage::RequestVoteResponse(RequestVoteResponse {
            success,
            term: new_term,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_follower_transitions() {
        let mut follower = Follower;
        let result = follower.become_follower();
        assert!(result.is_ok());

        let result = follower.become_candidate();
        assert!(result.is_ok());

        follower = Follower;
        let result = follower.become_leader();
        assert!(!result.is_ok());
        let err = result.err().unwrap();
        assert!(
            err.to_string()
                .contains("unsupported transition Follower -> Leader")
        );
    }

    #[test]
    fn test_candidate_transitions() {
        let mut candidate = Candidate;
        let result = candidate.become_follower();
        assert!(result.is_ok());

        candidate = Candidate;

        let result = candidate.become_candidate();
        assert!(result.is_ok());

        candidate = Candidate;

        let result = candidate.become_leader();
        assert!(result.is_ok());
    }

    #[test]
    fn test_leader_transitions() {
        let mut leader = Leader;

        let result = leader.become_follower();
        assert!(result.is_ok());

        leader = Leader;

        let result = leader.become_candidate();
        let err = result.err().unwrap();
        assert!(
            err.to_string()
                .contains("unsupported transition Leader -> Candidate")
        );

        leader = Leader;

        let result = leader.become_leader();
        assert!(result.is_ok());
    }

    #[test]
    fn test_node_state_default() {
        let state = NodeState::default("1".into(), vec![]);
        assert_eq!("1".to_string(), state.node_id);
        assert_eq!(Follower, state.role);
        assert_eq!(0, state.term);
        assert_eq!(1, state.log.len());
    }

    #[test]
    fn test_log_entry_initial() {
        let log_entity = LogEntry::initial();
        assert_eq!(0, log_entity.term);
        assert_eq!("initial".to_string(), log_entity.command);
    }

    #[test]
    fn test_node_request_vote_same_node() {
        let mut state = NodeState::default("1".into(), vec![]);
        let (got_ok, term) = state.request_vote("1".to_string(), 0, 0, 0);
        assert_eq!(false, got_ok);
        assert_eq!(0, term);
    }

    #[test]
    fn test_node_request_vote_lower_term() {
        let mut state = NodeState::default("1".into(), vec![]);
        state.update_term(1);
        let (got_ok, term) = state.request_vote("2".to_string(), 0, 0, 0);
        assert_eq!(false, got_ok);
        assert_eq!(1, term);
    }

    #[test]
    fn test_node_request_vote_same_term_twice() {
        let mut state = NodeState::default("1".into(), vec![]);
        let (got_ok, term) = state.request_vote("2".to_string(), 1, 0, 0);
        assert_eq!(true, got_ok);
        assert_eq!(1, term);

        let (got_ok, term) = state.request_vote("2".to_string(), 1, 0, 0);
        assert_eq!(false, got_ok);
        assert_eq!(1, term);
    }

    #[test]
    fn test_node_request_log_term_ahead() {
        let mut state = NodeState::default("1".into(), vec![]);
        state.add_log(LogEntry::new(1, "test command".to_string()));

        let (got_ok, term) = state.request_vote("2".to_string(), 1, 0, 0);
        assert_eq!(false, got_ok);
        assert_eq!(1, term);
    }

    #[test]
    fn test_node_request_log_index_ahead() {
        let mut state = NodeState::default("1".into(), vec![]);
        state.add_log(LogEntry::new(1, "test command".to_string()));

        let (got_ok, term) = state.request_vote("2".to_string(), 1, 0, 2);
        assert_eq!(false, got_ok);
        assert_eq!(0, term);
    }
}
