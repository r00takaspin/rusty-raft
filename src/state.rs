use crate::messages;
use crate::messages::Response;
use crate::state::NodeRole::{Candidate, Follower, Leader};
use crate::storage::Storage;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::sync::{Mutex, oneshot};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

type NodeId = String;
type Index = u64;

#[derive(Serialize, Deserialize, Debug)]
struct LogEntry {
    term: u64,
    command: String,
}

impl LogEntry {
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
        if self.term == term && !self.voted_for.is_none() {
            return (false, self.term);
        }

        // log always have messages
        let last_record = self.log.last().unwrap();

        // last log term greater than candidate
        if last_record.term > last_log_term {
            return (false, self.term);
        }

        // node has move actual log records
        if last_log_index > (self.log.len() - 1) as u64 {
            return (false, self.term);
        }

        self.term = term;
        self.voted_for = Some(node_id);

        // successfully give vote to candidate
        (true, self.term)
    }
}

pub struct Node {
    state: NodeState,
    storage: Arc<Box<dyn Storage + Send + Sync>>,
    rx: Receiver<messages::Request>,
}

impl Node {
    pub fn new(
        state: NodeState,
        rx: Receiver<messages::Request>,
        storage: Arc<Box<dyn Storage + Send + Sync>>,
    ) -> Self {
        Self { state, rx, storage }
    }

    pub async fn run(&mut self) {
        info!("starting node {}", self.state.node_id);

        while let Some(request) = self.rx.recv().await {
            let params: Vec<&str> = request.command.split(' ').collect();

            if params.len() < 1 {
                error!("command not provided");

                continue;
            }

            match params[0] {
                "set" => self.handle_set(request.command, request.resp_tx),
                "state" => self.handle_state(request.resp_tx).await,
                "request_vote" => self.request_vote(request.command, request.resp_tx).await,
                _ => info!("unsupported command {}", request.command),
            }
        }
    }

    pub fn handle_set(&mut self, _: String, response: Sender<Response>) {
        if response
            .send(Response {
                msg: "Ok".to_string(),
            })
            .is_err()
        {
            error!("response channel was closed");
        }
    }

    pub async fn handle_state(&mut self, response: Sender<Response>) {
        let response_str = format!(
            "node_id: {}, role: {}, term: {}, peers: {:?}",
            self.state.node_id, self.state.role, self.state.term, self.state.peers,
        );

        if response.send(Response { msg: response_str }).is_err() {
            error!("response channel was closed");
        }
    }

    pub async fn request_vote(&mut self, command: String, response: Sender<Response>) {
        // 0 - "request_vote"
        // 1 - node_id
        // 2 - term
        // 3 - last_log_index
        // 4 - last_log_term

        let parts: Vec<&str> = command.split(' ').collect();

        if parts.len() != 5 {
            if response
                .send(Response {
                    msg: "invalid command format".to_string(),
                })
                .is_err()
            {
                error!("response channel was closed");
            }
            error!("invalid command {}", command);

            return;
        }

        let node_id: String = parts[0].to_string();
        let mut term: u64 = 0;
        let mut last_log_index: u64 = 0;
        let mut last_log_term: u64 = 0;

        if let Ok(_term) = parts[2].parse::<u64>() {
            term = _term;
        } else {
            if response
                .send(Response {
                    msg: "invalid command format".to_string(),
                })
                .is_err()
            {
                error!("response channel was closed");
            }
            error!("invalid term {}", term);

            return;
        }

        if let Ok(_last_log_index) = parts[3].parse::<u64>() {
            last_log_index = _last_log_index;
        } else {
            if response
                .send(Response {
                    msg: "invalid command format".to_string(),
                })
                .is_err()
            {
                error!("response channel was closed");
            }
            error!("invalid last_log_index {}", last_log_index);

            return;
        }

        if let Ok(_last_log_term) = parts[4].parse::<u64>() {
            last_log_term = _last_log_term;
        } else {
            if response
                .send(Response {
                    msg: "invalid command format".to_string(),
                })
                .is_err()
            {
                error!("response channel was closed");
            }
            error!("invalid last_log_term {}", last_log_term);

            return;
        }

        let (success, new_term) =
            self.state
                .request_vote(node_id, term, last_log_index, last_log_term);
        if success {
            if self.storage.save(&self.state).is_err() {
                error!("persistent save failed");

                return;
            }
        }

        if response
            .send(Response {
                msg: format!("{success} {new_term}"),
            })
            .is_err()
        {
            error!("response channel was closed");

            return;
        }
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
}
