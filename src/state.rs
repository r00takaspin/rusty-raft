use crate::state::NodeRole::{Candidate, Follower, Leader};
use anyhow::anyhow;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Clone, PartialEq, Eq)]
enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

type NodeId = String;
type Index = u64;

struct LogEntry {
    index: Index,
    term: u64,
    command: String,
}

struct NodeState {
    node_id: NodeId,
    role: NodeRole,
    peers: Vec<String>,
    term: u64,
    heartbeat_timeout: Duration,
    leader_election_timeout: Duration,
    log: Arc<Mutex<Vec<LogEntry>>>,
    current_index: u64,
    commited_index: u64,
    current_leader: Option<NodeId>,
}

impl NodeState {
    pub fn new(
        node_id: NodeId,
        peers: Vec<String>,
        heartbeat_timeout: Duration,
        leader_election_timeout: Duration,
    ) -> NodeState {
        NodeState {
            node_id,
            peers,
            role: Follower,
            term: 0,
            heartbeat_timeout,
            leader_election_timeout,
            log: Arc::new(Mutex::new(Vec::new())),
            current_index: 0,
            commited_index: 0,
            current_leader: None,
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
