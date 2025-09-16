use crate::messages;
use crate::messages::Response;
use crate::state::NodeRole::{Candidate, Follower, Leader};
use anyhow::anyhow;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Mutex, oneshot};

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

pub struct NodeState {
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
    pub fn default(
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

pub struct Node {
    state: Arc<Mutex<NodeState>>,
    rx: Receiver<messages::Request>,
}

impl Node {
    pub fn new(state: NodeState, rx: Receiver<messages::Request>) -> Node {
        Node {
            state: Arc::new(Mutex::new(state)),
            rx,
        }
    }

    pub async fn run(&mut self) {
        while let Some(request) = self.rx.recv().await {
            let params: Vec<&str> = request.command.split(' ').collect();

            match params[0] {
                "set" => self.handle_set(request.command, request.resp_tx),
                "state" => self.handle_state(request.resp_tx).await,
                "request_vote" => self.request_vote(request.command, request.resp_tx).await,
                _ => info!("unsupported command {}", request.command),
            }
        }
    }

    pub fn handle_set(&mut self, _: String, response: oneshot::Sender<Response>) {
        if response
            .send(Response {
                msg: "Ok".to_string(),
            })
            .is_err()
        {
            error!("response channel was closed");
        }
    }

    pub async fn handle_state(&mut self, response: oneshot::Sender<Response>) {
        let state = self.state.lock().await;

        let response_str = format!(
            "node_id: {}, role: {}, term: {}, peers: {:?}",
            state.node_id, state.role, state.term, state.peers,
        );

        if response.send(Response { msg: response_str }).is_err() {
            error!("response channel was closed");
        }
    }

    pub async fn request_vote(&mut self, command: String, response: oneshot::Sender<Response>) {
        // 0 - "request_vote"
        // 1 - node_id
        // 2 - term
        // 3 - last_log_index
        // 4 - last_log_term

        let parts: Vec<&str> = command.split(' ').collect();

        if parts.len() !=5 {
            if response.send(Response{msg: "invalid command format".to_string()}).is_err() {
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
            if response.send(Response{msg: "invalid command format".to_string()}).is_err() {
                error!("response channel was closed");
            }
            error!("invalid term {}", term);

            return;
        }

        if let Ok(_last_log_index) = parts[3].parse::<u64>() {
            last_log_index = _last_log_index;
        } else {
            if response.send(Response{msg: "invalid command format".to_string()}).is_err() {
                error!("response channel was closed");
            }
            error!("invalid last_log_index {}", term);

            return;
        }

        if let Ok(_last_log_term) = parts[4].parse::<u64>() {
            last_log_term = _last_log_term;
        } else {
            if response.send(Response{msg: "invalid command format".to_string()}).is_err() {
                error!("response channel was closed");
            }
            error!("invalid last_log_term {}", term);

            return;
        }

        response.send(Response{msg: "todo".to_string()});
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
