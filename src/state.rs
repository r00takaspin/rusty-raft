use crate::messages::*;
use crate::state::NodeRole::{Candidate, Follower, Leader};
use crate::storage::Storage;
use crate::transport::client::Client;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{Instant, sleep_until};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

pub type NodeId = String;
pub type NodeAddr = String;

pub type Index = usize;

#[derive(Serialize, Deserialize, Debug)]
pub struct LogEntry {
    term: u64,
    index: usize,
    command: String,
    commit_index: Option<Index>,
}

impl LogEntry {
    pub fn new(index: usize, term: u64, command: &str, commit_index: Option<Index>) -> Self {
        Self {
            index,
            term,
            command: command.to_string(),
            commit_index,
        }
    }

    pub fn initial() -> Self {
        Self {
            index: 0,
            term: 0,
            command: "initial".to_string(),
            commit_index: Some(0),
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
    nodes: Vec<(NodeId, NodeAddr)>,
    role: NodeRole,
    term: u64,
    log: Vec<LogEntry>,
    voted_for: Option<NodeId>,
    commit_index: Index,
    leader_id: Option<NodeId>,
    votes_num: usize,
}


impl NodeState {
    pub fn default(node_id: NodeId, nodes: Vec<(NodeId, NodeAddr)>) -> NodeState {
        // initialize node with initial replication log
        let mut log_entries = vec![];
        log_entries.push(LogEntry::initial());

        NodeState {
            node_id,
            nodes,
            role: Follower,
            term: 0,
            voted_for: None,
            log: log_entries,
            commit_index: 0,
            leader_id: None,
            votes_num: 0,
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
        if (self.log.len() - 1) > last_log_index {
            return (false, self.term);
        }

        // somehow new term less than current
        if !self.update_term(term) {
            return (false, self.term);
        }

        self.voted_for = Some(node_id);
        self.role = Follower;

        // successfully give vote to candidate
        (true, self.term)
    }

    fn append_log(
        &mut self,
        leader_id: NodeId,
        leader_term: u64,
        prev_log_index: Index,
        prev_log_term: u64,
        leader_commit_index: Index,
        leader_log: Vec<LogEntry>,
    ) -> (bool, u64) {
        // leader becomes follower if follower term is newer
        if self.term > leader_term {
            return (false, self.term);
        }

        if let Some(log_msg) = self.log.get(prev_log_index) {
            // previous log message doesn't match leader term
            if log_msg.term != prev_log_term {
                return (false, self.term);
            }
        // log messages doesn't exists on follower
        } else {
            return (false, self.term);
        }

        // replace log messages with leader log
        for _ in prev_log_index..self.log.len() - 1 {
            self.log.pop();
        }

        if !self.merge_logs(leader_log) {
            return (false, self.term);
        }

        // log consistency check ok
        if !self.update_term(leader_term) {
            error!("follower cannot update term to leader's term");

            return (false, self.term);
        }

        let last_index_to_commit = min(self.log.len() - 1, leader_commit_index);

        // TODO: apply log records in separate thread

        for i in self.commit_index..=last_index_to_commit {
            self.log[i as usize].commit_index = Some(leader_commit_index);
        }

        self.leader_id = Some(leader_id);
        self.commit_index = last_index_to_commit;
        self.role = Follower;

        (true, self.term)
    }

    fn heartbeat(
        &mut self,
        leader_id: NodeId,
        leader_term: u64,
        prev_log_index: Index,
        prev_log_term: u64,
        leader_commit_index: Index,
    ) -> (bool, u64) {
        // leader becomes follower if follower term is newer
        if self.term > leader_term {
            return (false, self.term);
        }

        // check follower log consistency
        if let Some(log_msg) = self.log.get(prev_log_index) {
            if log_msg.term != prev_log_term {
                return (false, self.term);
            }
        } else {
            return (false, self.term);
        }

        if !self.update_term(prev_log_term) {
            return (false, self.term);
        }

        let last_index_to_commit = min(self.log.len() - 1, leader_commit_index);

        // TODO: apply log records in separate thread

        for i in self.commit_index..=last_index_to_commit {
            self.log[i as usize].commit_index = Some(leader_commit_index);
        }

        self.leader_id = Some(leader_id);
        self.role = Follower;

        (true, self.term)
    }

    fn merge_logs(&mut self, leader_log_chunk: Vec<LogEntry>) -> bool {
        if leader_log_chunk.is_empty() {
            return true;
        }

        let mut curr_index = self.log[self.log.len() - 1].index + 1;

        for log in leader_log_chunk.iter() {
            if curr_index != log.index {
                return false;
            }

            curr_index += 1;
        }

        for log in leader_log_chunk.into_iter() {
            self.log.push(log);
        }

        true
    }

    pub fn become_follower(&mut self, term: u64) -> Result<(), anyhow::Error> {
        self.role.become_follower()?;
        self.term = term;
        self.voted_for = None;
        self.votes_num = 0;

        Ok(())
    }

    pub fn become_candidate(&mut self) -> Result<(), anyhow::Error> {
        self.voted_for = Some(self.node_id.clone());
        self.leader_id = None;
        self.role.become_candidate()?;
        self.term += 1;
        self.votes_num = 1; // votes for himself

        Ok(())
    }

    fn update_term(&mut self, term: u64) -> bool {
        if term >= self.term {
            self.term = term;

            return true;
        }

        false
    }

    // use only for test purposes
    fn add_log(&mut self, log: LogEntry) {
        self.log.push(log);
    }

    pub fn incr_votes(&mut self) -> Result<bool, anyhow::Error> {
        self.votes_num += 1;

        info!(
            "node: {} got {}/{} votes",
            self.node_id,
            self.votes_num,
            self.nodes.len()
        );

        if self.votes_num >= self.nodes.len() / 2 + 1 {
            self.role.become_leader()?;

            return Ok(true);
        }

        Ok(false)
    }
}

pub struct Node {
    state: NodeState,
    storage: Arc<Box<dyn Storage>>,
    api_rx: Receiver<NodeMessage>,
    client_tx: Sender<(NodeAddr, RpcMessage)>,
    client_rx: Receiver<(NodeAddr, RpcMessage)>,
    election_timeout: Duration,
    election_timer: Instant,
    heartbeat_timeout: Duration,
    heartbeat_timer: Instant,
}

impl Node {
    pub fn new(
        state: NodeState,
        api_rx: Receiver<NodeMessage>,
        client_tx: Sender<(NodeAddr, RpcMessage)>,
        client_rx: Receiver<(NodeAddr, RpcMessage)>,
        storage: Arc<Box<dyn Storage>>,
        election_timeout: Duration,
        heartbeat_timeout: Duration,
    ) -> Self {
        Self {
            state,
            api_rx,
            client_tx,
            client_rx,
            storage,
            election_timeout,
            election_timer: Instant::now() + election_timeout,
            heartbeat_timeout,
            heartbeat_timer: Instant::now() + heartbeat_timeout,
        }
    }

    pub async fn run(&mut self) {
        info!("starting as {}", self.state.role);

        loop {
            tokio::select! {
                // handle network responses
                Some((peer, response)) = self.client_rx.recv() => {
                    self.handle_response(peer, response);
                }
                // incoming api requests loop
                Some(request) = self.api_rx.recv() => {
                    if request.resp_tx.is_none() {
                        error!("node: request response channel is empty");
                    }

                    let rx_resp = request.resp_tx.unwrap();

                    let response = self.handle_request(request.rpc_message);

                    if rx_resp.send(response).is_err() {
                        error!("node: oneshot response channel closed");
                    }
                }
                _ = sleep_until(self.election_timer) => {
                    let _ = self.make_election().await.map_err(|err| {
                        error!("node: make_election failure: {:?}", err);
                    });
                },
                _ = sleep_until(self.heartbeat_timer) => {
                    let _ = self.send_heartbeats().await.map_err(|err| {
                        error!("node: send_heartbeats failure: {:?}", err);
                    });
                }
            }
        }
    }

    async fn make_election(&mut self) -> Result<(), anyhow::Error> {
        self.state.become_candidate()?;
        self.election_timer = Instant::now() + self.election_timeout;

        info!("election started");

        let state = &self.state;

        for (node_id, addr) in self.state.nodes.iter() {
            let node_id = node_id.clone();

            let msg = RpcMessage::RequestVote(RequestVoteRequest {
                node_id: state.node_id.clone(),
                term: state.term,
                last_log_index: state.log.len() - 1,
                last_log_term: state.log[self.state.log.len() - 1].term,
            });

            if self.client_tx.send((addr.clone(), msg)).await.is_err() {
                error!("node: failed to send message");
            }
        }

        Ok(())
    }

    async fn send_heartbeats(&mut self) -> Result<(), anyhow::Error> {
        if self.state.role != Leader {
            return Ok(());
        }

        info!("sending heartbeats");

        let state = &self.state;

        self.heartbeat_timer = Instant::now() + self.heartbeat_timeout;

        for (_, addr) in self.state.nodes.iter() {
            let msg = RpcMessage::HeartbeatRequest(HeartbeatRequest {
                leader_id: state.node_id.clone(),
                leader_term: state.term,
                prev_log_index: state.log.len() - 1,
                prev_log_term: state.log[self.state.log.len() - 1].term,
                leader_commit_index: self.state.commit_index,
            });

            if self.client_tx.send((addr.clone(), msg)).await.is_err() {
                error!("node: failed to send message");
            }
        }

        Ok(())
    }

    pub fn handle_request(&mut self, request: RpcMessage) -> RpcMessage {
        match request {
            RpcMessage::SetRequest(req) => self.handle_set(req),
            RpcMessage::StateRequest(req) => self.handle_state(req),
            RpcMessage::RequestVote(req) => self.handle_request_vote(req),
            RpcMessage::AppendLogRequest(req) => self.handle_append_log(req),
            RpcMessage::HeartbeatRequest(req) => self.handle_heartbeat(req),
            _ => RpcMessage::Error(ErrorResponse {
                err_msg: "unsupported rpc request".to_string(),
            }),
        }
    }

    pub fn handle_set(&mut self, request: SetRequest) -> RpcMessage {
        if self.state.role != Leader {
            return RpcMessage::Error(ErrorResponse{err_msg: "operation requires leader role".to_string()});
        }

        let nodes = self.state.nodes.clone();

        for (_, peer) in nodes.into_iter() {
            let request = request.clone();
            let peer = peer.clone();
            let request = RpcMessage::SetRequest(request);

            if let Err(err) = self.client_tx.try_send((peer, request)) {
                error!("node: failed to send message: {:?}", err);

                return RpcMessage::Error(ErrorResponse{err_msg: err.to_string()});
            }
        }

        RpcMessage::SetResponse(SetResponse{ok: true})
    }

    pub fn handle_state(&mut self, _: StateRequest) -> RpcMessage {
        RpcMessage::StateResponse(StateResponse {
            node_id: self.state.node_id.clone(),
            role: self.state.role.clone(),
            term: self.state.term,
        })
    }

    fn handle_request_vote(&mut self, request: RequestVoteRequest) -> RpcMessage {
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

            info!(
                "{} votes for {}",
                self.state.node_id,
                request.node_id.clone()
            );
        }

        if self.storage.save(&self.state).is_err() {
            return RpcMessage::Error(ErrorResponse {
                err_msg: "persistent save failed".into(),
            });
        }

        RpcMessage::RequestVoteResponse(RequestVoteResponse {
            ok: success,
            term: new_term,
        })
    }

    pub fn handle_append_log(&mut self, request: AppendLogRequest) -> RpcMessage {
        self.election_timer = Instant::now() + self.election_timeout;

        let (success, new_term) = self.state.append_log(
            request.leader_id,
            request.leader_term,
            request.prev_log_index,
            request.prev_log_term,
            request.leader_commit_index,
            request.leader_log,
        );

        if self.storage.save(&self.state).is_err() {
            return RpcMessage::Error(ErrorResponse {
                err_msg: "persistent save failed".into(),
            });
        }

        RpcMessage::AppendLogResponse(AppendLogResponse {
            ok: success,
            term: new_term,
        })
    }

    pub fn handle_heartbeat(&mut self, request: HeartbeatRequest) -> RpcMessage {
        self.election_timer = Instant::now() + self.election_timeout;

        let (success, new_term) = self.state.heartbeat(
            request.leader_id,
            request.leader_term,
            request.prev_log_index,
            request.prev_log_term,
            request.leader_commit_index,
        );

        if self.storage.save(&self.state).is_err() {
            return RpcMessage::Error(ErrorResponse {
                err_msg: "persistent save failed".into(),
            });
        }

        RpcMessage::HeartbeatResponse(HeartbeatResponse {
            ok: success,
            term: new_term,
        })
    }

    fn handle_response(&mut self,peer: String, response: RpcMessage) {
        match response {
            RpcMessage::RequestVoteResponse(resp) => self.handle_request_vote_response(peer, resp),
            RpcMessage::HeartbeatResponse(resp) => self.handle_heartbeat_response(peer, resp),
            _ => debug!("unhandled rpc response: {:?}", response),
        }
    }

    fn handle_request_vote_response(&mut self, peer: String, response: RequestVoteResponse) {
        self.election_timer = Instant::now() + self.election_timeout;

        if response.ok {
            match self.state.incr_votes() {
                Err(err) => error!("failed to increment votes: {}", err),
                Ok(true) => {
                    self.heartbeat_timer = Instant::now() + self.heartbeat_timeout;
                    info!("{} becomes leader!", self.state.node_id)
                }
                Ok(false) => info!("{} get new vote", self.state.node_id),
            }

            info!("request vote success {:?}", response);
        }

        if response.term > self.state.term {
            if let Err(err) = self.state.become_follower(response.term) {
                error!("failed to become follower: {}", err);
            }
        }
    }

    fn handle_heartbeat_response(&mut self, peer: String, response: HeartbeatResponse) {
        self.election_timer = Instant::now() + self.election_timeout;

        if response.ok {
            return
        }

        if response.term > self.state.term {
            if let Err(err) = self.state.become_follower(response.term) {
                error!("failed to become follower: {}", err);
            }
        }

        // todo: implement log forwarding
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
        let (got_ok, term) = state.request_vote("1".into(), 0, 0, 0);
        assert_eq!(false, got_ok);
        assert_eq!(0, term);
    }

    #[test]
    fn test_node_request_vote_lower_term() {
        let mut state = NodeState::default("1".into(), vec![]);
        state.update_term(1);
        let (got_ok, term) = state.request_vote("2".into(), 0, 0, 0);
        assert_eq!(false, got_ok);
        assert_eq!(1, term);
    }

    #[test]
    fn test_node_request_vote_same_term_twice() {
        let mut state = NodeState::default("1".into(), vec![]);
        let (got_ok, term) = state.request_vote("2".into(), 1, 0, 0);
        assert_eq!(true, got_ok);
        assert_eq!(1, term);

        let (got_ok, term) = state.request_vote("2".into(), 1, 0, 0);
        assert_eq!(false, got_ok);
        assert_eq!(1, term);
    }

    #[test]
    fn test_node_request_log_term_ahead() {
        let mut state = NodeState::default("1".into(), vec![]);
        state.add_log(LogEntry::new(1, 1, "test command", None));

        let (got_ok, term) = state.request_vote("2".into(), 1, 0, 0);
        assert_eq!(false, got_ok);
        assert_eq!(1, term);
    }

    #[test]
    fn test_node_request_log_index_ahead() {
        let mut state = NodeState::default("1".into(), vec![]);
        state.add_log(LogEntry::new(1, 1, "test command", None));

        let (got_ok, term) = state.request_vote("2".into(), 1, 0, 2);
        assert_eq!(false, got_ok);
        assert_eq!(0, term);
    }

    #[test]
    fn test_node_merge_logs_empty_log() {
        let mut state = NodeState::default("1".into(), vec![]);

        let ok = state.merge_logs(vec![]);
        assert_eq!(true, ok);
    }

    #[test]
    fn test_node_merge_logs_empty_log_success() {
        let mut state = NodeState::default("1".into(), vec![]);

        let leader_logs = vec![
            LogEntry::new(1, 1, "test command", None),
            LogEntry::new(2, 2, "test command#2", None),
            LogEntry::new(3, 2, "test command#3", None),
        ];

        let ok = state.merge_logs(leader_logs);
        assert_eq!(true, ok);
    }

    #[test]
    fn test_node_merge_logs_gap_between_logs() {
        let mut state = NodeState::default("1".into(), vec![]);
        let leader_logs = vec![LogEntry::new(2, 1, "test command", None)];

        let ok = state.merge_logs(leader_logs);
        assert_eq!(false, ok);
    }

    #[test]
    fn test_node_merge_logs_broken_index_order() {
        let mut state = NodeState::default("1".into(), vec![]);
        let leader_logs = vec![
            LogEntry::new(1, 1, "test command#1", None),
            LogEntry::new(3, 1, "test command#2", None),
            LogEntry::new(2, 1, "test command#3", None),
        ];

        let ok = state.merge_logs(leader_logs);
        assert_eq!(false, ok);
    }

    #[test]
    fn test_node_append_log_success() {
        let leader_id = "2".into();
        let leader_term = 1;
        let prev_log_index = 0;
        let prev_log_term = 0;
        let leader_commit_index = 0;

        let logs = vec![LogEntry::new(1, 1, "test command#1", None)];

        let mut state = NodeState::default("1".into(), vec![]);
        let (ok, term) = state.append_log(
            leader_id,
            leader_term,
            prev_log_index,
            prev_log_term,
            leader_commit_index,
            logs,
        );
        assert_eq!(true, ok);
        assert_eq!(leader_term, term);
    }

    #[test]
    fn test_node_append_log_follower_term_greater() {
        let leader_id = "2".into();
        let leader_term = 1;
        let prev_log_index = 0;
        let prev_log_term = 0;
        let leader_commit_index = 0;

        let logs = vec![LogEntry::new(1, 1, "test command#1", None)];
        let mut state = NodeState::default("1".into(), vec![]);
        state.update_term(leader_term + 1);
        let (ok, term) = state.append_log(
            leader_id,
            leader_term,
            prev_log_index,
            prev_log_term,
            leader_commit_index,
            logs,
        );
        assert_eq!(false, ok);
        assert_eq!(leader_term + 1, term);
    }

    #[test]
    fn test_node_append_log_prev_log_index_ahead() {
        let leader_id = "2".into();
        let leader_term = 1;
        let prev_log_index = 1;
        let prev_log_term = 0;
        let leader_commit_index = 0;

        let logs = vec![LogEntry::new(1, 1, "test command#1", None)];
        let mut state = NodeState::default("1".into(), vec![]);
        let (ok, term) = state.append_log(
            leader_id,
            leader_term,
            prev_log_index,
            prev_log_term,
            leader_commit_index,
            logs,
        );
        assert_eq!(false, ok);
        assert_eq!(0, term);
    }

    #[test]
    fn test_node_append_log_prev_log_term_incorrect() {
        let leader_id = "2".into();
        let leader_term = 2;
        let prev_log_index = 1;
        let prev_log_term = 2;
        let leader_commit_index = 0;

        let logs = vec![LogEntry::new(1, 1, "test command#1", None)];
        let mut state = NodeState::default("1".into(), vec![]);
        state.add_log(LogEntry::new(1, 1, "test command#2", None));
        let (ok, term) = state.append_log(
            leader_id,
            leader_term,
            prev_log_index,
            prev_log_term,
            leader_commit_index,
            logs,
        );
        assert_eq!(false, ok);
        assert_eq!(0, term);
    }

    #[test]
    fn test_node_append_log_recover_log() {
        let leader_term = 3;
        let prev_log_index = 3;
        let prev_log_term = 3;
        let leader_commit_index = 2;

        let leader_logs = vec![LogEntry::new(3, 3, "test command#3", None)];
        let mut state = NodeState::default("1".into(), vec![]);

        // follower log
        state.add_log(LogEntry::new(1, 1, "test command#1", None));
        state.update_term(1);

        let (ok, term) = state.append_log(
            "2".into(),
            leader_term,
            prev_log_index - 1,
            prev_log_term - 1,
            leader_commit_index,
            leader_logs,
        );
        assert_eq!(false, ok);
        assert_eq!(1, term);

        // leader decrements prev_log_index and add new log messages to leader_log
        let leader_logs = vec![
            LogEntry::new(2, 2, "test command#2", None),
            LogEntry::new(3, 3, "test command#3", None),
        ];

        let (ok, term) = state.append_log(
            "2".into(),
            leader_term,
            prev_log_index - 2,
            prev_log_term - 2,
            leader_commit_index,
            leader_logs,
        );
        assert_eq!(true, ok);
        assert_eq!(leader_term, term);
        assert_eq!(state.log.len(), 4);
    }

    #[test]
    fn test_node_append_log_recover_conflict_log() {
        let leader_term = 3;
        let prev_log_index = 1;
        let prev_log_term = 1;
        let leader_commit_index = 2;

        let leader_logs = vec![
            LogEntry::new(2, 2, "actual test command#2", None),
            LogEntry::new(3, 2, "actual test command#2", None),
        ];

        let mut state = NodeState::default("1".into(), vec![]);
        state.add_log(LogEntry::new(1, 1, "sync test command#1", None));
        state.add_log(LogEntry::new(2, 1, "conflict test command#2", None));
        state.add_log(LogEntry::new(3, 1, "conflict test command#2", None));

        let (ok, term) = state.append_log(
            "2".into(),
            leader_term,
            prev_log_index,
            prev_log_term,
            leader_commit_index,
            leader_logs,
        );
        assert_eq!(true, ok);
        assert_eq!(leader_term, term);
        assert_eq!(state.log.len(), 4);
    }

    #[test]
    fn test_node_append_log_empty_leader_log() {
        let leader_term = 3;
        let prev_log_index = 3;
        let prev_log_term = 3;
        let leader_commit_index = 3;

        let mut state = NodeState::default("1".into(), vec![]);
        state.add_log(LogEntry::new(1, 1, "command#1", None));
        state.add_log(LogEntry::new(2, 2, "command#2", None));
        state.add_log(LogEntry::new(3, 3, "command#3", None));

        let (ok, term) = state.append_log(
            "2".into(),
            leader_term,
            prev_log_index,
            prev_log_term,
            leader_commit_index,
            vec![],
        );
        assert_eq!(true, ok);
        assert_eq!(leader_term, term);
        assert_eq!(3, state.commit_index);
    }

    #[test]
    fn test_append_log_incorrect_pop_logic() {
        let mut state = NodeState::default("1".into(), vec![]);
        state.add_log(LogEntry {
            index: 1,
            term: 1,
            commit_index: None,
            command: "test#1".into(),
        });
        state.add_log(LogEntry {
            index: 2,
            term: 1,
            commit_index: None,
            command: "test#2".into(),
        });

        let (ok, term) = state.append_log(
            "2".into(),
            2,
            1,
            1,
            0,
            vec![LogEntry {
                index: 2,
                term: 2,
                commit_index: None,
                command: "test#2".into(),
            }],
        );

        assert_eq!(ok, true);
        assert_eq!(term, 2);
    }

    #[test]
    fn test_append_log_commit_index_out_of_bounds() {
        let mut state = NodeState::default("1".into(), vec![]);

        let (ok, term) = state.append_log("2".into(), 1, 0, 0, 5, vec![]);

        assert_eq!(true, ok);
        assert_eq!(term, 1);
        assert_eq!(0, state.commit_index);
    }

    #[test]
    fn test_heartbreat_follower_term_ahead() {
        let mut state = NodeState::default("1".into(), vec![]);
        state.update_term(2);

        let (ok, term) = state.heartbeat("2".into(), 1, 0, 0, 0);

        assert_eq!(false, ok);
        assert_eq!(term, 2);
    }

    #[test]
    fn test_heartbeat_follower_log_index_not_exists() {
        let mut state = NodeState::default("1".into(), vec![]);

        let (ok, term) = state.heartbeat("2".into(), 0, 1, 0, 0);

        assert_eq!(false, ok);
        assert_eq!(term, 0);
    }

    #[test]
    fn test_heartbeat_follower_log_wrong_log_term() {
        let mut state = NodeState::default("1".into(), vec![]);
        state.add_log(LogEntry::new(1, 2, "test command#1", None));

        let (ok, term) = state.heartbeat("2".into(), 0, 1, 0, 0);

        assert_eq!(false, ok);
        assert_eq!(term, 0);
    }
}
