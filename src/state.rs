use crate::messages::*;
use crate::state::NodeRole::{Candidate, Follower, Leader};
use crate::storage::Storage;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

pub type NodeId = String;
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
    role: NodeRole,
    peers: Vec<String>,
    term: u64,
    log: Vec<LogEntry>,
    voted_for: Option<NodeId>,
    commit_index: Index,
    leader_id: Option<NodeId>,
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
            commit_index: 0,
            leader_id: None,
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

        return true;
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
                RpcMessage::AppendLogRequest(req) => self.handle_append_log(req),
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

    pub fn handle_append_log(&mut self, request: AppendLogRequest) -> RpcMessage {
        let (success, new_term) = self.state.append_log(
            request.leader_id, request.leader_term, request.prev_log_index, request.prev_log_term,
            request.leader_commit_index, request.leader_log,
        );

        if self.storage.save(&self.state).is_err() {
            return RpcMessage::Error(ErrorResponse{err_msg: "persistent save failed".into()});
        }

        RpcMessage::AppendLogResponse(AppendLogResponse{ok: success, term: new_term})
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
}
