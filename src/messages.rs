use crate::state;
pub(crate) use crate::state::{Index, NodeId, NodeRole};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "data")]
pub enum RpcMessage {
    #[serde(rename = "SetRequest")]
    SetRequest(SetRequest),
    #[serde(rename = "SetResponse")]
    SetResponse(SetResponse),
    #[serde(rename = "RequestVote")]
    RequestVote(RequestVoteRequest),
    #[serde(rename = "RequestVoteResponse")]
    RequestVoteResponse(RequestVoteResponse),
    #[serde(rename = "StateRequest")]
    StateRequest(StateRequest),
    #[serde(rename = "StateResponse")]
    StateResponse(StateResponse),
    #[serde(rename = "AppendLogRequest")]
    AppendLogRequest(AppendLogRequest),
    #[serde(rename = "AppendLogResponse")]
    AppendLogResponse(AppendLogResponse),
    #[serde(rename = "HeartbeatRequest")]
    HeartbeatRequest(HeartbeatRequest),
    #[serde(rename = "HeartbeatResponse")]
    HeartbeatResponse(HeartbeatResponse),
    #[serde(rename = "ErrorResponse")]
    Error(ErrorResponse),
}

pub struct NodeMessage {
    pub rpc_message: RpcMessage,
    pub resp_tx: Option<oneshot::Sender<RpcMessage>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SetRequest {
    pub command: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StateRequest {}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVoteRequest {
    pub node_id: NodeId,
    pub term: u64,
    pub last_log_term: u64,
    pub last_log_index: Index,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SetResponse {
    pub ok: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StateResponse {
    pub node_id: NodeId,
    pub term: u64,
    pub role: NodeRole,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVoteResponse {
    pub ok: bool,
    pub term: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ErrorResponse {
    pub err_msg: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendLogRequest {
    pub leader_id: NodeId,
    pub leader_term: u64,
    pub prev_log_index: Index,
    pub prev_log_term: u64,
    pub leader_commit_index: Index,
    pub leader_log: Vec<state::LogEntry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendLogResponse {
    pub ok: bool,
    pub term: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HeartbeatRequest {
    pub leader_id: NodeId,
    pub leader_term: u64,
    pub prev_log_index: Index,
    pub prev_log_term: u64,
    pub leader_commit_index: Index,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HeartbeatResponse {
    pub ok: bool,
    pub term: u64,
}
