use crate::state::{NodeId, NodeRole};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Serialize, Deserialize)]
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
    #[serde(rename = "ErrorResponse")]
    Error(ErrorResponse),
}

pub struct NodeMessage {
    pub rpc_message: RpcMessage,
    pub resp_tx: Option<oneshot::Sender<RpcMessage>>,
}

#[derive(Serialize, Deserialize)]
pub struct SetRequest {
    pub command: String,
}

#[derive(Serialize, Deserialize)]
pub struct StateRequest {}

#[derive(Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub node_id: NodeId,
    pub term: u64,
    pub last_log_term: u64,
    pub last_log_index: u64,
}

#[derive(Serialize, Deserialize)]
pub struct SetResponse {
    pub ok: bool,
}

#[derive(Serialize, Deserialize)]
pub struct StateResponse {
    pub node_id: NodeId,
    pub term: u64,
    pub role: NodeRole,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestVoteResponse {
    pub success: bool,
    pub term: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ErrorResponse {
    pub err_msg: String,
}
