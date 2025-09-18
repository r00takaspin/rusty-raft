use crate::messages::*;
use anyhow::{Result, anyhow};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tracing::Instrument;

pub struct TcpApi {
    addr: String,
}

#[derive(Clone)]
struct Response {
    msg: String,
    is_exit: bool,
}

impl Response {
    fn new(msg: String, is_exit: bool) -> Self {
        Self {
            msg: format!("{msg}\n"),
            is_exit,
        }
    }
}

impl TcpApi {
    pub fn new(addr: String) -> Self {
        Self { addr }
    }

    pub async fn run(&mut self, sender: Sender<NodeMessage>) -> io::Result<()> {
        let listener = TcpListener::bind(self.addr.clone()).await?;

        tokio::spawn(async move {
            loop {
                let (socket, addr) = listener.accept().await?;
                let s = sender.clone();
                let span = tracing::info_span!("client_ip", client_ip = %addr);

                tokio::spawn(
                    async move {
                        handle_conn(socket, s).await;
                    }
                    .instrument(span),
                );
            }
        })
        .await?
    }
}

async fn handle_conn(mut socket: TcpStream, tx: Sender<NodeMessage>) {
    loop {
        let mut buf: [u8; 1024] = [0; 1024];

        let n = match socket.read(&mut buf).await {
            Ok(0) => {
                info!("<- client disconnected");
                return;
            }
            Ok(n) => n,
            Err(err) => {
                error!("read socket error: {err}");
                return;
            }
        };

        let msg = match String::from_utf8(buf[..n].to_vec()) {
            Ok(m) => m.trim().to_string(),
            Err(err) => {
                error!("parse utf8 string error: {err}");
                continue;
            }
        };

        info!("-> client {}", msg.trim());

        match handle_request(&msg, tx.clone()).await {
            Ok(response) => {
                if let Err(err) = handle_response(&response, &mut socket).await {
                    error!("socket write error: {err:?}");
                    return;
                }

                if !response.msg.is_empty() {
                    info!("<- client: {}", response.msg.trim());
                }

                // client closed connection
                if response.is_exit {
                    return;
                }
            }
            Err(err) => {
                error!("socket read error: {err}");
            }
        }
    }
}

async fn handle_request(
    request_msg: &str,
    tx: Sender<NodeMessage>,
) -> Result<Response, anyhow::Error> {
    let (resp_tx, resp_rx) = oneshot::channel();

    let message_json: serde_json::Value = serde_json::from_str(request_msg)?;
    let message: RpcMessage = serde_json::from_value(message_json.clone())?;

    let node_message = NodeMessage {
        rpc_message: message,
        resp_tx: Some(resp_tx),
    };

    tx.send(node_message)
        .await
        .map_err(|e| anyhow!("failed to send request: {}", e))?;

    match resp_rx.await {
        Ok(RpcMessage::SetResponse(resp)) => {
            let msg = serde_json::to_string(&resp)?;

            Ok(Response::new(msg, false))
        }
        Ok(RpcMessage::StateResponse(resp)) => {
            let msg = serde_json::to_string(&resp)?;

            Ok(Response::new(msg, false))
        }
        Ok(RpcMessage::RequestVoteResponse(resp)) => {
            let msg = serde_json::to_string(&resp)?;

            Ok(Response::new(msg, false))
        }
        Ok(RpcMessage::AppendLogResponse(resp)) => {
            let msg = serde_json::to_string(&resp)?;

            Ok(Response::new(msg, false))
        }
        _ => {
            let resp = RpcMessage::Error(ErrorResponse {
                err_msg: "unknown request".to_string(),
            });

            let msg = serde_json::to_string(&resp)?;

            Ok(Response::new(msg, false))
        }
    }
}

async fn handle_response(response: &Response, socket: &mut TcpStream) -> Result<()> {
    socket
        .write_all(response.msg.as_bytes())
        .await
        .map_err(|err| anyhow!("socket write error: {err:?}"))?;

    if response.is_exit {
        socket
            .shutdown()
            .await
            .map_err(|err| anyhow!("socket shutdown error: {err}"))?;
    }

    Ok(())
}
