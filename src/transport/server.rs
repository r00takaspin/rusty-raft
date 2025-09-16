use crate::messages::Request;
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
    fn success(msg: String) -> Self {
        Self {
            msg: format!("{}\n", msg),
            is_exit: false,
        }
    }

    fn error(error_msg: String) -> Self {
        Self {
            msg: format!("error: {}\n", error_msg),
            is_exit: false,
        }
    }

    fn exit() -> Self {
        Self {
            msg: "bye\n".to_string(),
            is_exit: true,
        }
    }

    fn unknown(command: &str) -> Self {
        Self {
            msg: format!("unknown command: {}\n", command),
            is_exit: false,
        }
    }
}

impl TcpApi {
    pub fn new(addr: String) -> Self {
        Self { addr }
    }

    pub async fn run(&mut self, sender: Sender<Request>) -> io::Result<()> {
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

async fn handle_conn(mut socket: TcpStream, tx: Sender<Request>) {
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

async fn handle_request(request_msg: &str, tx: Sender<Request>) -> Result<Response> {
    let trimmed_msg = request_msg.trim();

    match trimmed_msg {
        "exit" | "quit" => Ok(Response::exit()),
        cmd if cmd == "state" || cmd.starts_with("set") || cmd.starts_with("request_vote") => {
            handle_command(cmd, tx).await
        }
        _ => Ok(Response::unknown(trimmed_msg)),
    }
}

async fn handle_command(command: &str, tx: Sender<Request>) -> Result<Response> {
    let (resp_tx, resp_rx) = oneshot::channel();

    tx.send(Request {
        command: command.to_string(),
        resp_tx,
    })
    .await
    .map_err(|e| anyhow!("failed to send request: {}", e))?;

    match resp_rx.await {
        Ok(resp) => Ok(Response::success(resp.msg)),
        Err(e) => Ok(Response::error(format!("receive error: {}", e))),
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
