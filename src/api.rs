use anyhow::{Result, anyhow};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tracing::Instrument;

pub struct TcpApi {
    addr: String,
}

#[derive(Clone)]
struct Response {
    msg: String,
    is_exit: bool,
}

impl TcpApi {
    pub fn new(addr: String) -> Self {
        Self { addr }
    }

    pub async fn run(&mut self, sender: Sender<String>) -> io::Result<()> {
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

async fn handle_conn(mut socket: TcpStream, tx: Sender<String>) {
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

        info!("-> client {}", msg);

        match handle_request(&msg, tx.clone()).await {
            Ok(response) => {
                if let Err(err) = handle_response(&response, &mut socket).await {
                    error!("socket write error: {err:?}");
                    return;
                }

                if !response.msg.is_empty() {
                    info!("<- client: {}", response.msg);
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

async fn handle_request(request_msg: &str, tx: Sender<String>) -> Result<Response> {
    if request_msg.starts_with("set") {
        tx.send(request_msg.trim().to_string()).await?;
        Ok(Response {
            msg: "ok\n".to_string(),
            is_exit: false,
        })
    } else if request_msg.eq("state") {
        Ok(Response {
            msg: "state: unknown\n".to_string(),
            is_exit: false,
        })
    } else if request_msg.eq("exit") || request_msg.starts_with("quit") {
        Ok(Response {
            msg: "bye\n".to_string(),
            is_exit: true,
        })
    } else {
        Ok(Response {
            msg: format!("unknown command: {}\n", request_msg),
            is_exit: false,
        })
    }
}

async fn handle_response(response: &Response, socket: &mut TcpStream) -> Result<(), anyhow::Error> {
    match socket.write_all(response.msg.as_bytes()).await {
        Ok(_) => {}
        Err(err) => return Err(anyhow!("socket write error: {err:?}")),
    }

    if response.is_exit {
        match socket.shutdown().await {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!("socket shutdown error: {err}")),
        }
    } else {
        Ok(())
    }
}
