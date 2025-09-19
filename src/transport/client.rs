use crate::messages::{NodeId, RpcMessage};
use anyhow::Error;
use messages::prelude::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::timeout;

#[async_trait]
pub trait Client: Send + Sync {
    async fn send(&self, peer: String, msg: RpcMessage) -> Result<(), Error>;
}

pub struct TcpClient {
    timeout: Duration,
    resp_tx: Sender<(String, RpcMessage)>,
    resp_rx: Receiver<(String, RpcMessage)>,
}

impl TcpClient {
    pub fn new(
        timeout: Duration,
        resp_tx: Sender<(String, RpcMessage)>,
        resp_rx: Receiver<(String, RpcMessage)>,
    ) -> Self {
        Self {
            timeout,
            resp_tx,
            resp_rx,
        }
    }

    pub async fn run(&mut self) {
        let mut rx = &mut self.resp_rx;
        let resp_tx = self.resp_tx.clone();
        let timeout = self.timeout.clone();

        loop {
            tokio::select! {
                Some((peer, msg)) = rx.recv() => {
                    let resp_tx = self.resp_tx.clone();

                    tokio::spawn(async move {
                        send_async(peer, msg, resp_tx, timeout).await
                    });
                }
            }
        }
    }
}

async fn send_async(
    peer: String,
    msg: RpcMessage,
    resp_tx: Sender<(String, RpcMessage)>,
    request_timeout: Duration,
) -> Result<(), Error> {
    let connect_future = TcpStream::connect(&peer);
    let stream = timeout(request_timeout, connect_future)
        .await
        .map_err(|err| err)??;

    let mut stream = stream;

    let request = serde_json::to_string(&msg)?;

    let write_future = async {
        stream.write_all(request.as_bytes()).await?;
        stream.flush().await?;
        Ok::<_, tokio::io::Error>(())
    };

    timeout(request_timeout, write_future)
        .await
        .map_err(|err| err)??;

    let mut reader = tokio::io::BufReader::new(stream);
    let mut response_line = String::new();

    let read_future = reader.read_line(&mut response_line);
    timeout(request_timeout, read_future)
        .await
        .map_err(|err| err)??;

    let _ = reader.into_inner().shutdown().await;

    let response: RpcMessage = serde_json::from_str(&response_line)?;

    Ok(resp_tx.send((peer, response)).await?)
}
