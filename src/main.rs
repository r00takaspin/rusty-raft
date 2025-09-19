use crate::messages::{NodeId, RpcMessage};
use crate::state::{Node, NodeState};
use crate::storage::{JsonStorage, Storage};
use clap::Parser;
use config::*;
use rand::Rng;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tokio::sync::broadcast::channel;
use tokio::sync::mpsc;
use tracing::Instrument;
use transport::server::TcpApi;

#[macro_use]
extern crate log;
extern crate env_logger;

mod config;
mod messages;
mod state;
mod storage;
mod transport;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = NodeConfig::parse();
    let (node_tx, node_rx) = mpsc::channel::<messages::NodeMessage>(32);
    let storage = JsonStorage::new(&config.log_path[..]);
    let mut node_state = NodeState::default(config.clone().id, config.nodes);

    match storage.load() {
        Ok(res) => {
            node_state = *res;

            info!("node state: {node_state:?}");
        }
        Err(err) => {
            if storage.save(&node_state).is_err() {
                error!("failed to save default node state: {err}");

                return;
            }

            info!("default node state: {node_state:?}");
        }
    }

    let (node_to_client_tx, node_to_client_rx) = mpsc::channel::<(String, RpcMessage)>(32);
    let (client_to_node_tx, client_to_node_rx) = mpsc::channel::<(String, RpcMessage)>(32);

    let mut client = transport::client::TcpClient::new(
        Duration::from_millis(200), //TODO: config
        client_to_node_tx.clone(),
        node_to_client_rx,
    );

    tokio::spawn(async move { client.run().await });

    // election timeout between 150 and 300 ms
    let election_timeout = Duration::from_millis(rand::rng().random_range(10000..=15000)); // TODO: config
    let heartbeat_timeout = election_timeout / 5;

    let mut node = Node::new(
        node_state,
        node_rx,
        node_to_client_tx,
        client_to_node_rx,
        Arc::new(Box::new(storage)),
        election_timeout,
        heartbeat_timeout,
    );

    let span = tracing::info_span!("node", node_id = %config.id, addr  = %config.addr);

    tokio::spawn(
        async move {
            node.run().await;
        }
        .instrument(span),
    );

    let mut tcp_server = TcpApi::new(config.addr);
    match tcp_server.run(node_tx.clone()).await {
        Ok(_) => info!("server exited successfully"),
        Err(err) => error!("server crashed: {err:?}"),
    }
}
