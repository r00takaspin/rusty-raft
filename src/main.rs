use crate::state::{Node, NodeState};
use crate::storage::{JsonStorage, Storage};
use clap::Parser;
use config::*;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
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
    let (tx, rx) = mpsc::channel::<messages::NodeMessage>(32);

    let storage = JsonStorage::new(&config.log_path[..]);
    let mut node_state = NodeState::default(config.clone().id, config.peers);

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

    // election timeout between 150 and 300 ms
    let election_timeout = Duration::from_millis(rand::rng().random_range(150..=300));

    let mut node = Node::new(
        node_state,
        rx,
        Arc::new(Box::new(storage)),
        election_timeout,
    );

    let span = tracing::info_span!("node", node_id = %config.id, addr  = %config.addr);

    tokio::spawn(
        async move {
            node.run().await;
        }
        .instrument(span),
    );

    let mut tcp_server = TcpApi::new(config.addr);
    match tcp_server.run(tx.clone()).await {
        Ok(_) => info!("server exited successfully"),
        Err(err) => error!("server crashed: {err:?}"),
    }
}
