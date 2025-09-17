use crate::state::{Node, NodeState};
use crate::storage::{JsonStorage, Storage};
use clap::Parser;
use config::*;
use std::fs::File;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::mpsc;
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
    let (tx, rx) = mpsc::channel::<messages::Request>(32);

    let storage = JsonStorage::new(&config.log_path[..]);
    let mut node_state = NodeState::default(config.id, config.peers);

    match storage.load() {
        Ok(res) => {
            node_state = *res;

            info!("loaded node state: {:?}", node_state);
        }
        Err(err) => {
            if storage.save(&node_state).is_err() {
                error!("failed to save default node state: {err}");

                return;
            }

            info!("loaded default node state: {:?}", node_state);
        }
    }

    let mut node = Node::new(node_state, rx, Arc::new(Box::new(storage)));

    tokio::spawn(async move {
        node.run().await;
    });

    let mut tcp_server = TcpApi::new(config.addr);
    match tcp_server.run(tx.clone()).await {
        Ok(_) => info!("server exited successfully"),
        Err(err) => error!("server crashed: {err:?}"),
    }
}
