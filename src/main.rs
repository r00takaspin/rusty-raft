use crate::state::{Node, NodeState};
use clap::Parser;
use config::*;
use std::time::Duration;
use tokio::sync::mpsc;
use transport::server::TcpApi;

#[macro_use]
extern crate log;
extern crate env_logger;

mod config;
mod messages;
mod state;
mod transport;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = NodeConfig::parse();
    let (tx, rx) = mpsc::channel::<messages::Request>(32);

    tokio::spawn(async {
        let mut node = Node::new(
            NodeState::default(
                config.id,
                config.peers,
                Duration::from_millis(50),  // TODO: cfg
                Duration::from_millis(200), // TODO: cfg
            ),
            rx,
        );

        node.run().await;
    });

    let mut tcp_server = TcpApi::new(config.addr);
    match tcp_server.run(tx.clone()).await {
        Ok(_) => info!("server exited successfully"),
        Err(err) => error!("server crashed: {err:?}"),
    }
}
