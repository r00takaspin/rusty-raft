use api::TcpApi;
use clap::Parser;
use node::*;
use tokio::sync::mpsc;

#[macro_use]
extern crate log;
extern crate env_logger;

mod api;
mod node;
mod state;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config = NodeConfig::parse();
    let (tx, _rx) = mpsc::channel::<String>(32);
    let mut tcp_server = TcpApi::new(config.addr);

    match tcp_server.run(tx.clone()).await {
        Ok(_) => info!("server exited successfully"),
        Err(_) => error!("TCP server crashed"),
    }
}
