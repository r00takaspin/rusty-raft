use node::*;
use clap::Parser;
use std::io;
use std::thread::sleep;
use std::time::Duration;
use tokio::sync::mpsc;
use api::TcpApi;

#[macro_use]
extern crate log;
extern crate env_logger;

mod node;
mod state;
mod transport;
mod api;

#[tokio::main]
async fn main() {
    env_logger::init();

    let config = NodeConfig::parse();

    let (tx, mut rx) = mpsc::channel::<String>(32);

    let mut tcp_server = TcpApi::new(config.addr);

    tokio::spawn( async move {
        loop {
            debug!("msg received: {:?}", rx.recv().await.unwrap());
        }
    });

    tcp_server.run(tx.clone()).await;

    sleep(Duration::from_secs(50));

    ()
}
