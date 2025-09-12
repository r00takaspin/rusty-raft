use std::io;
use std::io::{Read};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
pub struct TcpApi {
    addr: String,
}

impl TcpApi {
    pub fn new(addr: String) -> Self {
        Self{
            addr,
        }
    }

    pub async fn run(&mut self, sender: Sender<String>) -> io::Result<()> {
        let listener = TcpListener::bind(self.addr.clone()).await?;


        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await?;
                let  s = sender.clone();

                tokio::spawn(async move {
                    handle_conn(socket, s).await;
                });
            }
        }).await?
    }
}

async fn handle_conn(mut socket: tokio::net::TcpStream, tx: mpsc::Sender<String>)  {
    loop {
        let mut buf: [u8; 1024] = [0; 1024];

        match socket.read(&mut buf).await {
            Ok(0) => (),
            Ok(n) => {
                let msg = String::from_utf8(buf[..n].to_vec()).unwrap();
                let result = handle_msg(msg.as_str(), tx.clone()).await;

                match result {
                    Ok(()) => {
                        println!("Message handled successfully");
                    }
                    Err(e) => {
                        eprintln!("Error handling message: {}", e);
                    }
                }

            }
            Err(e) => {
                error!("Read error: {}", e);
            }
        }
    }
}

async fn handle_msg(msg: &str, tx: Sender<String>) -> Result<(), Box<dyn std::error::Error>> {
    if msg.len() < 3 {
        Err("Invalid message length")?
    }

    if &msg[0..3] == "set" {
        tx.send(msg.to_string()).await?;
        Ok(())
    } else {
        Err("Invalid message")?
    }
}