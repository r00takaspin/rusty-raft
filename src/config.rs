use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct NodeConfig {
    /// Unique node ID
    #[arg(short, long)]
    pub id: String,

    /// List of peer addresses (comma-separated)
    #[arg(short, long, value_delimiter = ',', num_args = 1..)]
    pub peers: Vec<String>,

    /// Address to bind this node to
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    pub addr: String,
}
