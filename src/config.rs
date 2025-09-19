use crate::state;
use crate::state::NodeId;
use anyhow::anyhow;
use clap::Parser;
use std::collections::HashMap;

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct NodeConfig {
    /// Unique node ID
    #[arg(short, long)]
    pub id: String,

    /// List of peer addresses (comma-separated)
    #[arg(
        short,
        long,
        value_parser = parse_key_val,
        value_delimiter = ',',
        num_args = 1..,
    )]
    pub nodes: Vec<(NodeId, String)>,

    /// Address to bind this node to
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    pub addr: String,

    /// Path to persistent file storage
    #[arg(short, long)]
    pub log_path: String,
}

fn parse_key_val(s: &str) -> Result<(NodeId, String), anyhow::Error> {
    let pos = s.find('=');
    if pos.is_none() {
        return Err(anyhow!("invalid KEY=value: no `=` found in `{}`", s));
    }

    let pos = pos.unwrap();

    Ok((s[..pos].to_string(), s[pos + 1..].to_string()))
}
