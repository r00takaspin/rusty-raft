use crate::state::NodeState;
use std::fs::File;
use std::io::{BufReader, BufWriter};

pub trait Storage: Send + Sync {
    fn load(&self) -> Result<Box<NodeState>, anyhow::Error>;
    fn save(&self, state: &NodeState) -> Result<(), anyhow::Error>;
}

pub struct JsonStorage {
    file_path: String,
}

impl Storage for JsonStorage {
    fn load(&self) -> Result<Box<NodeState>, anyhow::Error> {
        let file = File::open(&self.file_path)?;
        let reader = BufReader::new(file);
        let data: NodeState = serde_json::from_reader(reader)?;
        Ok(Box::new(data))
    }

    fn save(&self, state: &NodeState) -> Result<(), anyhow::Error> {
        let file = File::create(self.file_path.clone())?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, state)?;

        Ok(())
    }
}

impl JsonStorage {
    pub fn new(file_path: &str) -> Self {
        Self {
            file_path: file_path.to_string(),
        }
    }
}
