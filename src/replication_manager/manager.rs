use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::{net::TcpStream, sync::mpsc};

use crate::command::RespCommand;
use crate::error_helpers::{invalid_data, invalid_data_err};
use crate::{
    replication_manager::replica::Replica,
    resp::{RespCodec, RespValue},
};

pub struct ReplicationManager {
    replicas: Arc<Mutex<HashMap<String, Replica>>>, // Keyed by host:port
}

impl ReplicationManager {
    pub fn new() -> Self {
        Self {
            replicas: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn add_replica(
        &mut self,
        addr: String,
        socket: SocketAddr,
        stream: TcpStream,
    ) -> io::Result<()> {
        dbg!("called add_replica");

        let replica = Replica::new(socket, stream);
        self.replicas.lock().await.insert(addr, replica);
        Ok(())
    }

    pub async fn send_to_replicas(&self, command: RespCommand) -> io::Result<()> {
        dbg!("called");
        let replicas_guard = self.replicas.lock().await; // Lock the mutex asynchronously
        for (key, replica) in replicas_guard.iter() {
            dbg!(replica);
            replica.send(command.clone()).await?;
        }
        Ok(())
    }
}
