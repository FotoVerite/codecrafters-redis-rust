use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time::interval;
use tokio::{net::TcpStream, sync::mpsc};

use crate::command::{ReplconfCommand, RespCommand};
use crate::error_helpers::{invalid_data, invalid_data_err};
use crate::replication_manager::replica::Replica;

pub struct ReplicationManager {
    replicas: Arc<Mutex<HashMap<String, Replica>>>, // Keyed by host:port
}

impl ReplicationManager {
    pub fn new() -> Self {
        let replicas = Arc::new(Mutex::new(HashMap::new()));
        Self { replicas }
    }

    pub async fn add_replica(
        &mut self,
        addr: &String,
        socket: SocketAddr,
        writer: OwnedWriteHalf,
    ) -> io::Result<()> {
        dbg!("called add_replica", &addr);

        let replica = Replica::new(socket, writer);
        self.replicas.lock().await.insert(addr.clone(), replica);
        Ok(())
    }

    pub async fn update_offset(&mut self, addr: &String, offset: u64) -> io::Result<()> {
        if let Some(replica) = self.replicas.lock().await.get_mut(addr) {
            replica.acknowledged_offset = offset
        }
        Ok(())
    }

    pub async fn replica_count(&self, offset: u64) -> io::Result<(usize)> {
        let guard = self.replicas.lock().await;
        //dbg!(guard.values().map(|r| r.acknowledged_offset).collect::<Vec<_>>());
        let len = guard.values().filter(|r| r.acknowledged_offset >= offset).count();
        Ok(len)
    }

    pub async fn send_to_replicas(&self, command: RespCommand) -> io::Result<()> {
        let replicas_guard = self.replicas.lock().await; // Lock the mutex asynchronously
        for (_key, replica) in replicas_guard.iter() {
            replica.send(command.clone()).await?;        }
        Ok(())
    }

}
