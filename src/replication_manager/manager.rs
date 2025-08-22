use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

use crate::command::RespCommand;
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
        addr: &str,
        socket: SocketAddr,
        writer: OwnedWriteHalf,
    ) -> io::Result<()> {

        let replica = Replica::new(socket, writer);
        self.replicas.lock().await.insert(addr.to_string(), replica);
        Ok(())
    }

    pub async fn update_offset(&mut self, addr: &String, offset: u64) -> io::Result<()> {
        if let Some(replica) = self.replicas.lock().await.get_mut(addr) {
            replica.acknowledged_offset = offset
        }
        Ok(())
    }

    pub async fn replica_count(&self, offset: u64) -> io::Result<usize> {
        let guard = self.replicas.lock().await;
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
