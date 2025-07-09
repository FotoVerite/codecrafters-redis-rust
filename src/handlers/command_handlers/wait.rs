
use std::{sync::Arc, time::Duration};

use tokio::sync::Mutex;

use crate::{
    command::{ReplconfCommand, RespCommand},
    resp::RespValue,
    replication_manager::manager::ReplicationManager,
    shared_store::shared_store::Store,
};

pub async fn wait_command(
    store: &Arc<Store>,
    manager: &Arc<Mutex<ReplicationManager>>,
    required_replicas: String,
    timeout_ms: String,
) -> Result<Option<RespValue>, Box<dyn std::error::Error>> {
    let offset = store.get_offset().await;
    let mut elapsed = 0;
    let poll_interval = 250;
    let ack_command = RespCommand::ReplconfCommand(ReplconfCommand::Getack("*".into()));

    {
        let guard = manager.lock().await;
        guard.send_to_replicas(ack_command.clone()).await?;
    }
    loop {
        let acked = {
            let manager = manager.lock().await;
            manager.replica_count(offset as u64).await?
        };
        if acked >= required_replicas.parse()? || elapsed >= timeout_ms.parse()? {
            break Ok(Some(RespValue::Integer(acked as i64)));
        }

        tokio::time::sleep(Duration::from_millis(poll_interval)).await;
        elapsed += poll_interval;
    }
}
