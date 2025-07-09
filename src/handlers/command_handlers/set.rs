
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    command::RespCommand,
    replication_manager::manager::ReplicationManager,
    resp::RespValue,
    shared_store::shared_store::Store,
};

pub async fn set_command(
    store: &Arc<Store>,
    manager: &Arc<Mutex<ReplicationManager>>,
    key: String,
    value: &Vec<u8>,
    px: Option<u64>,
    bytes: Vec<u8>,
) -> Result<Option<RespValue>, Box<dyn std::error::Error>> {
    store.set(&key, value.clone(), px).await;
    store.append_to_log(bytes).await;

    let copied_command = RespCommand::Set {
        key,
        value: value.clone(),
        px,
    };
    let guard = manager.lock().await;
    guard.send_to_replicas(copied_command).await?;
    Ok(Some(RespValue::SimpleString("OK".into())))
}
