use futures::future::select_all;
use std::{io, sync::Arc, time::Duration};
use tokio::{sync::Notify, task};

use crate::{resp::RespValue, shared_store::shared_store::Store};

async fn poll_lpop(store: &Arc<Store>, keys: &Vec<String>) -> io::Result<Option<RespValue>> {
    for key in keys {
        if let Some(resp) = store.lpop(key.to_string(), 1).await? {
            if !resp.is_empty() {
                let value = vec![
                    RespValue::BulkString(Some(key.as_bytes().into())),
                    RespValue::BulkString(Some(resp[0].clone())),
                ];
                return Ok(Some(RespValue::Array(value)));
            }
        }
    }
    Ok(None)
}

async fn try_poll_lpop(store: &Arc<Store>, keys: &Vec<String>) -> io::Result<Option<RespValue>> {
    let result = poll_lpop(store, keys).await?;
    if result.is_none() {
        Ok(None)
    } else {
        Ok(result)
    }
}
async fn wait_with_timeout(
    store: &Arc<Store>,
    keys: &Vec<String>,
    notifiers: &Vec<Arc<Notify>>,
    timeout_ms: u64,
) -> io::Result<Option<RespValue>> {
    let timeout = Duration::from_millis(timeout_ms);
    let futures = notifiers
        .iter()
        .map(|n| Box::pin(n.notified()))
        .collect::<Vec<_>>();

    tokio::select! {
        _ = select_all(futures) => {
            try_poll_lpop(store, keys).await
        }
        _ = tokio::time::sleep(timeout) => {
            Ok(Some(RespValue::BulkString(None)))
        }
    }
}

async fn wait_forever(
    store: &Arc<Store>,
    keys: &Vec<String>,
    notifiers: &Vec<Arc<Notify>>,
) -> io::Result<Option<RespValue>> {
    println!("Waiting Forever .");
    loop {
        let futures = notifiers
            .iter()
            .map(|n| Box::pin(n.notified()))
            .collect::<Vec<_>>();

        tokio::select! {
            _ = select_all(futures) => {
                println!("Waiting Forever called.");
                task::yield_now().await;
                if let Some(resp) = try_poll_lpop(store, keys).await? {
                    return Ok(Some(resp));
                }
            }
        }
    }
}

pub async fn blpop_command(
    store: &Arc<Store>,
    keys: &Vec<String>,
    timeout: u64,
) -> io::Result<Option<RespValue>> {
    // First, check if any stream already has entries
    if let Some(result) = try_poll_lpop(store, keys).await? {
        return Ok(Some(result));
    }
    // Get notifiers for the keys
    let notifiers = store.get_notifiers(keys).await;
    // Decide whether to wait with timeout or wait forever
    match timeout {
        0 => wait_forever(store, keys, &notifiers).await, // <- changed
        other => wait_with_timeout(store, keys, &notifiers, other).await,
    }
}
