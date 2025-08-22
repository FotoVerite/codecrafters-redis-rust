use futures::future::select_all;
use std::{io, sync::Arc, time::Duration};
use tokio::sync::Notify;

use crate::{resp::RespValue, shared_store::shared_store::Store};

/// Attempt to pop from any key immediately.
async fn poll_lpop(store: &Arc<Store>, keys: &[String]) -> io::Result<Option<RespValue>> {
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

/// Try to pop once; returns None if no data.
async fn try_poll_lpop(store: &Arc<Store>, keys: &[String]) -> io::Result<Option<RespValue>> {
    poll_lpop(store, keys).await
}

/// Wait with a timeout for any key to receive a push.
async fn wait_with_timeout(
    store: &Arc<Store>,
    keys: &[String],
    notifiers: &[Arc<Notify>],
    timeout_ms: u64,
) -> io::Result<Option<RespValue>> {
    let timeout = Duration::from_millis(timeout_ms);
    let futures = notifiers.iter().map(|n| Box::pin(n.notified())).collect::<Vec<_>>();

    tokio::select! {
        _ = select_all(futures) => {
            try_poll_lpop(store, keys).await
        }
        _ = tokio::time::sleep(timeout) => {
            Ok(Some(RespValue::BulkString(None))) // $-1\r\n for timeout
        }
    }
}

/// Wait forever until a value is available.
async fn wait_forever(
    store: &Arc<Store>,
    keys: &[String],
    notifiers: &[Arc<Notify>],
) -> io::Result<Option<RespValue>> {
    loop {
        // Poll first: maybe a value appeared while awaiting
        if let Some(resp) = try_poll_lpop(store, keys).await? {
            return Ok(Some(resp));
        }

        // No value yet: wait for any notifier
        let futures = notifiers.iter().map(|n| Box::pin(n.notified())).collect::<Vec<_>>();
        select_all(futures).await;

        // Once notified, loop again to attempt poll
    }
}

/// Main BLPOP command entry
pub async fn blpop_command(
    store: &Arc<Store>,
    keys: &[String],
    timeout: u64,
) -> io::Result<Option<RespValue>> {
    // Lock keyspace while doing both
    let notifiers = store.get_notifiers(keys).await; // register first

    if let Some(result) = try_poll_lpop(store, keys).await? {
        // pop first if any value exists
        return Ok(Some(result));
    }

    if timeout == 0 {
        wait_forever(store, keys, &notifiers).await
    } else {
        wait_with_timeout(store, keys, &notifiers, timeout).await
    }
}
