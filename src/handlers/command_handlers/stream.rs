
use std::{io, sync::Arc, time::Duration};
use futures::future::select_all;
use tokio::{
    sync::Notify,
    task,
};

use crate::{
    resp::RespValue,
    shared_store::{redis_stream::StreamEntry, shared_store::Store, stream_id::StreamID},
};

pub fn encode_stream(resp: Vec<(StreamID, StreamEntry)>) -> Vec<RespValue> {
    let mut outer = vec![];
    for (_, entry) in resp {
        match entry {
            StreamEntry::Data { id, fields } => {
                let bulkstring_id = RespValue::BulkString(Some(id.to_string().into()));
                let field_array = {
                    let values = {
                        fields
                            .iter()
                            .flat_map(|(k, v)| {
                                vec![
                                    RespValue::BulkString(Some(k.clone().into())),
                                    RespValue::BulkString(Some(v.clone().into())),
                                ]
                            })
                            .collect()
                    };
                    RespValue::Array(values)
                };
                outer.push(RespValue::Array(vec![bulkstring_id, field_array]));
            }
        }
    }
    outer
}

async fn poll_xread(
    store: &Arc<Store>,
    keys: &Vec<String>,
    ids: &[StreamID],
) -> io::Result<Vec<RespValue>> {
    let mut outer = vec![];
    for (key, id) in keys.iter().zip(ids) {
        let resp = store.xread(key, id).await?;
        if !resp.is_empty() {
            let inner = RespValue::Array(encode_stream(resp));
            let full = vec![RespValue::BulkString(Some(key.clone().into_bytes())), inner];
            outer.push(RespValue::Array(full));
        }
    }
    Ok(outer)
}

async fn try_poll_xread(
    store: &Arc<Store>,
    keys: &Vec<String>,
    ids: &Vec<StreamID>,
) -> io::Result<Option<RespValue>> {
    let result = poll_xread(store, keys, ids).await?;
    if result.is_empty() {
        Ok(None)
    } else {
        Ok(Some(RespValue::Array(result)))
    }
}
async fn wait_with_timeout(
    store: &Arc<Store>,
    keys: &Vec<String>,
    ids: &Vec<StreamID>,
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
            try_poll_xread(store, keys, ids).await
        }
        _ = tokio::time::sleep(timeout) => {
            Ok(Some(RespValue::BulkString(None)))
        }
    }
}

async fn wait_forever(
    store: &Arc<Store>,
    keys: &Vec<String>,
    ids: &Vec<StreamID>,
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

                if let Some(resp) = try_poll_xread(store, keys, ids).await? {
                    dbg!(&resp);
                    return Ok(Some(resp));
                }
            }
        }
    }
}

pub async fn xread_command(
    store: &Arc<Store>,
    block: &Option<u64>,
    keys: &Vec<String>,
    ids: &Vec<String>,
) -> io::Result<Option<RespValue>> {
    let ids = store.resolve_stream_ids(keys, ids).await?;
    // First, check if any stream already has entries
    if let Some(result) = try_poll_xread(store, keys, &ids).await? {
        return Ok(Some(result));
    }

    // Get notifiers for the keys
    let notifiers = store.get_notifiers(keys).await?;

    // Decide whether to wait with timeout or wait forever
    match block {
        Some(0) => wait_forever(store, keys, &ids, &notifiers).await, // <- changed
        Some(ms) => wait_with_timeout(store, keys, &ids, &notifiers, *ms).await,
        None => try_poll_xread(store, keys, &ids).await,
    }
}
