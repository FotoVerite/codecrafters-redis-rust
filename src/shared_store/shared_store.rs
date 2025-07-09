use futures::io;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tokio::time::Instant;

use crate::error_helpers::{invalid_data, invalid_data_err};
use crate::resp::RespValue;
use crate::shared_store::redis_stream::{Stream, StreamEntries};
use crate::shared_store::stream_id::StreamID;

#[derive(Debug, Clone)]
pub enum RedisValue {
    Text(Vec<u8>),
    Stream(Stream),
    // Add ZSet, List, etc. as needed
}

#[derive(Debug, Clone)]
pub struct Entry {
    value: RedisValue,
    expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: RedisValue, expires_at: Option<Instant>) -> Self {
        Self { value, expires_at }
    }
}
type SharedStore = Arc<RwLock<HashMap<String, Entry>>>;
type Log = Arc<RwLock<Vec<u8>>>;
#[derive(Debug, Clone)]
pub struct Store {
    keyspace: SharedStore,
    log: Log,
}

impl Store {
    pub fn new() -> Self {
        Self {
            keyspace: Arc::new(RwLock::new(HashMap::new())),
            log: Arc::new(RwLock::new(vec![])),
        }
    }

    pub async fn get(&self, key: &str) -> io::Result<RespValue> {
        let value = {
            if let Some(resp_value) = self._get(key).await? {
                match resp_value {
                    RedisValue::Text(value) => Some(value),
                    _ => Some("".to_string().as_bytes().to_vec()),
                }
            } else {
                None
            }
        };

        Ok(RespValue::BulkString(value))
    }

    pub async fn get_type(&self, key: &str) -> io::Result<RespValue> {
        match self._get(key).await? {
            Some(redis_value) => match redis_value {
                RedisValue::Stream(_) => Ok(RespValue::SimpleString("stream".into())),
                RedisValue::Text(_) => Ok(RespValue::SimpleString("string".into())),
            },
            None => Ok(RespValue::SimpleString("none".into())),
        }
    }

    async fn _get(&self, key: &str) -> io::Result<Option<RedisValue>> {
        let value = {
            let map = self.keyspace.read().await;
            let entry = map.get(key).cloned();
            if let Some(entry) = entry {
                match entry.expires_at {
                    Some(expiry) if Instant::now() >= expiry => None,
                    _ => Some(entry.value),
                }
            } else {
                None
            }
        };
        Ok(value)
    }

    pub async fn resolve_stream_ids(
        &self,
        keys: &Vec<String>,
        ids: &Vec<String>,
    ) -> io::Result<Vec<StreamID>> {
        if keys.len() != ids.len() {
            return Err(invalid_data_err("Mismatched keys and IDs"));
        }
        let map = self.keyspace.read().await;
        let mut ret = Vec::with_capacity(keys.len());
        for (key, id) in keys.iter().zip(ids) {
            match id.as_str() {
                "$" => {
                    let entry = map
                        .get(key)
                        .ok_or_else(|| invalid_data_err(format!("Missing key: {}", key)))?;

                    match &entry.value {
                        RedisValue::Stream(stream) => ret.push(stream.previous_id().clone()),
                        _ => return invalid_data("Key is not for a stream"),
                    }
                }
                _ => ret.push(id.as_str().try_into()?),
            }
        }

        Ok(ret)
    }

    pub async fn keys(&self) -> RespValue {
        let mut values = vec![];
        let map = self.keyspace.read().await;
        for key in map.keys() {
            let entry = map.get(key).cloned();
            if let Some(entry) = entry {
                match entry.expires_at {
                    Some(expiry) if Instant::now() >= expiry => (),
                    _ => {
                        values.push(RespValue::BulkString(Some(key.as_bytes().to_vec())));
                    }
                }
            } else {
                ()
            }
        }

        RespValue::Array(values)
    }

    pub async fn set(&self, key: &str, value: Vec<u8>, px: Option<u64>) {
        let mut map: tokio::sync::RwLockWriteGuard<'_, HashMap<String, Entry>> =
            self.keyspace.write().await;
        let expires_at = px.map(|ms| Instant::now() + Duration::from_millis(ms));

        let entry = Entry::new(RedisValue::Text(value), expires_at);
        map.insert(key.to_string(), entry);
    }

    pub async fn xrange(
        &self,
        key: String,
        start: Option<String>,
        end: Option<String>,
    ) -> io::Result<StreamEntries> {
        let map = self.keyspace.read().await;
        match map.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::Stream(stream) => {
                    let start: Option<StreamID> = match start {
                        Some(s) => {
                            if s == "-" {
                                None
                            } else {
                                Some(s.as_str().try_into()?)
                            }
                        }
                        None => None,
                    };
                    let end: Option<StreamID> = match end {
                        Some(s) => {
                            if s == "+" {
                                None
                            } else {
                                Some(s.as_str().try_into()?)
                            }
                        }
                        None => None,
                    };

                    let range = stream.get_range(start, end);
                    Ok(range)
                }
                _ => Err(invalid_data_err(
                    "ERR XRANGE on key holding the wrong kind of value",
                )),
            },
            None => Ok(vec![]), // Return empty on missing key
        }
    }

    pub async fn xread(&self, key: &String, start: &StreamID) -> io::Result<StreamEntries> {
        let map = self.keyspace.read().await;
        match map.get(key) {
            Some(entry) => match &entry.value {
                RedisValue::Stream(stream) => {
                    let range = stream.get_from(*start);
                    Ok(range)
                }
                _ => Err(invalid_data_err(
                    "ERR XREAD on key holding the wrong kind of value",
                )),
            },
            None => Ok(vec![]), // Return empty on missing key
        }
    }

    pub async fn get_notifiers(&self, keys: &Vec<String>) -> io::Result<Vec<Arc<Notify>>> {
        let map = self.keyspace.read().await;
        let mut ret = Vec::with_capacity(keys.len());
        for key in keys {
            let entry = map
                .get(key)
                .ok_or_else(|| invalid_data_err(format!("Missing key: {}", key)))?;

            match &entry.value {
                RedisValue::Stream(stream) => ret.push(stream.notify.clone()),
                _ => return invalid_data("Key is not for a stream"),
            }
        }
        Ok(ret)
    }

    pub async fn xadd(
        &self,
        key: &str,
        id: String,
        fields: Vec<(String, String)>,
    ) -> io::Result<String> {
        let mut map = self.keyspace.write().await;

        if let Some(entry) = map.get_mut(key) {
            match &mut entry.value {
                RedisValue::Text(_) => {
                    return Err(invalid_data_err("ERR calling Text Value with stream key"));
                }
                RedisValue::Stream(stream) => {
                    let stream_id: StreamID =
                        StreamID::from_redis_input(Some(*stream.previous_id()), id)?;
                    stream.append(stream_id.clone(), fields)?;
                    Ok(stream_id.to_string())
                }
            }
        } else {
            let stream_id: StreamID = StreamID::from_redis_input(None, id)?;
            let mut stream = Stream::new();
            stream.append(stream_id.clone(), fields)?;
            let entry = Entry::new(RedisValue::Stream(stream), None);
            map.insert(key.to_string(), entry);
            Ok(stream_id.to_string())
        }
    }

    // pub async fn del(&self, key: &str) {
    //     let mut map = self.keyspace.write().await;
    //     map.remove(key);
    // }

    pub async fn append_to_log(&self, bytes: Vec<u8>) {
        let mut log = self.log.write().await;
        log.extend(bytes);
    }

    pub async fn get_offset(&self) -> usize {
        let log = self.log.read().await;
        log.len()
    }
}
