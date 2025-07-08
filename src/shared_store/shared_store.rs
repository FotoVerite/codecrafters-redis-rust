use futures::io;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::error_helpers::{invalid_data, invalid_data_err};
use crate::resp::RespValue;
use crate::shared_store::redis_stream::{Stream, StreamEntry};

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

    pub async fn xadd(
        &self,
        key: &str,
        id: String,
        fields: Vec<(String, String)>,
    ) -> io::Result<String> {
        let mut map: tokio::sync::RwLockWriteGuard<'_, HashMap<String, Entry>> =
            self.keyspace.write().await;

        if let Some(entry) = map.get_mut(key) {
            match &mut entry.value {
                RedisValue::Text(_) => {
                    return Err(invalid_data_err("ERR calling Text Value with stream key"));
                }
                RedisValue::Stream(stream) => {
                    let generated_id = Self::generate_id(&id, Some(stream.previous_id()))?;
                    dbg!(&generated_id);
                    Self::validate_id(&generated_id, stream.previous_id())?;
                    stream.append(generated_id.clone(), fields);
                    Ok(generated_id)
                }
            }
        } else {
            let generated_id = Self::generate_id(&id, None)?;
            dbg!(&generated_id);
            Self::validate_id(&generated_id, "0-0")?;
            let mut stream = Stream::new();
            stream.append(generated_id.clone(), fields);
            let entry = Entry::new(RedisValue::Stream(stream), None);
            map.insert(key.to_string(), entry);
            Ok(generated_id)
        }
    }

    fn validate_id(id: &str, previous: &str) -> io::Result<bool> {
        if id == "0-0" {
            return Err(invalid_data_err(
                "ERR The ID specified in XADD must be greater than 0-0",
            ));
        }
        let (milli, incr) = parse_stream_id(id)?;
        let (previous_milli, previous_incr) = parse_stream_id(previous)?;
        if milli < previous_milli || milli == previous_milli && incr <= previous_incr {
            return Err(invalid_data_err(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
            ));
        }
        Ok(true)
    }

    fn generate_id(id: &String, previous: Option<&str>) -> io::Result<String> {
        if id != "*" && !id.ends_with('*') {
            return Ok(id.clone());
        }
        match id.as_str() {
            "*" => Ok("0-1".into()),
            _ => {
                let mut parts = id.splitn(2, '-');
                let ms = parts
                    .next()
                    .ok_or_else(|| invalid_data_err("Invalid ID format"))?
                    .parse::<u64>()
                    .map_err(|_| invalid_data_err("Invalid milliseconds in stream ID"))?;
                if let Some(previous) = previous {
                    let (previous_ms, previous_incr) = parse_stream_id(previous)?;
                    if ms == previous_ms {
                        Ok(format!("{}-{}", ms, previous_incr + 1))
                    } else {
                        Ok(format!("{}-{}", ms, 0))
                    }
                } else {
                    match ms {
                        0 => Ok("0-1".into()),
                        other => Ok(format!("{}-{}", other, 0)),
                    }
                }
            }
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

fn parse_stream_id(id: &str) -> io::Result<(u64, u64)> {
    let mut parts = id.splitn(2, '-');
    let ms = parts
        .next()
        .ok_or_else(|| invalid_data_err("Invalid ID format"))?
        .parse::<u64>()
        .map_err(|_| invalid_data_err("Invalid milliseconds in stream ID"))?;

    let seq = parts
        .next()
        .ok_or_else(|| invalid_data_err("Invalid ID format"))?
        .parse::<u64>()
        .map_err(|_| invalid_data_err("Invalid sequence in stream ID"))?;

    Ok((ms, seq))
}
