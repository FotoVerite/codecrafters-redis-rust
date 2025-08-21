use futures::io;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::time::Instant;

use crate::error_helpers::{invalid_data, invalid_data_err};
use crate::resp::RespValue;
use crate::shared_store::channel::Channel;
use crate::shared_store::redis_list::List;
use crate::shared_store::redis_stream::{Stream, StreamEntries};
use crate::shared_store::stream_id::StreamID;

#[derive(Debug, Clone)]
pub enum RedisValue {
    Text(Vec<u8>),
    Stream(Stream),
    List(List),
    Channel(Channel),
    #[allow(dead_code)]
    Queue(VecDeque<Vec<u8>>), // Add ZSet, List, etc. as needed
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub(crate) value: RedisValue,
    expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: RedisValue, expires_at: Option<Instant>) -> Self {
        Self { value, expires_at }
    }
}
type SharedStore = Arc<RwLock<HashMap<String, Entry>>>;
type Log = Arc<RwLock<Vec<u8>>>;
pub type NotifierStore = Mutex<HashMap<String, Arc<Notify>>>;
#[derive(Debug)]
pub struct Store {
    pub(crate) keyspace: SharedStore,
    notifiers: NotifierStore,
    log: Log,
}

impl Store {
    pub fn new() -> Self {
        Self {
            keyspace: Arc::new(RwLock::new(HashMap::new())),
            notifiers: Mutex::new(HashMap::new()),
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
                RedisValue::Channel(_) => Ok(RespValue::SimpleString("channel".into())),
                RedisValue::List(_) => Ok(RespValue::SimpleString("list".into())),
                RedisValue::Stream(_) => Ok(RespValue::SimpleString("stream".into())),
                RedisValue::Text(_) => Ok(RespValue::SimpleString("string".into())),
                RedisValue::Queue(_) => Ok(RespValue::SimpleString("queue".into())),
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

    async fn _get_mut(&self, key: &str) -> io::Result<Option<RedisValue>> {
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

    pub async fn incr(&self, key: &String) -> io::Result<Option<RespValue>> {
        let mut map = self.keyspace.write().await;
        let error = Ok(Some(RespValue::Error(
            "ERR value is not an integer or out of range".into(),
        )));
        if let Some(previous) = map.get_mut(key) {
            match &previous.value {
                RedisValue::Text(value) => {
                    let copy = value.clone();
                    let string_value = match String::from_utf8(copy) {
                        Ok(s) => s,
                        Err(_) => return error,
                    };
                    let mut number = match string_value.parse::<i64>() {
                        Ok(i) => i,
                        Err(_) => return error,
                    };
                    number += 1;
                    let new_value = (number).to_string().into_bytes();
                    previous.value = RedisValue::Text(new_value);
                    Ok(Some(RespValue::Integer(number)))
                }

                _ => error,
            }
        } else {
            let entry = Entry {
                value: RedisValue::Text("1".as_bytes().into()),
                expires_at: None,
            };
            map.insert(key.clone(), entry);
            Ok(Some(RespValue::Integer(1)))
        }
    }
    pub async fn set(&self, key: &str, value: Vec<u8>, px: Option<u64>) {
        let mut map = self.keyspace.write().await;
        let expires_at = px.map(|ms| Instant::now() + Duration::from_millis(ms));
        let entry = Entry::new(RedisValue::Text(value), expires_at);
        map.insert(key.to_string(), entry);
    }

    pub async fn rpush(&self, key: String, values: Vec<Vec<u8>>) -> io::Result<usize> {
        let mut map = self.keyspace.write().await;
        let len = values.len();
        match map.get_mut(&key) {
            Some(entry) => match &mut entry.value {
                RedisValue::List(list) => Ok(list.rpush(values)?),
                _ => Err(invalid_data_err(
                    "ERR RPUSH on key holding the wrong kind of value",
                )),
            },
            None => {
                let mut guard = self.notifiers.lock().await;
                let notify = guard.entry(key.clone()).or_insert(Arc::new(Notify::new()));
                let list = List::new(notify.clone(), values);
                let entry = Entry::new(RedisValue::List(list), None);
                map.insert(key.clone(), entry);
                notify.notify_waiters();

                Ok(len)
            }
        }
    }

    pub async fn lpop(&self, key: String, amount: usize) -> io::Result<Option<Vec<Vec<u8>>>> {
        let mut map = self.keyspace.write().await;
        match map.get_mut(&key) {
            Some(entry) => match &mut entry.value {
                RedisValue::List(list) => Ok(list.lpop(amount)?),
                _ => Err(invalid_data_err(
                    "ERR LPOP on key holding the wrong kind of value",
                )),
            },
            None => Ok(None),
        }
    }

    pub async fn lpush(&self, key: String, mut values: Vec<Vec<u8>>) -> io::Result<usize> {
        let mut map = self.keyspace.write().await;
        let len = values.len();
        match map.get_mut(&key) {
            Some(entry) => match &mut entry.value {
                RedisValue::List(list) => Ok(list.lpush(values)?),

                _ => Err(invalid_data_err(
                    "ERR LPUSH on key holding the wrong kind of value",
                )),
            },
            None => {
                let mut guard = self.notifiers.lock().await;
                let notify = guard.entry(key.clone()).or_insert(Arc::new(Notify::new()));
                let list = List::new(notify.clone(), values);
                let entry = Entry::new(RedisValue::List(list), None);
                map.insert(key.clone(), entry);
                notify.notify_waiters();

                Ok(len)
            }
        }
    }

    pub async fn llen(&self, key: String) -> io::Result<usize> {
        let map = self.keyspace.read().await;
        match map.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::List(arr) => {
                    let len = arr.entries.len();
                    return Ok(len);
                }
                _ => Err(invalid_data_err(
                    "ERR LPUSH on key holding the wrong kind of value",
                )),
            },
            None => Ok(0),
        }
    }

    pub async fn lrange(
        &self,
        key: String,
        mut start: isize,
        mut end: isize,
    ) -> io::Result<Vec<Vec<u8>>> {
        let map = self.keyspace.read().await;
        match map.get(&key) {
            Some(entry) => match &entry.value {
                RedisValue::List(arr) => {
                    let len = arr.entries.len() as isize;
                    if start < 0 {
                        start += len;
                    }
                    if end < 0 {
                        end += len;
                    }

                    // Clamp to bounds
                    start = start.max(0);
                    end = end.min(len - 1);
                    if start > end || start >= len {
                        return Ok(vec![]);
                    }

                    let u_start = start as usize;
                    let u_end = (end + 1) as usize;
                    return Ok(arr.entries[u_start..u_end].to_vec());
                }
                _ => Err(invalid_data_err(
                    "ERR LPUSH on key holding the wrong kind of value",
                )),
            },
            None => Ok(vec![]),
        }
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

    pub async fn xadd(
        &self,
        key: &str,
        id: String,
        fields: Vec<(String, String)>,
    ) -> io::Result<String> {
        let mut map = self.keyspace.write().await;

        if let Some(entry) = map.get_mut(key) {
            match &mut entry.value {
                RedisValue::Stream(stream) => {
                    let stream_id: StreamID =
                        StreamID::from_redis_input(Some(*stream.previous_id()), id)?;
                    stream.append(stream_id.clone(), fields)?;
                    Ok(stream_id.to_string())
                }
                _ => {
                    return Err(invalid_data_err(
                        "ERR calling None Stream Value with stream key",
                    ));
                }
            }
        } else {
            let stream_id: StreamID = StreamID::from_redis_input(None, id)?;
            let mut guard = self.notifiers.lock().await;
            let notify = guard
                .entry(key.to_string())
                .or_insert(Arc::new(Notify::new()));
            let mut stream = Stream::new(notify.clone());
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

    pub async fn get_notifiers(&self, keys: &[String]) -> Vec<Arc<Notify>> {
        let mut ret = Vec::with_capacity(keys.len());
        let mut guard = self.notifiers.lock().await;
        for key in keys {
            let notify = guard.entry(key.clone()).or_insert(Arc::new(Notify::new()));
            ret.push(notify.clone());
        }
        ret
    }
}
