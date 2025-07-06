use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::resp::RespValue;

#[derive(Debug, Clone)]
pub struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: Vec<u8>, expires_at: Option<Instant>) -> Self {
        Self { value, expires_at }
    }
}
type SharedStore = Arc<RwLock<HashMap<String, Entry>>>;

#[derive(Debug, Clone)]
pub struct Store {
    data: SharedStore,
}

impl Store {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get(&self, key: &str) -> crate::shared_store::RespValue {
        let value = {
            let map = self.data.read().await;
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

        RespValue::BulkString(value)
    }

    pub async fn keys(&self) -> crate::shared_store::RespValue {
        let mut values = vec![];
        let map = self.data.read().await;
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
            self.data.write().await;
        let expires_at = px.map(|ms| Instant::now() + Duration::from_millis(ms));

        let entry = Entry::new(value, expires_at);
        map.insert(key.to_string(), entry);
    }

    pub async fn del(&self, key: &str) {
        let mut map = self.data.write().await;
        map.remove(key);
    }
}
