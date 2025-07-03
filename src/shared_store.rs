use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::resp::RespValue;

type SharedStore = Arc<RwLock<HashMap<String, Vec<u8>>>>;

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
            dbg!(&map);
            map.get(key).cloned()
        };
        dbg!(&value);
        RespValue::BulkString(value)
    }

    pub async fn set(&self, key: &str, value: Vec<u8>) {
        let mut map = self.data.write().await;
        map.insert(key.to_string(), value);
    }

    pub async fn del(&self, key: &str) {
        let mut map = self.data.write().await;
        map.remove(key);
    }
}
