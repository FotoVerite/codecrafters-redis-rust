use std::{collections::HashMap, net::SocketAddr};

use tokio::sync::mpsc::Sender;

use crate::{resp::RespValue, shared_store::shared_store::{Entry, RedisValue, Store}};

#[derive(Debug, Clone)]
pub struct Channel {
    name: String,
    pub clients: HashMap<SocketAddr, Sender<RespValue>>,
}

impl Channel {
    pub fn new(name: String) -> Self {
        Self {
            name,
            clients: HashMap::new(),
        }
    }
}

impl Store {
    pub async fn subscribe(
        &self,
        channel_name: String,
        client: SocketAddr,
        tx: Sender<RespValue>,
    ) {
        let channel_name = format!("channel-{channel_name}");
        let mut keyspace = self.keyspace.write().await;
        if let Some(entry) = keyspace.get_mut(&channel_name) {
            match &mut entry.value {
                RedisValue::Channel(channel) => {
                    channel.clients.insert(client, tx);
                }
                _ => {}
            }
        } else {
            let mut channel = Channel::new(channel_name.clone());
            channel.clients.insert(client, tx);
            let entry = Entry::new(RedisValue::Channel(channel), None);
            keyspace.insert(channel_name, entry);
        }
    }
}
