use std::{collections::HashMap, net::SocketAddr};

use tokio::sync::mpsc::Sender;

use crate::{
    handlers::client,
    resp::RespValue,
    shared_store::shared_store::{Entry, RedisValue, Store},
};

#[derive(Debug, Clone)]
pub struct Channel {
    #[allow(dead_code)]
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
    pub async fn subscribe(&self, channel_name: String, client: SocketAddr, tx: Sender<RespValue>) {
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

    pub async fn send_to_channel(
        &self,
        channel_name: String,
        msg: String,
    ) -> anyhow::Result<usize> {
        let called_name = channel_name.clone();
        let channel_name = format!("channel-{channel_name}");
        let mut keyspace = self.keyspace.write().await;
        if let Some(entry) = keyspace.get_mut(&channel_name) {
            match &mut entry.value {
                RedisValue::Channel(channel) => {
                    let size = channel.clients.len();
                    for (_, tx) in &channel.clients {
                        let mut response = vec![];
                        response.push(RespValue::BulkString(Some("message".into())));
                        response.push(RespValue::BulkString(Some(called_name.clone().into())));
                        response.push(RespValue::BulkString(Some(msg.clone().into())));
                        tx.send(RespValue::Array(response)).await?;
                    }
                    Ok(size)
                }
                _ => Ok(0),
            }
        } else {
            Ok(0)
        }
    }

    pub async fn unsubscribe(
        &self,
        channel_name: String,
        addr: SocketAddr
    ) -> anyhow::Result<()> {
        let channel_name = format!("channel-{channel_name}");
        let mut keyspace = self.keyspace.write().await;
        if let Some(entry) = keyspace.get_mut(&channel_name) {
            match &mut entry.value {
                RedisValue::Channel(channel) => {
                    channel.clients.remove(&addr);
                    Ok(())
                }
                _ => Ok(()),
            }
        } else {
            Ok(())
        }
    }
}
