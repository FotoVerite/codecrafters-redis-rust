use std::{io, sync::Arc};
use futures::{SinkExt, StreamExt};
use tokio::{net::TcpStream};
use tokio_util::codec::Framed;

use crate::{
    command::{self, ReplconfCommand, RespCommand},
    error_helpers::invalid_data_err,
    handlers::info,
    resp::{RespCodec, RespValue},
    server_info::ServerInfo,
    shared_store::shared_store::Store,
};

pub async fn handle_replication_connection(
    framed: &mut Framed<TcpStream, RespCodec>,
    store: Arc<Store>,
    info: Arc<ServerInfo>,
) -> Result<(), Box<dyn std::error::Error>>{
    while let Some(result) = framed.next().await {
        let (resp_value, bytes) = result?;
        let command = command::Command::try_from_resp(resp_value)?;
        let response = match command {
            RespCommand::Set { key, value, px } => {
                store.set(&key, value, px).await;
                store.append_to_log(bytes).await;

                None
            }
            RespCommand::Get(key) => Some(store.get(&key).await?),

            RespCommand::Info(string) => Some(super::info::info_command(string, info.clone())),
            // The master might send PINGs to check the connection
            RespCommand::Ping => {
                store.append_to_log(bytes).await;
                None // Slaves don't typically respond to PINGs from the master in this context
            }
            RespCommand::ReplconfCommand(ReplconfCommand::Getack(string)) => {
                //store.append_to_log(bytes).await;
                let resp = handle_ack_command(string, store.clone()).await;
                if let Some(value) = resp {
                    framed.send(value).await?;
                    store.append_to_log(bytes).await;
                }
                None
            }
            _ => {
                None // Handle other commands from the master if necessary
            }
        };
        if let Some(value) = response {
            framed.send(value).await?;
        }
    }

    Ok(())
}

pub fn handle_replconf_command(
    command: ReplconfCommand,
    _rdb: Arc<ServerInfo>,
    peer_addr: &mut Option<String>,
) -> RespValue {
    match command {
        ReplconfCommand::ListeningPort(addr) => *peer_addr = Some(addr),
        ReplconfCommand::Ack(string) => {}
        _ => {}
    }
    RespValue::SimpleString("OK".into())
}

pub async fn handle_ack_command(string: String, store: Arc<Store>) -> Option<RespValue> {
    match string.to_ascii_lowercase().as_str() {
        "*" => {
            let mut values = vec![];
            values.push(RespValue::BulkString(Some("REPLCONF".into())));
            values.push(RespValue::BulkString(Some("ACK".into())));
            let length = store.get_offset().await;
            values.push(RespValue::BulkString(Some(length.to_string().into())));

            return Some(RespValue::Array(values));
        }
        _ => return None,
    };
}
