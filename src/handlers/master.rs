use std::{ sync::Arc};
use futures::{SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::Mutex};
use tokio_util::codec::Framed;

use crate::{
    command::{self, RespCommand},
    handlers::{command_handlers::{config, psync, set, stream, type_command, wait, xadd, xrange}, keys, replication::handle_replconf_command},
    rdb::config::RdbConfig,
    replication_manager::manager::ReplicationManager,
    resp::{RespCodec, RespValue},
    server_info::ServerInfo,
    shared_store::shared_store::Store,
};

pub async fn handle_master_connection(
    socket: TcpStream,
    store: Arc<Store>,
    rdb: Arc<RdbConfig>,
    manager: Arc<Mutex<ReplicationManager>>,
    info: Arc<ServerInfo>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut framed = Framed::new(socket, RespCodec);
    let mut peer_addr = None;
    while let Some(result) = framed.next().await {
        let (resp_value, bytes) = result?;
        let command: command::RespCommand = command::Command::try_from_resp(resp_value)?;

        if let RespCommand::PSYNC(string, pos) = command.clone() {
            if let Some(peer_addr) = peer_addr {
                psync::psync_command(
                    framed,
                    string,
                    pos,
                    info.clone(),
                    manager.clone(),
                    peer_addr,
                )
                .await?;
                break; // End the loop for this connection
            }
        }

        let response_value = match command {
            RespCommand::Ping => Some(RespValue::SimpleString("PONG".into())),
            RespCommand::Echo(s) => Some(RespValue::BulkString(Some(s.into_bytes()))),
            RespCommand::Incr(key) => {
                store.incr(&key).await?
            }
            RespCommand::Get(key) => Some(store.get(&key).await?),
            RespCommand::Set { key, value, px } => {
                set::set_command(&store, &manager, key, &value, px, bytes).await?
            }
            RespCommand::Type(key) => type_command::type_command(&store, key).await?,
            RespCommand::ConfigCommand(command) => {
                Some(config::config_command(command, rdb.clone()))
            }
            RespCommand::Keys(string) => Some(super::keys::keys_command(string, store.clone()).await),
            RespCommand::Info(string) => Some(super::info::info_command(string, info.clone())),
            RespCommand::ReplconfCommand(command) => Some(handle_replconf_command(
                command,
                info.clone(),
                &mut peer_addr,
            )),
            RespCommand::RDB(_) => None,
            RespCommand::Wait(required_replicas, timeout_ms) => {
                wait::wait_command(&store, &manager, required_replicas, timeout_ms).await?
            }
            
            RespCommand::Xadd { key, id, fields } => {
                xadd::xadd_command(&store, key, id, fields, bytes).await?
            } // Should be handled above
            RespCommand::Xrange { key, start, end } => {
                xrange::xrange_command(&store, key, start, end).await?
            }
            RespCommand::Xread {
                count,
                block,
                keys,
                ids,
            } => stream::xread_command(&store, &block, &keys, &ids).await?,
            _ => {unimplemented!("{:?}", format!("{}", command))}
        };

        println!("Sending: {:?}", &response_value);

        if let Some(value) = response_value {
            framed.send(value).await?;
        }
    }

    Ok(())
}
