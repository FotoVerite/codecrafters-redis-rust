use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::{net::TcpStream, sync::Mutex};
use tokio_util::codec::Framed;

use crate::{
    command::{self, RespCommand},
    handlers::{
        command_handlers::{
            config,
            list::{self, rpush},
            psync, set, stream, type_command, wait, xadd, xrange,
        },
        replication::handle_replconf_command,
        session::Session,
    },
    rdb::config::RdbConfig,
    replication_manager::manager::ReplicationManager,
    resp::{RespCodec, RespValue},
    server_info::ServerInfo,
    shared_store::shared_store::Store,
};

pub struct _CommandContext {
    pub store: Arc<Store>,
    pub rdb: Arc<RdbConfig>,
    pub manager: Arc<Mutex<ReplicationManager>>,
    pub info: Arc<ServerInfo>,
    pub session: Session,
}

pub async fn handle_master_connection(
    socket: TcpStream,
    store: Arc<Store>,
    rdb: Arc<RdbConfig>,
    manager: Arc<Mutex<ReplicationManager>>,
    info: Arc<ServerInfo>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut session = Session::new();
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
        if session.in_multi {
            match command {
                RespCommand::Exec => {
                    session.in_multi = false;
                    if session.queued.is_empty() {
                        framed.send(RespValue::Array(vec![])).await?;
                        continue;
                    }
                    let mut responses = Vec::new();
                    let queue = &session.queued.clone();
                    for (queued_command, bytes) in queue {
                        let response = process_command(
                            store.clone(),
                            rdb.clone(),
                            manager.clone(),
                            info.clone(),
                            &mut session,
                            queued_command.clone(),
                            bytes.clone(),
                            &mut peer_addr,
                        )
                        .await?;

                        if let Some(resp) = response {
                            responses.push(resp);
                        } else {
                        }
                    }

                    framed.send(RespValue::Array(responses)).await?;
                    session.queued.clear();
                }
                RespCommand::Discard => {
                    framed.send(RespValue::SimpleString("OK".into())).await?;

                    session.queued.clear();
                    session.in_multi = false;
                }
                _ => {
                    session.queued.push((command, bytes));
                    framed
                        .send(RespValue::BulkString(Some("QUEUED".into())))
                        .await?;
                }
            }
            continue;
        }
        let response_value = process_command(
            store.clone(),
            rdb.clone(),
            manager.clone(),
            info.clone(),
            &mut session,
            command,
            bytes,
            &mut peer_addr,
        )
        .await?;

        println!("Sending: {:?}", &response_value);

        if let Some(value) = response_value {
            framed.send(value).await?;
        }
    }

    Ok(())
}

async fn process_command(
    store: Arc<Store>,
    rdb: Arc<RdbConfig>,
    manager: Arc<Mutex<ReplicationManager>>,
    info: Arc<ServerInfo>,
    session: &mut Session,
    command: RespCommand,
    bytes: Vec<u8>,
    peer_addr: &mut Option<String>,
) -> Result<Option<RespValue>, Box<dyn std::error::Error>> {
    let response_value = match command {
        RespCommand::Ping => Some(RespValue::SimpleString("PONG".into())),
        RespCommand::Echo(s) => Some(RespValue::BulkString(Some(s.into_bytes()))),
        RespCommand::Exec => Some(RespValue::Error("ERR EXEC without MULTI".into())),
        RespCommand::Discard => Some(RespValue::Error("ERR DISCARD without MULTI".into())),
        RespCommand::Rpush { key, values } => list::rpush(store, key, values).await?,
        RespCommand::Lpush { key, start , end} => list::lrange(store, key, start , end).await?,

        RespCommand::Multi => {
            session.in_multi = true;
            Some(RespValue::SimpleString("OK".into()))
        }
        RespCommand::Incr(key) => store.incr(&key).await?,
        RespCommand::Get(key) => Some(store.get(&key).await?),
        RespCommand::Set { key, value, px } => {
            set::set_command(&store, &manager, key, &value, px, bytes).await?
        }
        RespCommand::Type(key) => type_command::type_command(&store, key).await?,
        RespCommand::ConfigCommand(command) => Some(config::config_command(command, rdb.clone())),
        RespCommand::Keys(string) => Some(super::keys::keys_command(string, store.clone()).await),
        RespCommand::Info(string) => Some(super::info::info_command(string, info.clone())),
        RespCommand::ReplconfCommand(command) => {
            Some(handle_replconf_command(command, info.clone(), peer_addr))
        }
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
            count: _,
            block,
            keys,
            ids,
        } => stream::xread_command(&store, &block, &keys, &ids).await?,
        _ => {
            unimplemented!("{:?}", format!("{}", command))
        }
    };

    Ok(response_value)
}
