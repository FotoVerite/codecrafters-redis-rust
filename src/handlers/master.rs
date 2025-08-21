use futures::{SinkExt, StreamExt};
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{self, Receiver, Sender},
        Mutex,
    },
};
use tokio_util::codec::{Framed, FramedWrite};

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
    rdb_parser::config::RdbConfig,
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
            if let Some(peer_addr) = peer_addr.clone() {
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

        if let RespCommand::Subscribe(channel_name) = command.clone() {
            subscribe(framed, store, channel_name).await?;
            break;
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
        RespCommand::BLPop(keys, timeout) => {
            list::blpop::blpop_command(&store, &keys, timeout).await?
        }

        RespCommand::Llen(key) => list::llen(store, key).await?,
        RespCommand::Lpop(key, amount) => list::lpop(store, key, amount).await?,
        RespCommand::Lpush { key, values } => list::lpush(store, key, values).await?,
        RespCommand::Rpush { key, values } => list::rpush(store, key, values).await?,
        RespCommand::Lrange { key, start, end } => list::lrange(store, key, start, end).await?,

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

async fn subscribe(
    mut framed: Framed<TcpStream, RespCodec>,
    store: Arc<Store>,
    channel_name: String,
) -> anyhow::Result<()> {
    let mut channels = vec![];
    channels.push(channel_name.clone());
    let addr = framed.get_ref().peer_addr()?;
    let (tx, mut rx) = mpsc::channel(1024);
    let response =
        subscribe_to_channel(store.clone(), channel_name, &mut channels, addr, tx.clone()).await;
    if let Err(_) = framed.send(RespValue::Array(response)).await {
        return Ok(()); // client disconnected immediately
    }
    loop {
        tokio::select! {
               Some(msg) = rx.recv() => {
                   // send pub/sub message to client
                   framed.send(msg).await?;
               },
               Some(Ok((resp_value, _bytes))) = framed.next() => {
               if let Ok(command) = command::Command::try_from_resp(resp_value) {
                   if let command::RespCommand::Subscribe(new_channel) = command {
                       channels.push(new_channel.clone());
                       let response = subscribe_to_channel(store.clone(), new_channel, &mut channels, addr, tx.clone()).await;
                       framed.send(RespValue::Array(response)).await?;
                   }
               }
           },
           else => break,
        // both streams closed
           }
    }
    Ok(())
}

async fn subscribe_to_channel(
    store: Arc<Store>,
    channel_name: String,
    channels: &mut Vec<String>,
    addr: SocketAddr,
    tx: Sender<RespValue>,
) -> Vec<RespValue> {
    store.subscribe(channel_name.clone(), addr, tx).await;
    let mut response = vec![];
    response.push(RespValue::BulkString(Some("subscribe".into())));
    response.push(RespValue::BulkString(Some(channel_name.into())));
    response.push(RespValue::Integer(channels.len() as i64));
    response
}
