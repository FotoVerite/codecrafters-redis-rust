use futures::{channel, SinkExt, StreamExt};
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{self, Sender},
        Mutex,
    },
};

use crate::{
    command::{self, RespCommand},
    handlers::{
        client::{Client, ClientMode},
        command_handlers::{
            config,
            list::{self},
            psync, set, stream, type_command, wait, xadd, xrange,
        },
        replication::handle_replconf_command,
        session::Session,
    },
    rdb_parser::config::RdbConfig,
    replication_manager::manager::ReplicationManager,
    resp::RespValue,
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
    let mut client = Client::new(socket);
    let mut session = Session::new();

    while let Some(result) = client.framed.next().await {
        let (resp_value, bytes) = result?;
        let command: command::RespCommand = command::Command::try_from_resp(resp_value)?;

        if let RespCommand::PSYNC(string, pos) = command.clone() {
            psync::psync_command(
                client.framed,
                string,
                pos,
                info.clone(),
                manager.clone(),
                client.addr.to_string(),
            )
            .await?;
            break; // End the loop for this connection
        }

        match client.mode {
            ClientMode::Normal => {
                handle_normal_mode(
                    &mut client,
                    &mut session,
                    command,
                    bytes,
                    store.clone(),
                    rdb.clone(),
                    manager.clone(),
                    info.clone(),
                )
                .await?;
            }
            ClientMode::Subscribed => {
                handle_subscribed_mode(&mut client, command, store.clone()).await?;
            }
            ClientMode::Multi => {
                handle_multi_mode(
                    &mut client,
                    &mut session,
                    command,
                    bytes,
                    store.clone(),
                    rdb.clone(),
                    manager.clone(),
                    info.clone(),
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn handle_normal_mode(
    client: &mut Client,
    _session: &mut Session,
    command: RespCommand,
    bytes: Vec<u8>,
    store: Arc<Store>,
    rdb: Arc<RdbConfig>,
    manager: Arc<Mutex<ReplicationManager>>,
    info: Arc<ServerInfo>,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        RespCommand::Subscribe(channel_name) => {
            client.mode = ClientMode::Subscribed;
            run_subscribed_loop(client, store, channel_name).await?;
            return Ok(()); // Break the loop after subscribe
        }
        RespCommand::Multi => {
            client.mode = ClientMode::Multi;
            client
                .framed
                .send(RespValue::SimpleString("OK".into()))
                .await?;
        }
        _ => {
            let response = process_command(
                store,
                rdb,
                manager,
                info,
                command,
                bytes,
                &mut Some(client.addr.to_string()),
            )
            .await?;
            if let Some(response) = response {
                client.framed.send(response).await?;
            }
        }
    }
    Ok(())
}

async fn handle_subscribed_mode(
    client: &mut Client,
    command: RespCommand,
    store: Arc<Store>,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        RespCommand::Subscribe(channel_name) => {
            subscribe_to_channel(store, channel_name, client).await?;
        }
        RespCommand::Ping => {
            let mut response = vec![];
            response.push(RespValue::BulkString(Some("pong".into())));
            response.push(RespValue::BulkString(Some("".into())));

            client.framed.send(RespValue::Array(response)).await?;
        }
        RespCommand::Unsubscribe => {
            // TODO: Implement unsubscribe logic
            client
                .framed
                .send(RespValue::SimpleString("OK".into()))
                .await?;
        }
        RespCommand::PSubscribe => {
            // TODO: Implement psubscribe logic
            client
                .framed
                .send(RespValue::SimpleString("OK".into()))
                .await?;
        }
        RespCommand::PunSubscribe => {
            // TODO: Implement punsubscribe logic
            client
                .framed
                .send(RespValue::SimpleString("OK".into()))
                .await?;
        }
        RespCommand::Quit => {
            // TODO: Implement quit logic
            client
                .framed
                .send(RespValue::SimpleString("OK".into()))
                .await?;
        }
        // RespCommand::Reset => {
        //     // TODO: Implement reset logic
        //     client.framed.send(RespValue::SimpleString("OK".into())).await?;
        // }
        _ => {
            let error_message = format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                command
            );
            client.framed.send(RespValue::Error(error_message)).await?;
        }
    }
    Ok(())
}

async fn handle_multi_mode(
    client: &mut Client,
    session: &mut Session,
    command: RespCommand,
    bytes: Vec<u8>,
    store: Arc<Store>,
    rdb: Arc<RdbConfig>,
    manager: Arc<Mutex<ReplicationManager>>,
    info: Arc<ServerInfo>,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        RespCommand::Exec => {
            client.mode = ClientMode::Normal;
            if session.queued.is_empty() {
                client.framed.send(RespValue::Array(vec![])).await?;
                return Ok(());
            }
            let mut responses = Vec::new();
            let queue = &session.queued.clone();
            for (queued_command, bytes) in queue {
                let response = process_command(
                    store.clone(),
                    rdb.clone(),
                    manager.clone(),
                    info.clone(),
                    queued_command.clone(),
                    bytes.clone(),
                    &mut Some(client.addr.to_string()),
                )
                .await?;

                if let Some(resp) = response {
                    responses.push(resp);
                }
            }

            client.framed.send(RespValue::Array(responses)).await?;
            session.queued.clear();
        }
        RespCommand::Discard => {
            client.mode = ClientMode::Normal;
            client
                .framed
                .send(RespValue::SimpleString("OK".into()))
                .await?;
            session.queued.clear();
        }
        _ => {
            session.queued.push((command, bytes));
            client
                .framed
                .send(RespValue::BulkString(Some("QUEUED".into())))
                .await?;
        }
    }
    Ok(())
}

async fn process_command(
    store: Arc<Store>,
    rdb: Arc<RdbConfig>,
    manager: Arc<Mutex<ReplicationManager>>,
    info: Arc<ServerInfo>,
    command: RespCommand,
    bytes: Vec<u8>,
    peer_addr: &mut Option<String>,
) -> Result<Option<RespValue>, Box<dyn std::error::Error>> {
    let response_value = match command {
        RespCommand::Ping => Some(RespValue::SimpleString("PONG".into())),
        RespCommand::Publish(channel, msg) => {
            let amount =store.send_to_channel(channel, msg).await?;
            Some(RespValue::Integer(amount as i64))
        },

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

        RespCommand::Multi => Some(RespValue::Error("ERR MULTI calls can not be nested".into())),
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
            let mut p_addr = peer_addr.clone();
            let ret = handle_replconf_command(command, info.clone(), &mut p_addr);
            *peer_addr = p_addr;
            Some(ret)
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

async fn run_subscribed_loop(
    client: &mut Client,
    store: Arc<Store>,
    channel_name: String,
) -> anyhow::Result<()> {
    _ = subscribe_to_channel(Arc::clone(&store), channel_name, client).await;

    loop {
        tokio::select! {
               Some(msg) = client.rx.recv() => {
                   // send pub/sub message to client
                   client.framed.send(msg).await?;
               },
               Some(Ok((resp_value, _bytes))) = client.framed.next() => {
                        let command: command::RespCommand = command::Command::try_from_resp(resp_value)?;

              _ = handle_subscribed_mode(client,  command, Arc::clone(&store)).await;
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
    client: &mut Client,
) -> anyhow::Result<()> {
    store
        .subscribe(channel_name.clone(), client.addr, client.tx.clone())
        .await;
    client.channels.push(channel_name.clone());
    let mut response = vec![];
    response.push(RespValue::BulkString(Some("subscribe".into())));
    response.push(RespValue::BulkString(Some(channel_name.into())));
    response.push(RespValue::Integer(client.channels.len() as i64));
    if let Err(_) = client.framed.send(RespValue::Array(response)).await {
        return Ok(()); // client disconnected immediately
    }
    Ok(())
}
