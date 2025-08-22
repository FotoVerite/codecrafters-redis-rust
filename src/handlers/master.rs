use futures::{SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
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
    resp::RespValue,
    server_context::ServerContext,
};

pub async fn handle_master_connection(
    socket: TcpStream,
    context: ServerContext,
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
                context.info.clone(),
                context.manager.clone(),
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
                    &context,
                )
                .await?;
            }
            ClientMode::Subscribed => {
                handle_subscribed_mode(&mut client, command, &context).await?;
            }
            ClientMode::Multi => {
                handle_multi_mode(
                    &mut client,
                    &mut session,
                    command,
                    bytes,
                    &context,
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
    context: &ServerContext,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        RespCommand::Subscribe(channel_name) => {
            client.mode = ClientMode::Subscribed;
            run_subscribed_loop(client, context, channel_name).await?;
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
                context,
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
    context: &ServerContext,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        RespCommand::Subscribe(channel_name) => {
            subscribe_to_channel(context, channel_name, client).await?;
        }
        RespCommand::Ping => {
            let response = vec![
                RespValue::BulkString(Some("pong".into())),
                RespValue::BulkString(Some("".into())),
            ];

            client.framed.send(RespValue::Array(response)).await?;
        }
        RespCommand::Unsubscribe(channel_name) => {
            // TODO: Implement unsubscribe logic
            unsubscribe_from_channel(context, channel_name, client).await?;
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
                "ERR Can't execute '{command}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
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
    context: &ServerContext,
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
                    context,
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
    context: &ServerContext,
    command: RespCommand,
    bytes: Vec<u8>,
    peer_addr: &mut Option<String>,
) -> Result<Option<RespValue>, Box<dyn std::error::Error>> {
    let response_value = match command {
        RespCommand::Ping => Some(RespValue::SimpleString("PONG".into())),
        RespCommand::Publish(channel, msg) => {
            let amount = context.store.send_to_channel(channel, msg).await?;
            Some(RespValue::Integer(amount as i64))
        }

        RespCommand::Echo(s) => Some(RespValue::BulkString(Some(s.into_bytes()))),
        RespCommand::Exec => Some(RespValue::Error("ERR EXEC without MULTI".into())),
        RespCommand::Discard => Some(RespValue::Error("ERR DISCARD without MULTI".into())),
        RespCommand::BLPop(keys, timeout) => {
            list::blpop::blpop_command(&context.store, &keys, timeout).await?
        }

        RespCommand::Llen(key) => list::llen(context.store.clone(), key).await?,
        RespCommand::Lpop(key, amount) => list::lpop(context.store.clone(), key, amount).await?,
        RespCommand::Lpush { key, values } => list::lpush(context.store.clone(), key, values).await?,
        RespCommand::Rpush { key, values } => list::rpush(context.store.clone(), key, values).await?,
        RespCommand::Lrange { key, start, end } => list::lrange(context.store.clone(), key, start, end).await?,

        RespCommand::Zadd(key, rank, value) => {
            let result = context.store.zadd(key, rank, value).await?;
            Some(RespValue::Integer(result))
        }
        RespCommand::Zcard(key) => {
            let result = context.store.zcard(key).await?;
            Some(RespValue::Integer(result))
        }
        RespCommand::Zrange(key, start, stop) => {
            let result = context.store.zrange(key, start, stop).await?;
            let mut response = vec![];
            for ret in result {
                response.push(RespValue::BulkString(Some(ret.into())))
            }
            Some(RespValue::Array(response))
        }
        RespCommand::ZScore(key, value) => {
            if let Some(result) = context.store.zscore(key, value).await? {
                let string_msg = result.to_string();
                Some(RespValue::BulkString(Some(string_msg.into())))
            } else {
                Some(RespValue::BulkString(None))
            }
        }
        RespCommand::Zrank(key, value) => {
            let result = context.store.zrank_command(key, value).await?;
            if let Some(result) = result {
                Some(RespValue::Integer(result as i64))
            } else {
                Some(RespValue::BulkString(None))
            }
        }
         RespCommand::ZRem(key, value) => {
            let result = context.store.zrem(key, value).await?;
            if let Some(result) = result {
                Some(RespValue::Integer(result))
            } else {
                Some(RespValue::Integer(0))
            }
        }

        RespCommand::Multi => Some(RespValue::Error("ERR MULTI calls can not be nested".into())),
        RespCommand::Incr(key) => context.store.incr(&key).await?,
        RespCommand::Get(key) => Some(context.store.get(&key).await?),
        RespCommand::Set { key, value, px } => {
            set::set_command(&context.store, &context.manager, key, &value, px, bytes).await?
        }

        RespCommand::Type(key) => type_command::type_command(&context.store, key).await?,
        RespCommand::ConfigCommand(command) => Some(config::config_command(command, context.rdb.clone())),
        RespCommand::Keys(string) => Some(super::keys::keys_command(string, context.store.clone()).await),
        RespCommand::Info(string) => Some(super::info::info_command(string, context.info.clone())),
        RespCommand::ReplconfCommand(command) => {
            let mut p_addr = peer_addr.clone();
            let ret = handle_replconf_command(command, context.info.clone(), &mut p_addr);
            *peer_addr = p_addr;
            Some(ret)
        }
        RespCommand::RDB(_) => None,
        RespCommand::Wait(required_replicas, timeout_ms) => {
            wait::wait_command(&context.store, &context.manager, required_replicas, timeout_ms).await?
        }

        RespCommand::Xadd { key, id, fields } => {
            xadd::xadd_command(&context.store, key, id, fields, bytes).await?
        } // Should be handled above
        RespCommand::Xrange { key, start, end } => {
            xrange::xrange_command(&context.store, key, start, end).await?
        }
        RespCommand::Xread {
            count: _,
            block,
            keys,
            ids,
        } => stream::xread_command(&context.store, &block, &keys, &ids).await?,
        _ => {
            unimplemented!("{:?}", format!("{}", command))
        }
    };

    Ok(response_value)
}

async fn run_subscribed_loop(
    client: &mut Client,
    context: &ServerContext,
    channel_name: String,
) -> anyhow::Result<()> {
    _ = subscribe_to_channel(context, channel_name, client).await;

    loop {
        tokio::select! {
               Some(msg) = client.rx.recv() => {
                   // send pub/sub message to client
                   client.framed.send(msg).await?;
               },
               Some(Ok((resp_value, _bytes))) = client.framed.next() => {
                        let command: command::RespCommand = command::Command::try_from_resp(resp_value)?;

              _ = handle_subscribed_mode(client,  command, context).await;
           },
           else => break,
        // both streams closed
           }
    }
    Ok(())
}

async fn subscribe_to_channel(
    context: &ServerContext,
    channel_name: String,
    client: &mut Client,
) -> anyhow::Result<()> {
    context.store
        .subscribe(channel_name.clone(), client.addr, client.tx.clone())
        .await;
    client.channels.push(channel_name.clone());
    let response = vec![
        RespValue::BulkString(Some("subscribe".into())),
        RespValue::BulkString(Some(channel_name.into())),
        RespValue::Integer(client.channels.len() as i64),
    ];
    if (client.framed.send(RespValue::Array(response)).await).is_err() {
        return Ok(()); // client disconnected immediately
    }
    Ok(())
}

async fn unsubscribe_from_channel(
    context: &ServerContext,
    channel_name: String,
    client: &mut Client,
) -> anyhow::Result<()> {
    _ = context.store.unsubscribe(channel_name.clone(), client.addr).await;
    client.channels.pop();
    let response = vec![
        RespValue::BulkString(Some("unsubscribe".into())),
        RespValue::BulkString(Some(channel_name.into())),
        RespValue::Integer(client.channels.len() as i64),
    ];
    if (client.framed.send(RespValue::Array(response)).await).is_err() {
        return Ok(()); // client disconnected immediately
    }
    Ok(())
}
