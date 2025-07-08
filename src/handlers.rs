use std::{io, sync::Arc, time::Duration};

use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{tcp::OwnedWriteHalf, TcpStream},
    sync::Mutex,
    time::sleep,
};
use tokio_util::codec::{Framed, FramedRead};

use crate::{
    command::{self, ConfigCommand, ReplconfCommand, RespCommand},
    error_helpers::{invalid_data, invalid_data_err},
    rdb::config::RdbConfig,
    replication_manager::manager::ReplicationManager,
    resp::{self, RespCodec, RespValue},
    server_info::ServerInfo,
    shared_store::shared_store::Store,
};

pub async fn handle_replication_connection(
    framed: &mut Framed<TcpStream, RespCodec>,
    store: Arc<Store>,
    info: Arc<ServerInfo>,
) -> Result<(), Box<dyn std::error::Error>> {
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

            RespCommand::Info(string) => Some(handle_info_command(string, info.clone())),
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

pub async fn handle_master_connection(
    socket: TcpStream,
    store: Arc<Store>,
    rdb: Arc<RdbConfig>,
    manager: Arc<Mutex<ReplicationManager>>,
    info: Arc<ServerInfo>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut framed = Framed::new(socket, resp::RespCodec);
    let mut peer_addr = None;
    while let Some(result) = framed.next().await {
        let (resp_value, bytes) = result?;
        let command: command::RespCommand = command::Command::try_from_resp(resp_value)?;

        if let RespCommand::PSYNC(string, pos) = command.clone() {
            if let Some(peer_addr) = peer_addr {
                handle_psync_command(
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
            RespCommand::Get(key) => Some(store.get(&key).await?),
            RespCommand::Set { key, value, px } => {
                store.set(&key, value.clone(), px).await;
                store.append_to_log(bytes).await;

                let copied_command = RespCommand::Set {
                    key,
                    value: value.clone(),
                    px,
                };
                let guard = manager.lock().await;
                guard.send_to_replicas(copied_command).await?;
                Some(RespValue::SimpleString("OK".into()))
            }
            RespCommand::Type(key) => Some(store.get_type(&key).await?),
            RespCommand::ConfigCommand(command) => {
                Some(handle_config_command(command, rdb.clone()))
            }
            RespCommand::Keys(string) => Some(handle_keys_command(string, store.clone()).await),
            RespCommand::Info(string) => Some(handle_info_command(string, info.clone())),
            RespCommand::ReplconfCommand(command) => Some(handle_replconf_command(
                command,
                info.clone(),
                &mut peer_addr,
            )),
            RespCommand::RDB(_) => None,
            RespCommand::Wait(required_replicas, timeout_ms) => {
                let offset = store.get_offset().await;
                let mut elapsed = 0;
                let poll_interval = 250;
                let ack_command = RespCommand::ReplconfCommand(ReplconfCommand::Getack("*".into()));

                {
                    let guard = manager.lock().await;
                    guard.send_to_replicas(ack_command.clone()).await?;
                }
                loop {
                    let acked = {
                        let manager = manager.lock().await;
                        manager.replica_count(offset as u64).await?
                    };
                    if acked >= required_replicas.parse()? || elapsed >= timeout_ms.parse()? {
                        break Some(RespValue::Integer(acked as i64));
                    }

                    tokio::time::sleep(Duration::from_millis(poll_interval)).await;
                    elapsed += poll_interval;
                }
            }
            RespCommand::PSYNC(_, _) => unreachable!(),
            RespCommand::Xadd { key, id, fields } => {
                store.append_to_log(bytes).await;
                match store.xadd(&key, id.clone(), fields).await {
                    Ok(_) => Some(RespValue::SimpleString(id)),
                    Err(e) => Some(RespValue::Error(e.to_string())),
                }
            } // Should be handled above
        };

        println!("Sending: {:?}", &response_value);

        if let Some(value) = response_value {
            framed.send(value).await?;
        }
    }

    Ok(())
}

async fn handle_psync_command(
    framed: Framed<TcpStream, RespCodec>,
    _string: String,
    _pos: i64,
    info: Arc<ServerInfo>,
    manager: Arc<Mutex<ReplicationManager>>,
    peer_addr: String,
) -> io::Result<()> {
    let mut stream = framed.into_inner();
    let mut peer_address = stream.peer_addr()?;
    let first_response = format!("+FULLRESYNC {} 0\r\n", info.master_replid);

    stream.write_all(first_response.as_bytes()).await?;

    let blank_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    let rdb_bytes = hex::decode(blank_hex).unwrap();
    let header = format!("${}\r\n", rdb_bytes.len());
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(rdb_bytes.as_slice()).await?;

    stream.flush().await?;
    let (read_half, mut write_half) = stream.into_split();
    manager
        .lock()
        .await
        .add_replica(&peer_addr, peer_address, write_half)
        .await?;
    let mut framed_reader = FramedRead::new(read_half, RespCodec);
    while let Some(result) = framed_reader.next().await {
        let (resp_value, _) = result?;
        let command: command::RespCommand = command::Command::try_from_resp(resp_value)?;

        match command {
            RespCommand::Ping => {}
            RespCommand::ReplconfCommand(ReplconfCommand::Ack(offset)) => {
                let offset = offset.parse::<u64>().map_err(|_| invalid_data_err("msg"))?;
                manager
                    .lock()
                    .await
                    .update_offset(&peer_addr, offset)
                    .await?;
            }

            _ => {}
        };
    }
    Ok(())
}
fn handle_replconf_command(
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

async fn handle_ack_command(string: String, store: Arc<Store>) -> Option<RespValue> {
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
fn handle_config_command(command: ConfigCommand, rdb: Arc<RdbConfig>) -> RespValue {
    match command {
        ConfigCommand::Get(key) => {
            if let Some(resp) = rdb.get(key.as_str()) {
                let mut vec = vec![];
                vec.push(RespValue::BulkString(Some(key.into_bytes())));
                vec.push(RespValue::BulkString(Some(resp.into_bytes())));
                RespValue::Array(vec)
            } else {
                RespValue::BulkString(None)
            }
        }
        _ => RespValue::SimpleString("Ok".into()),
    }
}

fn handle_info_command(_command: String, info: Arc<ServerInfo>) -> RespValue {
    RespValue::BulkString(Some(info.info_section().into_bytes()))
}

async fn handle_keys_command(command: String, store: Arc<Store>) -> RespValue {
    match command.as_str() {
        "*" => store.keys().await,
        other => RespValue::Array(vec![]),
    }
}

pub async fn debug_peek_handshake(stream: TcpStream) -> std::io::Result<TcpStream> {
    let mut reader = BufReader::new(stream);

    // Peek into the handshake response
    let buf = reader.fill_buf().await?;
    println!(
        "[debug] Peeked handler bytes: {:02X?}",
        &buf[..buf.len().min(64)]
    );

    // Optionally consume nothing
    // reader.consume(buf.len());

    // Recover the TcpStream from BufReader
    let stream = reader.into_inner();
    Ok(stream)
}
