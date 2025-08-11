mod command;
mod error_helpers;
mod handlers;
mod heartbeat;
mod rdb_parser;
mod replication_manager;
mod resp;
mod server_info;
mod shared_store;

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use tokio::{net::TcpListener, sync::Mutex};
use tokio_util::codec::Framed;

use crate::{
    error_helpers::{invalid_data_err},
    handlers::{master::handle_master_connection, replication::handle_replication_connection},
    rdb_parser::{config::RdbConfig, length_encoded_values::LengthEncodedValue},
    replication_manager::manager::ReplicationManager,
    server_info::ServerInfo,
    shared_store::shared_store::Store,
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    let server_info: ServerInfo = ServerInfo::new()?;
    let store = Arc::new(Store::new());
    let rdb = Arc::new(RdbConfig::new());

    let peer_address = format!("127.0.0.1:{}", &server_info.tcp_port);
    //Uncomment this block to pass the first stage

    let info = Arc::new(server_info.clone());
    let replication_manager = Arc::new(Mutex::new(ReplicationManager::new()));
    {
        let database = rdb.load()?;
        for (key, (value, _value_type, px)) in database.key_values {
            let key = String::from_utf8(key).map_err(|_| invalid_data_err("Invalid Key"))?;
            let value = match value {
                LengthEncodedValue::Integer(int) => int.to_be_bytes().to_vec(),
                LengthEncodedValue::String(value) => value,
            };
            let now_epoch_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();

            let expires_at = match px {
                Some(epoch_ms) if epoch_ms <= now_epoch_ms as u64 => {
                    continue; // Already expired, skip
                }
                Some(epoch_ms) => {
                    let ttl = epoch_ms - now_epoch_ms as u64;
                    Some(ttl)
                }
                None => None,
            };

            store.set(&key, value, expires_at).await;
        }
    }
    match server_info.role.to_ascii_lowercase().as_str() {
        "master" => {
            let listener = TcpListener::bind(&peer_address).await?;

            loop {
                let (socket, addr) = listener.accept().await?;
                println!("New connection from {}", addr);
                let store_clone = store.clone();
                let rdb_clone = rdb.clone();
                let info_clone = info.clone();
                let replication_manager_clone = replication_manager.clone();
                // Spawn a new async task to handle the connection
                tokio::spawn(async move {
                    if let Err(e) = handle_master_connection(
                        socket,
                        store_clone,
                        rdb_clone,
                        replication_manager_clone,
                        info_clone,
                    )
                    .await
                    {
                        eprintln!("Error handling {}: {:?}", addr, e);
                    }
                    // Use `socket` to read/write asynchronously here
                });
            }
        }
        "slave" => {
            let listener = TcpListener::bind(&peer_address).await?;
            println!("Slave listening on {}", peer_address);
            let info_clone_for_handshake = info.clone();
            let store_clone_for_handshake = store.clone();
            tokio::spawn(async move {
                match info_clone_for_handshake.handshake().await {
                    Ok(Some((socket, _))) => {
                        println!("Handshake successful, connected to master.");
                        let store_for_heartbeat: Arc<Store> = store_clone_for_handshake.clone();

                        let framed = Arc::new(Mutex::new(socket));
                        let heartbeat_framed = framed.clone();
                        setup_heartbeat(heartbeat_framed, store_for_heartbeat);
                        let listener_framed = framed.clone();
                        let listener_store = store_clone_for_handshake.clone();
                        let listener_info = info_clone_for_handshake.clone();
                        setup_master_listener(listener_framed, listener_store, listener_info)
                    }
                    Ok(None) => {
                        eprintln!("Handshake returned Ok(None) - no socket available.");
                    }
                    Err(e) => {
                        eprintln!("Handshake with master failed with error: {:?}", e);
                    }
                }
            });

            loop {
                let (socket, addr) = listener.accept().await?;
                println!("New connection from {}", addr);
                let store_clone = store.clone();
                let info_clone = info.clone();
                // Spawn a new async task to handle the client connection
                tokio::spawn(async move {
                    let mut framed = Framed::new(socket, resp::RespCodec);

                    if let Err(e) =
                        handle_replication_connection(&mut framed, store_clone, info_clone).await
                    {
                        eprintln!("Error handling {}: {:?}", addr, e);
                    }
                });
            }
        }
        _ => {
            eprintln!("Unknown role: {}", server_info.role);
            std::process::exit(1);
        }
    }
}

type ArcFrame = Arc<Mutex<Framed<tokio::net::TcpStream, resp::RespCodec>>>;

fn setup_heartbeat(framed: ArcFrame, store: Arc<Store>) {
    tokio::spawn(async move {
        _ = heartbeat::send_heartbeat(framed, store).await;
    });
}

fn setup_master_listener(framed: ArcFrame, store: Arc<Store>, info: Arc<ServerInfo>) {
    tokio::spawn(async move {
        let mut guard = framed.lock().await;

        handle_replication_connection(&mut guard, store, info)
            .await
            .map_err(|e| invalid_data_err(format!("Replication Listener had error, {}", e)))
    });
}
