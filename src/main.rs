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

use anyhow::Result;
use tokio::{net::TcpListener, sync::Mutex};
use tokio_util::codec::Framed;

use crate::{
    error_helpers::invalid_data_err,
    handlers::{
        master::handle_master_connection,
        replication::handle_replication_connection,
        slave::{setup_heartbeat, setup_master_listener},
    },
    rdb_parser::{config::RdbConfig, length_encoded_values::LengthEncodedValue},
    replication_manager::manager::ReplicationManager,
    server_info::ServerInfo,
    shared_store::shared_store::Store,
};

#[tokio::main]
async fn main() -> Result<()> {
    println!("Logs from your program will appear here!");

    let server_info = Arc::new(ServerInfo::new()?);
    let store = Arc::new(Store::new());
    let rdb = Arc::new(RdbConfig::new());

    load_database(&rdb, &store).await?;

    match server_info.role.to_ascii_lowercase().as_str() {
        "master" => run_master(server_info, store, rdb).await?,
        "slave" => run_slave(server_info, store).await?,
        _ => {
            eprintln!("Unknown role: {}", server_info.role);
            std::process::exit(1);
        }
    }

    Ok(())
}

async fn load_database(rdb: &RdbConfig, store: &Store) -> Result<()> {
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
            Some(epoch_ms) if epoch_ms <= now_epoch_ms as u64 => continue,
            Some(epoch_ms) => Some(epoch_ms - now_epoch_ms as u64),
            None => None,
        };

        store.set(&key, value, expires_at).await;
    }
    Ok(())
}

async fn run_master(
    server_info: Arc<ServerInfo>,
    store: Arc<Store>,
    rdb: Arc<RdbConfig>,
) -> Result<()> {
    let listener =
        TcpListener::bind(format!("127.0.0.1:{}", server_info.tcp_port)).await?;
    let replication_manager = Arc::new(Mutex::new(ReplicationManager::new()));

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from {}", addr);
        let store_clone = store.clone();
        let rdb_clone = rdb.clone();
        let info_clone = server_info.clone();
        let replication_manager_clone = replication_manager.clone();

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
        });
    }
}

async fn run_slave(server_info: Arc<ServerInfo>, store: Arc<Store>) -> Result<()> {
    let listener =
        TcpListener::bind(format!("127.0.0.1:{}", server_info.tcp_port)).await?;
    println!("Slave listening on 127.0.0.1:{}", server_info.tcp_port);

    let info_clone_for_handshake = server_info.clone();
    let store_clone_for_handshake = store.clone();

    tokio::spawn(async move {
        match info_clone_for_handshake.handshake().await {
            Ok(Some((socket, _))) => {
                println!("Handshake successful, connected to master.");
                let store_for_heartbeat = store_clone_for_handshake.clone();
                let framed = Arc::new(Mutex::new(socket));
                setup_heartbeat(framed.clone(), store_for_heartbeat);
                setup_master_listener(
                    framed.clone(),
                    store_clone_for_handshake.clone(),
                    info_clone_for_handshake.clone(),
                );
            }
            Ok(None) => eprintln!("Handshake returned Ok(None) - no socket available."),
            Err(e) => eprintln!("Handshake with master failed with error: {:?}", e),
        }
    });

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from {}", addr);
        let store_clone = store.clone();
        let info_clone = server_info.clone();

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
