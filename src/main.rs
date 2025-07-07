mod command;
mod error_helpers;
mod handlers;
mod rdb;
mod replication_manager;
mod resp;
mod server_info;
mod shared_store;

use std::sync::Arc;

use tokio::{net::TcpListener, sync::Mutex};

use crate::{
    rdb::config::RdbConfig, replication_manager::manager::ReplicationManager,
    server_info::ServerInfo, shared_store::Store,
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
        for (key, value, px) in database {
            store.set(&key, value, px).await;
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
                    if let Err(e) = handlers::handle_master_connection(
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
            let rdb_clone_for_handshake = rdb.clone();
            let replication_manager_clone_for_handshake = replication_manager.clone();

            tokio::spawn(async move {
                if let Ok(Some(socket)) = info_clone_for_handshake.handshake().await {
                    println!("Handshake successful, connected to master.");
                    if let Err(e) = handlers::handle_replication_connection(
                        socket,
                        store_clone_for_handshake,
                        info_clone_for_handshake,
                    )
                    .await
                    {
                        eprintln!("Error in connection with master: {:?}", e);
                    }
                } else {
                    eprintln!("Handshake with master failed.");
                }
            });

            loop {
                let (socket, addr) = listener.accept().await?;
                println!("New connection from {}", addr);
                let store_clone = store.clone();
                let rdb_clone = rdb.clone();
                let info_clone = info.clone();
                let replication_manager_clone = replication_manager.clone();
                // Spawn a new async task to handle the client connection
                tokio::spawn(async move {
                    if let Err(e) = handlers::handle_replication_connection(
                        socket,
                        store_clone,
                        info_clone,
                    )
                    .await
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
    Ok(())
}
