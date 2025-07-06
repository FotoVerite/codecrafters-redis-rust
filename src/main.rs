mod command;
mod error_helpers;
mod rdb;
mod replication_manager;
mod resp;
mod server_info;
mod shared_store;

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio::{
    io::{self, AsyncWriteExt},
    net::{unix::SocketAddr, TcpListener, TcpStream},
    sync::Mutex,
};
use tokio_util::codec::Framed;

use crate::{
    command::{ConfigCommand, ReplconfCommand, RespCommand},
    rdb::config::RdbConfig,
    replication_manager::manager::ReplicationManager,
    resp::{RespCodec, RespValue},
    server_info::ServerInfo,
    shared_store::Store,
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    let server_info: ServerInfo = ServerInfo::new()?;
    server_info.handshake().await?;
    let peer_address = format!("127.0.0.1:{}", server_info.tcp_port);
    //Uncomment this block to pass the first stage

    let listener = TcpListener::bind(&peer_address).await?;
    let store = Arc::new(Store::new());
    let rdb = Arc::new(RdbConfig::new());
    let info = Arc::new(server_info);
    let replication_manager = Arc::new(Mutex::new(ReplicationManager::new()));
    {
        let database = rdb.load()?;
        for (key, value, px) in database {
            store.set(&key, value, px).await;
        }
    }
    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from {}", addr);
        let store_clone = store.clone();
        let rdb_clone = rdb.clone();
        let info_clone = info.clone();
        let replication_manager_clone = replication_manager.clone();
        let pee_addr_clone = peer_address.clone();
        // Spawn a new async task to handle the connection
        tokio::spawn(async move {
            if let Err(e) = handle_connection(
                socket,
                store_clone,
                rdb_clone,
                replication_manager_clone,
                info_clone,
                pee_addr_clone,
            )
            .await
            {
                eprintln!("Error handling {}: {:?}", addr, e);
            }
            // Use `socket` to read/write asynchronously here
        });
    }

    async fn handle_connection(
        socket: TcpStream,
        store: Arc<Store>,
        rdb: Arc<RdbConfig>,
        manager: Arc<Mutex<ReplicationManager>>,
        info: Arc<ServerInfo>,
        peer_addr: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut framed = Framed::new(socket, resp::RespCodec);

        while let Some(result) = framed.next().await {
            let resp_value = result?;
            let command: command::RespCommand = command::Command::try_from_resp(resp_value)?;
            println!("Received: {:?}", &command);

            if let RespCommand::PSYNC(string, pos) = command {
                handle_psync_command(
                    framed,
                    string,
                    pos,
                    info.clone(),
                    manager.clone(),
                    peer_addr.clone(),
                )
                .await?;
                break; // End the loop for this connection
            }

            let response_value = match command {
                RespCommand::Ping => Some(RespValue::SimpleString("PONG".into())),
                RespCommand::Echo(s) => Some(RespValue::BulkString(Some(s.into_bytes()))),
                RespCommand::Get(key) => Some(store.get(&key).await),
                RespCommand::Set { key, value, px } => {
                    store.set(&key, value.clone(), px).await;
                    let copied_command = RespCommand::Set {
                        key,
                        value: value.clone(),
                        px,
                    };
                    let guard = manager.lock().await;
                    guard.send_to_replicas(copied_command).await?;
                    Some(RespValue::SimpleString("OK".into()))
                }
                RespCommand::ConfigCommand(command) => {
                    Some(handle_config_command(command, rdb.clone()))
                }
                RespCommand::Keys(string) => Some(handle_keys_command(string, store.clone()).await),
                RespCommand::Info(string) => Some(handle_info_command(string, info.clone())),
                RespCommand::ReplconfCommand(command) => {
                    Some(handle_replconf_command(command, info.clone()))
                }
                RespCommand::PSYNC(_, _) => unreachable!(), // Should be handled above
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

        let first_response = format!("+FULLRESYNC {} 0\r\n", info.master_replid);

        stream.write_all(first_response.as_bytes()).await?;

        let blank_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        let rdb_bytes = hex::decode(blank_hex).unwrap();
        let header = format!("${}\r\n", rdb_bytes.len());
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(rdb_bytes.as_slice()).await?;
        stream.flush().await?;
        manager
            .lock()
            .await
            .add_replica(peer_addr, stream.peer_addr()?, stream)
            .await?;

        Ok(())
    }
    fn handle_replconf_command(_command: ReplconfCommand, _rdb: Arc<ServerInfo>) -> RespValue {
        RespValue::SimpleString("OK".into())
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
}
