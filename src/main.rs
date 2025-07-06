mod command;
mod error_helpers;
mod rdb;
mod resp;
mod server_info;
mod shared_store;

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio::{
    io::{self, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::codec::Framed;

use crate::{
    command::{ConfigCommand, ReplconfCommand, RespCommand},
    rdb::config::RdbConfig,
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
    let bind = format!("127.0.0.1:{}", server_info.tcp_port);
    //Uncomment this block to pass the first stage

    let listener = TcpListener::bind(bind).await?;
    let store = Arc::new(Store::new());
    let rdb = Arc::new(RdbConfig::new());
    let info = Arc::new(server_info);
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
        // Spawn a new async task to handle the connection
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, store_clone, rdb_clone, info_clone).await {
                eprintln!("Error handling {}: {:?}", addr, e);
            }
            // Use `socket` to read/write asynchronously here
        });
    }

    async fn handle_connection(
        socket: TcpStream,
        store: Arc<Store>,
        rdb: Arc<RdbConfig>,
        info: Arc<ServerInfo>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut framed = Framed::new(socket, resp::RespCodec);

        while let Some(result) = framed.next().await {
            let resp_value = result?;
            let command: command::RespCommand = command::Command::try_from_resp(resp_value)?;
            println!("Received: {:?}", &command);

            let response_value: Option<RespValue> = match command {
                RespCommand::Ping => Some(RespValue::SimpleString("PONG".into())),
                RespCommand::Echo(s) => Some(RespValue::BulkString(Some(s.into_bytes()))),
                RespCommand::Get(key) => Some(store.get(&key).await),
                RespCommand::Set { key, value, px } => {
                    store.set(&key, value, px).await;
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
                RespCommand::PSYNC(string, pos) => {
                    handle_psync_command(&mut framed, string, pos, info.clone()).await?
                }
            };

            println!("Sending: {:?}", &response_value);

            if let Some(value) = response_value {
                framed.send(value).await?;
            }
        }

        Ok(())
    }

    async fn handle_psync_command(
        framed: &mut Framed<TcpStream, RespCodec>,
        _string: String,
        _pos: i64,
        info: Arc<ServerInfo>,
    ) -> io::Result<Option<RespValue>> {
        let first_response =
            RespValue::SimpleString(format!("FULLRESYNC {} 0", info.master_replid).into());
        framed.send(first_response).await?;

        let blank_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        let rdb_bytes = hex::decode(blank_hex).unwrap();

        let stream = framed.get_mut();
        let header = format!("${}\r\n", rdb_bytes.len());
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(rdb_bytes.as_slice()).await?;
        stream.flush().await?;

        Ok(None)
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
