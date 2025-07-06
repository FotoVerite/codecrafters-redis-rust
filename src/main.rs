mod command;
mod rdb;
mod resp;
mod shared_store;

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use crate::{
    command::{ConfigCommand, RespCommand}, rdb::config::RdbConfig, resp::RespValue, shared_store::Store
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    //Uncomment this block to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let store = Arc::new(Store::new());
    let rdb = Arc::new(RdbConfig::new());
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
        // Spawn a new async task to handle the connection
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, store_clone, rdb_clone).await {
                eprintln!("Error handling {}: {:?}", addr, e);
            }
            // Use `socket` to read/write asynchronously here
        });
    }

    async fn handle_connection(
        socket: TcpStream,
        store: Arc<Store>,
        rdb: Arc<RdbConfig>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut framed = Framed::new(socket, resp::RespCodec);

        while let Some(result) = framed.next().await {
            let resp_value = result?;
            let command: command::RespCommand = command::Command::try_from_resp(resp_value)?;
            println!("Received: {:?}", &command);

            let response_value = match command {
                RespCommand::Ping => RespValue::SimpleString("PONG".into()),
                RespCommand::Echo(s) => RespValue::BulkString(Some(s.into_bytes())),
                RespCommand::Get(key) => store.get(&key).await,
                RespCommand::Set { key, value, px } => {
                    store.set(&key, value, px).await;
                    RespValue::SimpleString("OK".into())
                }
                RespCommand::ConfigCommand(command) => handle_config_command(command, rdb.clone()),
                RespCommand::Keys(string) => handle_keys_command(string, store.clone()).await,
            };
            println!("Sending: {:?}", &response_value);

            framed.send(response_value).await?;
        }

        Ok(())
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

    async fn handle_keys_command(command: String, store: Arc<Store>) -> RespValue {
        match command.as_str() {
            "*" => store.keys().await,
            other => RespValue::Array(vec![]),
        }
    }
}
