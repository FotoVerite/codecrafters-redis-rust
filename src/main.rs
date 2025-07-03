mod command;
mod resp;
mod shared_store;

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

use crate::{command::RespCommand, resp::RespValue, shared_store::Store};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    //Uncomment this block to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let store = Arc::new(Store::new());
    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from {}", addr);
        let store_clone = store.clone();
        // Spawn a new async task to handle the connection
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, store_clone).await {
                eprintln!("Error handling {}: {:?}", addr, e);
            }
            // Use `socket` to read/write asynchronously here
        });
    }

    async fn handle_connection(
        socket: TcpStream,
        store: Arc<Store>,
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
                RespCommand::Set(key, value) => {
                    store.set(&key, value).await;
                    RespValue::SimpleString("OK".into())
                }
            };
            println!("Sending: {:?}", &response_value);

            framed.send(response_value).await?;
        }

        Ok(())
    }
}
