mod command;
mod resp;

use futures::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
};
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    //Uncomment this block to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from {}", addr);

        // Spawn a new async task to handle the connection
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                eprintln!("Error handling {}: {:?}", addr, e);
            }
            // Use `socket` to read/write asynchronously here
        });
    }

    async fn handle_connection(socket: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
        let mut framed = Framed::new(socket, resp::RespCodec);

        while let Some(result) = framed.next().await {
            let resp_value = result?;
            let command: command::RespCommand = command::Command::try_from_resp(resp_value)?;
            println!("Received: {:?}", command);
            framed.send(command.to_resp()).await?;
        }

        Ok(())
    }
}
