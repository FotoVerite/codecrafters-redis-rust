
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener,
    },
};
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
            let (reader, writer) = socket.into_split();
            let reader = BufReader::new(reader);
            let line = String::new();

            let _ = async_parse_stream(reader, writer, line).await;

            // Use `socket` to read/write asynchronously here
        });
    }

    async fn async_parse_stream(
        mut reader: BufReader<OwnedReadHalf>,
        mut writer: OwnedWriteHalf,
        mut line: String,
    ) -> std::io::Result<()> {
        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line).await?;
            if bytes_read == 0 {
                // Connection closed
                break;
            }
            if line.trim() == "PING" {
                writer.write_all(b"+PONG\r\n").await?;
            }
        }
        Ok(())
    }
}
