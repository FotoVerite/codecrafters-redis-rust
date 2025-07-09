use tokio::{io::{AsyncBufReadExt, BufReader}, net::TcpStream};

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
