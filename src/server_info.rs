use std::io::{self, BufRead};

use futures::{SinkExt, StreamExt};
use std::io::Read;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};
use tokio_util::codec::Framed;

use crate::{
    error_helpers,
    resp::{RespCodec, RespValue},
};

#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub redis_version: String,
    pub redis_mode: String,
    pub os: String,
    pub arch_bits: usize,
    pub process_id: u32,
    pub uptime_in_seconds: u64,
    pub uptime_in_days: u64,
    pub hz: usize,
    pub lru_clock: u64,
    pub executable: String,
    pub config_file: Option<String>,
    pub tcp_port: u16,
    pub role: String,
    pub repl_host: Option<String>,
    pub repl_port: Option<u16>, // <- add this
    pub master_replid: String,
    pub master_repl_offset: u64,
}

impl ServerInfo {
    pub fn new() -> io::Result<Self> {
        let mut tcp_port = 6379u16;
        let mut role = "master";
        let mut repl_host = None;
        let mut repl_port = None;
        let mut args = std::env::args().peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--port" => {
                    if let Some(port_str) = args.next() {
                        tcp_port = port_str.parse().unwrap_or_else(|_| 6379u16)
                    }
                }
                "--replicaof" => {
                    role = "slave";
                    parse_repl_instance(&mut args, &mut repl_host, &mut repl_port)?;
                }

                _ => {}
            }
        }
        Ok(Self {
            redis_version: "7.2.0".into(),
            redis_mode: "standalone".into(),
            os: std::env::consts::OS.into(),
            arch_bits: 64,
            process_id: std::process::id(),
            uptime_in_seconds: 0,
            uptime_in_days: 0,
            hz: 10,
            lru_clock: 0,
            executable: std::env::args().next().unwrap_or_default(),
            config_file: None,
            tcp_port,
            role: role.into(),
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".into(),
            master_repl_offset: 0,
            repl_host,
            repl_port, // <- default role }
        })
    }

    pub fn info_section(&self) -> String {
        format!(
            "# Server\n\
            redis_version:{}\n\
            redis_mode:{}\n\
            os:{}\n\
            arch_bits:{}\n\
            process_id:{}\n\
            uptime_in_seconds:{}\n\
            uptime_in_days:{}\n\
            hz:{}\n\
            lru_clock:{}\n\
            executable:{}\n\
            config_file:{}\n\
            tcp_port:{}\n\
            role:{}\n\
            master_replid:{}\n\
            master_repl_offset:{}\n",
            self.redis_version,
            self.redis_mode,
            self.os,
            self.arch_bits,
            self.process_id,
            self.uptime_in_seconds,
            self.uptime_in_days,
            self.hz,
            self.lru_clock,
            self.executable,
            self.config_file.clone().unwrap_or_default(),
            self.tcp_port,
            self.role,
            self.master_replid,
            self.master_repl_offset
        )
    }

    pub async fn handshake(
        &self,
    ) -> Result<Option<TcpStream>, Box<dyn std::error::Error + Send + Sync>> {
        if self.role.as_str() == "master" {
            return Ok(None);
        }
        if let (Some(host), Some(port)) = (&self.repl_host, self.repl_port) {
            let mut stream = TcpStream::connect((host.as_str(), port)).await?;
            let mut framed = Framed::new(stream, RespCodec);
            framed
                .send(RespValue::Array(vec![RespValue::BulkString(Some(
                    "PING".into(),
                ))]))
                .await?;
            let _ = framed.next().await; // optionally check for +OK

            let port_str = self.tcp_port.to_string();
            framed
                .send(RespValue::Array(vec![
                    RespValue::BulkString(Some("REPLCONF".into())),
                    RespValue::BulkString(Some("listening-port".into())),
                    RespValue::BulkString(Some(port_str.into_bytes())),
                ]))
                .await?;
            let _ = framed.next().await; // optionally check for +OK

            // Step 3: Send REPLCONF capa psync2
            framed
                .send(RespValue::Array(vec![
                    RespValue::BulkString(Some("REPLCONF".into())),
                    RespValue::BulkString(Some("capa".into())),
                    RespValue::BulkString(Some("psync2".into())),
                ]))
                .await?;
            let _ = framed.next().await;

            framed
                .send(RespValue::Array(vec![
                    RespValue::BulkString(Some("PSYNC".into())),
                    RespValue::BulkString(Some("?".into())),
                    RespValue::BulkString(Some("-1".into())),
                ]))
                .await?;
            if let Some(Ok(RespValue::SimpleString(fullresync_line))) = framed.next().await {
                println!("Got FULLRESYNC: {}", fullresync_line);
            } else {
                return Err("Expected +FULLRESYNC line".into());
            }

            // read RDB bulk string
            // if let Some(Ok(RespValue::BulkString(Some(rdb_bytes)))) = framed.next().await {
            //     // rdb_bytes is the entire RDB payload
            //     println!("Got RDB of length {}", rdb_bytes.len());
            // } else {
            //     println!("Expected bulk string with RDB");
            //     return Err("Expected bulk string with RDB".into());
            // }

            // Extract the stream back from the framed object before peeking
          

            let mut socket = framed.into_inner();
            let _rdb = read_rdb_from_master(&mut socket).await?;
            return Ok(Some(socket));
        }
        Ok(None)
    }
}

fn parse_repl_instance(
    args: &mut impl Iterator<Item = String>,
    host: &mut Option<String>,
    port: &mut Option<u16>,
) -> io::Result<()> {
    if let Some(host_str) = args.next() {
        let parts: Vec<&str> = host_str.split_whitespace().collect();
        if parts.len() == 2 {
            *host = Some(parts[0].into());
            *port = Some(
                parts[1]
                    .parse()
                    .map_err(|_| error_helpers::invalid_data_err("Invalid host"))?,
            );
            return Ok(());
        } else {
            *host = Some(host_str)
        }
    }

    if let Some(port_str) = args.next() {
        *port = Some(
            port_str
                .parse::<u16>()
                .map_err(|_| error_helpers::invalid_data_err("Invalid host"))?,
        )
    }
    Ok(())
}

pub async fn debug_peek_handshake( stream: TcpStream) -> std::io::Result<TcpStream> {
    let mut reader = BufReader::new(stream);

    // Peek into the handshake response
    let buf = reader.fill_buf().await?;
    println!(
        "[debug] Peeked handshake bytes: {:02X?}",
        &buf[..buf.len().min(64)]
    );

    // Optionally consume nothing
    // reader.consume(buf.len());

    // Recover the TcpStream from BufReader
    let stream = reader.into_inner();
    Ok(stream)
}

async fn read_rdb_from_master(
    stream: &mut TcpStream,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut buf = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        let n = stream.read(&mut byte).await?;
        if n == 0 {
            return Err(Box::<dyn std::error::Error + Send + Sync>::from("Connection closed"));
        }
        buf.push(byte[0]);
        let len = buf.len();
        if len >= 2 && buf[len - 2..] == *b"\r\n" {
            break;
        }
    }
    if buf.first() != Some(&b'$') {
        Err(Box::<dyn std::error::Error + Send + Sync>::from("Expected RESP bulk string"))
    } else {
        let len_str = std::str::from_utf8(&buf[1..buf.len() - 2])?;
        let rdb_len: usize = len_str.parse()?;
        // strip `$` and `\r\n`
        let mut rdb = vec![0u8; rdb_len];
        stream.read_exact(&mut rdb).await?;
        Ok(rdb)
    }
}