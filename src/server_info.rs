use std::io::{self};

use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::error_helpers;

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

    pub async fn handshake(&self) -> io::Result<()> {
        dbg!(&self.role, &self.repl_host, self.repl_port);
        if self.role.as_str() == "master" {
            return Ok(());
        }
        if let (Some(host), Some(port)) = (&self.repl_host, self.repl_port) {
            dbg!(host, port);
            let mut stream = TcpStream::connect((host.as_str(), port)).await?;
            let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
            stream.write_all(ping_cmd).await?;
            stream.flush().await?;
        }
        Ok(())
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
