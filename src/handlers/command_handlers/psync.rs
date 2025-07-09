use std::{io, sync::Arc};

use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
};
use tokio_util::codec::{Framed, FramedRead};
use futures::StreamExt;

use crate::{
    command::{self, ReplconfCommand, RespCommand},
    error_helpers::invalid_data_err,
    replication_manager::manager::ReplicationManager,
    resp::{RespCodec, RespValue},
    server_info::ServerInfo,
};

pub async fn psync_command(
    framed: Framed<TcpStream, RespCodec>,
    _string: String,
    _pos: i64,
    info: Arc<ServerInfo>,
    manager: Arc<tokio::sync::Mutex<ReplicationManager>>,
    peer_addr: String,
) -> io::Result<()> {
    let mut stream = framed.into_inner();
    let peer_address = stream.peer_addr()?;
    let first_response = format!("+FULLRESYNC {} 0\r\n", info.master_replid);

    stream.write_all(first_response.as_bytes()).await?;

    let blank_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
    let rdb_bytes = hex::decode(blank_hex).unwrap();
    let header = format!("${}\r\n", rdb_bytes.len());
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(rdb_bytes.as_slice()).await?;

    stream.flush().await?;
    let (read_half, write_half) = stream.into_split();
    manager
        .lock()
        .await
        .add_replica(&peer_addr, peer_address, write_half)
        .await?;
    let mut framed_reader = FramedRead::new(read_half, RespCodec);
    while let Some(result) = framed_reader.next().await {
        let (resp_value, _) = result?;
        let command: command::RespCommand = command::Command::try_from_resp(resp_value)?;

        match command {
            RespCommand::Ping => {}
            RespCommand::ReplconfCommand(ReplconfCommand::Ack(offset)) => {
                let offset = offset.parse::<u64>().map_err(|_| invalid_data_err("msg"))?;
                manager
                    .lock()
                    .await
                    .update_offset(&peer_addr, offset)
                    .await?;
            }

            _ => {}
        };
    }
    Ok(())
}