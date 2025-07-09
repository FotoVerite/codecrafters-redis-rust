use futures::{io, SinkExt};
use std::net::SocketAddr;
use tokio::{
    net::tcp::OwnedWriteHalf,
    sync::mpsc::{self, Sender},
};
use tokio_util::codec::FramedWrite;

use crate::{
    command::{ReplconfCommand, RespCommand},
    error_helpers::invalid_data_err,
    resp::{RespCodec, RespValue},
};

#[derive(Debug)]
pub struct Replica {
    pub address: SocketAddr,
    pub tx: Sender<RespCommand>,
    pub acknowledged_offset: u64,
    pub is_online: bool,
}

impl Replica {
    pub fn new(address: SocketAddr, stream: OwnedWriteHalf) -> Self {
        let (tx, mut rx) = mpsc::channel::<RespCommand>(32);

        tokio::spawn(async move {
            let mut framed  = FramedWrite::new(stream, RespCodec);

            while let Some(command) = rx.recv().await {
                match command {
                    RespCommand::Set { key, value, px } => {
                        let values = vec![
                            RespValue::BulkString(Some(b"SET".to_vec())),
                            RespValue::BulkString(Some(key.into())),
                            RespValue::BulkString(Some(value)),
                        ];
                        let request = RespValue::Array(values);
                        let _ = framed.send(request).await;
                    }
                    RespCommand::ReplconfCommand(ReplconfCommand::Getack(_)) => {
                        let values = vec![
                            RespValue::BulkString(Some(b"REPLCONF".to_vec())),
                            RespValue::BulkString(Some(b"GETACK".to_vec())),
                            RespValue::BulkString(Some(b"*".to_vec())),
                        ];
                        let request = RespValue::Array(values);
                        let _ = framed.send(request).await;
                    }
                    _ => {}
                }
            }
        });
        Self {
            address,
            tx,
            acknowledged_offset: 0,
            is_online: true,
        }
    }

    pub async fn send(&self, command: RespCommand) -> io::Result<()> {
        self.tx.send(command).await.map_err(|e| {
            invalid_data_err(format!(
                "Failed to send command to replica {}: {}",
                self.address, e
            ))
        })?;
        Ok(())
    }
}
