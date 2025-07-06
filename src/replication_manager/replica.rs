use futures::{io, SinkExt};
use std::net::SocketAddr;
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, Receiver, Sender},
};
use tokio_util::codec::Framed;

use crate::{
    command::RespCommand,
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
    pub fn new(address: SocketAddr, stream: TcpStream) -> Self {
        let (tx, mut rx) = mpsc::channel::<RespCommand>(32);

        tokio::spawn(async move {
            let mut framed: Framed<TcpStream, RespCodec> = Framed::new(stream, RespCodec);

            while let Some(command) = rx.recv().await {
                dbg!("hello");
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
        dbg!(&self.address);
        self.tx.send(command).await.map_err(|e| {
            invalid_data_err(format!(
                "Failed to send command to replica {}: {}",
                self.address, e
            ))
        })?;
        Ok(())
    }
}
