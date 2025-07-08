use std::{io, sync::Arc};

use futures::SinkExt;
use tokio::net::TcpStream;
use tokio::{
    sync::Mutex,
    time::{interval, Duration},
};
use tokio_util::codec::Framed;

use crate::{
    resp::{RespCodec, RespValue},
    shared_store::shared_store::Store,
};

pub async fn send_heartbeat(
    framed: Arc<Mutex<Framed<TcpStream, RespCodec>>>,
    store: Arc<Store>,
) -> io::Result<()> {

    let mut ticker = interval(Duration::from_millis(200));
    loop {
        dbg!("sending heartbeat");

        ticker.tick().await;

        let offset = store.get_offset().await;

        let ack_command = RespValue::Array(vec![
            RespValue::BulkString(Some(b"REPLCONF".to_vec())),
            RespValue::BulkString(Some(b"ACK".to_vec())),
            RespValue::BulkString(Some(offset.to_string().into_bytes())),
        ]);
        dbg!("sending");
        let mut guard = framed.lock().await;
        guard.send(ack_command).await?;
    }
}


