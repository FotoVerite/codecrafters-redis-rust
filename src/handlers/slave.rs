use std::sync::Arc;

use tokio::sync::Mutex;
use tokio_util::codec::Framed;

use crate::{
    error_helpers::invalid_data_err,
    heartbeat,
    resp::{self},
    server_info::ServerInfo,
    shared_store::shared_store::Store,
};

use super::replication::handle_replication_connection;

type ArcFrame = Arc<Mutex<Framed<tokio::net::TcpStream, resp::RespCodec>>>;

pub fn setup_heartbeat(framed: ArcFrame, store: Arc<Store>) {
    tokio::spawn(async move {
        _ = heartbeat::send_heartbeat(framed, store).await;
    });
}

pub fn setup_master_listener(framed: ArcFrame, store: Arc<Store>, info: Arc<ServerInfo>) {
    tokio::spawn(async move {
        let mut guard = framed.lock().await;

        handle_replication_connection(&mut guard, store, info)
            .await
            .map_err(|e| invalid_data_err(format!("Replication Listener had error, {}", e)))
    });
}