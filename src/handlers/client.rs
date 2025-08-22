use crate::resp::{RespCodec, RespValue};
use std::{net::SocketAddr};
use tokio::{net::TcpStream, sync::mpsc::{self, Receiver, Sender}};
use tokio_util::codec::Framed;

#[derive(Debug, PartialEq, Eq)]
pub enum ClientMode {
    Normal,
    Subscribed,
    Multi,
}

pub struct Client {
    pub framed: Framed<TcpStream, RespCodec>,
    pub mode: ClientMode,
    pub addr: SocketAddr,
    pub channels: Vec<String>,
    pub rx: Receiver<RespValue>,
    pub tx: Sender<RespValue>,
}

impl Client {
    pub fn new(socket: TcpStream) -> Self {
        let addr = socket.peer_addr().unwrap();
        let (tx, mut rx) = mpsc::channel(1024);

        Self {
            framed: Framed::new(socket, RespCodec),
            mode: ClientMode::Normal,
            addr,
            channels: vec![],
            rx,
            tx,
        }
    }
}
