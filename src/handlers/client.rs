use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use crate::resp::RespCodec;

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
}

impl Client {
    pub fn new(socket: TcpStream) -> Self {
        let addr = socket.peer_addr().unwrap();
        Self {
            framed: Framed::new(socket, RespCodec),
            mode: ClientMode::Normal,
            addr,
        }
    }
}
