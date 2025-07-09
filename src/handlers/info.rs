use std::sync::Arc;

use crate::{
    resp::RespValue,
    server_info::ServerInfo,
};

pub fn info_command(_command: String, info: Arc<ServerInfo>) -> RespValue {
    RespValue::BulkString(Some(info.info_section().into_bytes()))
}
