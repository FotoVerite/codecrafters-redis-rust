
use std::sync::Arc;

use crate::{
    command::ConfigCommand, rdb_parser::config::RdbConfig, resp::RespValue
};

pub fn config_command(command: ConfigCommand, rdb: Arc<RdbConfig>) -> RespValue {
    match command {
        ConfigCommand::Get(key) => {
            if let Some(resp) = rdb.get(key.as_str()) {
                let vec = vec![
                    RespValue::BulkString(Some(key.into_bytes())),
                    RespValue::BulkString(Some(resp.into_bytes())),
                ];
                RespValue::Array(vec)
            } else {
                RespValue::BulkString(None)
            }
        }
        _ => RespValue::SimpleString("Ok".into()),
    }
}
