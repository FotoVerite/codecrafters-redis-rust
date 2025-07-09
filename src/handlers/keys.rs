use std::sync::Arc;

use crate::{
    resp::RespValue,
    shared_store::shared_store::Store,
};

pub async fn keys_command(command: String, store: Arc<Store>) -> RespValue {
    match command.as_str() {
        "*" => store.keys().await,
        _ => RespValue::Array(vec![]),
    }
}
