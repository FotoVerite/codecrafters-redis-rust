
use std::sync::Arc;

use crate::{
    resp::RespValue,
    shared_store::shared_store::Store,
};

pub async fn xadd_command(
    store: &Arc<Store>,
    key: String,
    id: String,
    fields: Vec<(String, String)>,
    bytes: Vec<u8>,
) -> Result<Option<RespValue>, Box<dyn std::error::Error>> {
    store.append_to_log(bytes).await;
    match store.xadd(&key, id.clone(), fields).await {
        Ok(generated_id) => Ok(Some(RespValue::BulkString(Some(generated_id.into_bytes())))),
        Err(e) => Ok(Some(RespValue::Error(e.to_string()))),
    }
}
