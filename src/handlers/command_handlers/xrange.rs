
use std::sync::Arc;

use crate::{
    resp::RespValue,
    shared_store::shared_store::Store,
    handlers::command_handlers::stream,
};

pub async fn xrange_command(
    store: &Arc<Store>,
    key: String,
    start: Option<String>,
    end: Option<String>,
) -> Result<Option<RespValue>, Box<dyn std::error::Error>> {
    let resp = store.xrange(key, start, end).await?;
    let outer = stream::encode_stream(resp);
    Ok(Some(RespValue::Array(outer)))
}
