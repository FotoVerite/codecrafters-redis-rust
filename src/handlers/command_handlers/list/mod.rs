use std::{io, sync::Arc};

use crate::{resp::RespValue, shared_store::shared_store::Store};

pub async  fn rpush(store: Arc<Store>, key: String, values: Vec<Vec<u8>>) -> io::Result<Option<RespValue>> {
    let len = store.rpush( key, values).await?;
    let result = RespValue::Integer(len as i64);
    Ok(Some(result))
}

pub async  fn lrange(store: Arc<Store>, key: String, start: isize, end: isize) -> io::Result<Option<RespValue>> {
    let values = store.lrange( key, start, end).await?;
    let mut arr = vec![];
    for v in values {
        arr.push(RespValue::BulkString(Some(v)));
    }
    let result = RespValue::Array(arr);
    Ok(Some(result))
}