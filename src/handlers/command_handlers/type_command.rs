
use std::sync::Arc;

use crate::{
    resp::RespValue,
    shared_store::shared_store::Store,
};

pub async fn type_command(
    store: &Arc<Store>,
    key: String,
) -> Result<Option<RespValue>, Box<dyn std::error::Error>> {
    Ok(Some(store.get_type(&key).await?))
}
