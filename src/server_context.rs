use std::sync::Arc;
use tokio::sync::Mutex;

use crate::{
    rdb_parser::config::RdbConfig,
    replication_manager::manager::ReplicationManager,
    server_info::ServerInfo,
    shared_store::shared_store::Store,
};

pub struct ServerContext {
    pub store: Arc<Store>,
    pub rdb: Arc<RdbConfig>,
    pub manager: Arc<Mutex<ReplicationManager>>,
    pub info: Arc<ServerInfo>,
}

impl ServerContext {
    pub fn new(
        store: Arc<Store>,
        rdb: Arc<RdbConfig>,
        manager: Arc<Mutex<ReplicationManager>>,
        info: Arc<ServerInfo>,
    ) -> Self {
        Self {
            store,
            rdb,
            manager,
            info,
        }
    }
}
