use std::collections::BTreeMap;
use std::io;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use tokio::sync::Notify;

use crate::error_helpers::invalid_data_err;
use crate::shared_store::stream_id::StreamID;

type Fields = Vec<(String, String)>;
pub type StreamEntries = Vec<(StreamID, StreamEntry)>;

#[derive(Debug, Clone)]
pub enum StreamEntry {
    Data { id: StreamID, fields: Fields },
}

#[derive(Debug, Clone)]
pub enum _ControlType {
    GroupCreate { group_name: String },
    Ack { consumer: String, ids: Vec<String> },
}

#[derive(Debug, Clone)]
pub struct Stream {
    pub notify: Arc<Notify>,
    entries: BTreeMap<StreamID, StreamEntry>, // ID as key
}

impl Stream {
    pub fn append(&mut self, id: StreamID, fields: Fields) -> io::Result<()> {
        let entry = StreamEntry::Data {
            id,
            fields,
        };
        Self::validate_id(&id, self.previous_id())?;
        self.entries.insert(id, entry);
        self.notify.notify_waiters();
        Ok(())
    }

    pub fn previous_id(&self) -> &StreamID {
        if let Some((key, _)) = self.entries.last_key_value() {
            return key;
        }
        &StreamID { ms: 0, seq: 0 }
    }

    pub fn new(notify: Arc<Notify>) -> Self {
        Self {
            notify,
            entries: BTreeMap::new(),
        }
    }
    pub fn get_from(&self, start: StreamID) -> StreamEntries {
        self.entries
            .range::<StreamID, _>((Excluded(start), Unbounded))
            .map(|(id, entry)| (*id, entry.clone()))
            .collect()
    }
    pub fn get_range(&self, start: Option<StreamID>, end: Option<StreamID>) -> StreamEntries {
        let lower = {
            match start {
                Some(start) => Included(start),
                None => Unbounded,
            }
        };
        let upper = {
            match end {
                Some(end) => Included(end),
                None => Unbounded,
            }
        };
        self.entries
            .range::<StreamID, _>((lower, upper))
            .map(|(id, entry)| (*id, entry.clone()))
            .collect()
    }

    fn validate_id(id: &StreamID, previous: &StreamID) -> io::Result<bool> {
        if id == &(StreamID { ms: 0, seq: 0 }) {
            return Err(invalid_data_err(
                "ERR The ID specified in XADD must be greater than 0-0",
            ));
        }
        if id.ms < previous.ms || id.ms == previous.ms && id.seq <= previous.seq {
            return Err(invalid_data_err(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
            ));
        }
        Ok(true)
    }
}
