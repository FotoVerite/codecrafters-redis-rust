use std::collections::BTreeMap;
use std::io;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub struct List {
    pub notify: Arc<Notify>,
    pub entries: Vec<Vec<u8>>, // ID as key
}

impl List {
    pub fn rpush(&mut self, values: Vec<Vec<u8>>) -> io::Result<usize> {
        self.entries.extend(values);
        self.notify.notify_waiters();
        Ok(self.entries.len())
    }

    pub fn lpop(&mut self, amount: usize) -> io::Result<Option<Vec<Vec<u8>>>> {
        if self.entries.is_empty() {
            self.notify.notify_waiters();
            return Ok(None);
        }
        let values = self.entries.drain(..amount).collect();
        self.notify.notify_waiters();
        Ok(Some(values))
    }

    pub fn lpush(&mut self, mut values: Vec<Vec<u8>>) -> io::Result<usize> {
        values.extend(self.entries.drain(..));
        self.entries = values.clone();
        self.notify.notify_waiters();
        Ok(self.entries.len())
    }

    pub fn new(notify: Arc<Notify>, values: Vec<Vec<u8>>) -> Self {
        Self {
            notify,
            entries: values,
        }
    }
}
