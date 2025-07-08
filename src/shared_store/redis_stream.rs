use std::collections::BTreeMap;
type Fields = Vec<(String, String)>;

#[derive(Debug, Clone)]
pub enum StreamEntry {
    Data {
        id: String,
        fields: Fields,
    },
    Tombstone {
        id: String,
    },
    Control {
        kind: ControlType,
    },
}

#[derive(Debug, Clone)]
pub enum ControlType {
    GroupCreate { group_name: String },
    Ack { consumer: String, ids: Vec<String> },
}

#[derive(Debug, Clone,)]
pub struct Stream {
    entries: BTreeMap<String, StreamEntry>, // ID as key
}

impl Stream {
    pub fn append(&mut self, id: String, fields: Fields) {
        let entry = StreamEntry::Data {
            id: id.clone(),
            fields,
        };
        self.entries.insert(id, entry);
    }

    pub fn new() -> Self {
        Self {entries: BTreeMap::new()}
    }
}