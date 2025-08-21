use crate::command::RespCommand;

pub struct Session {
    pub queued: Vec<(RespCommand, Vec<u8>)>,
}

impl Session {
    pub fn new() -> Self {
        Self {
            queued: vec![],
        }
    }
}
