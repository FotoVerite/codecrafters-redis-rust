use crate::command::RespCommand;

pub struct Session {
    pub in_multi: bool,
    pub queued: Vec<RespCommand>,
    pub client_id: u64, // optional
}

impl Session {
    pub fn new() -> Self {
        Self {
            in_multi: false,
            queued: vec![],
            client_id: 1,
        }
    }

   
}
