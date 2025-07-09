pub struct Session {
    pub in_multi: bool,
    pub queued: Vec<RespCommand>,
    pub client_id: u64, // optional
}