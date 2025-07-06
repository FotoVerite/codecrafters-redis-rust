pub struct ServerInfo {
    pub redis_version: String,
    pub redis_mode: String,
    pub os: String,
    pub arch_bits: usize,
    pub process_id: u32,
    pub uptime_in_seconds: u64,
    pub uptime_in_days: u64,
    pub hz: usize,
    pub lru_clock: u64,
    pub executable: String,
    pub config_file: Option<String>,
    pub tcp_port: u16,
    pub role: String, // <- add this
}

impl ServerInfo {
    pub fn new() -> Self {
        let mut tcp_port = 6379u16;
        let mut args = std::env::args().peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--port" => {
                    if let Some(port_str) = args.next() {
                        tcp_port = port_str.parse().unwrap_or_else(|_| {
                            6379u16
                        })
                    }
                }

                _ => {}
            }
        }
        Self {
            redis_version: "7.2.0".into(),
            redis_mode: "standalone".into(),
            os: std::env::consts::OS.into(),
            arch_bits: 64,
            process_id: std::process::id(),
            uptime_in_seconds: 0,
            uptime_in_days: 0,
            hz: 10,
            lru_clock: 0,
            executable: std::env::args().next().unwrap_or_default(),
            config_file: None,
            tcp_port,
            role: "master".into(), // <- default role }
        }
    }
}
