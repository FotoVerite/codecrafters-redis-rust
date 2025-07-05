use std::path::Path;

use tokio::io;

#[derive(Debug, Clone)]
pub struct RdbConfig {
    pub dir: String,
    pub dbfilename: String,
}

impl RdbConfig {
    pub fn new() -> Self {
        Self {
            dir: "/tmp/redis-files".to_string(),
            dbfilename: "dump.rdb".to_string(),
        }
    }

    fn dir(&self) -> &String {
        &self.dir
    }

    fn dbfilename(&self) -> &String {
        &self.dbfilename
    }

    pub fn get(&self, key: &str) -> Option<String> {
        match key {
            "dir" => Some(self.dir.clone()),
            "dbfilename" => Some(self.dbfilename.clone()),
            _ => None,
        }
    }

    fn set_dir(&mut self, input: String) -> Result<(), io::Error> {
        let path = Path::new(&input);
        if path.is_dir() {
            self.dir = input;
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Dir"))
        }
    }

    fn set_dbfilename(&mut self, input: String) -> Result<(), io::Error> {
        let path = Path::new(&input);
        if path.is_file() {
            self.dbfilename = input;
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Dir"))
        }
    }
}
