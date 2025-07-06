#![allow(dead_code)]

use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::Path,
};

#[derive(Debug, Clone)]
pub struct RdbConfig {
    pub dir: String,
    pub dbfilename: String,
}

impl RdbConfig {
    pub fn new() -> Self {
        let mut dir = "/tmp/redis-files".to_string();
        let mut dbfilename = "dump.rdb".to_string();
        let mut args = std::env::args().peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--dir" => {
                    if let Some(dir_str) = args.next() {
                        dir = dir_str
                    }
                }
                "--dbfilename" => {
                    if let Some(dbfilename_str) = args.next() {
                        dbfilename = dbfilename_str
                    }
                }
                _ => {}
            }
        }
        Self { dir, dbfilename }
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
            Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Filename"))
        }
    }
}
