use std::{
    io,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::error_helpers::invalid_data_err;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamID {
    pub ms: u64,
    pub seq: u64,
}

impl StreamID {
    pub fn new(ms: u64, seq: u64) -> io::Result<Self> {
        if ms == 0 && seq == 0 {
            Err(invalid_data_err("ID must be greater than 0-0"))
        } else {
            Ok(Self { ms, seq })
        }
    }

    pub fn from_redis_input(previous: Option<StreamID>, given: String) -> io::Result<Self> {
        Ok(Self::generate(previous, given)?)
    }

    pub fn generate(previous: Option<StreamID>, given: String) -> io::Result<Self> {
        match given.as_str() {
            "*" => Self::generate_full_id(previous),
            s if s.ends_with('*') => {
                let ms_str = s.strip_suffix('*').unwrap().strip_suffix('-').unwrap_or("");
                let ms = ms_str
                    .parse::<u64>()
                    .map_err(|_| invalid_data_err("Invalid ms"))?;
                let mut seq = match previous {
                    Some(prev) if prev.ms == ms => prev.seq + 1,
                    _ => 0,
                };
                if ms == 0 && seq == 0 {
                    seq += 1;
                }
                StreamID::new(ms, seq)
            }
            _ => StreamID::try_from(given.as_str()),
        }
    }

    fn generate_full_id(previous: Option<StreamID>) -> io::Result<Self> {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        let seq = if let Some(previous) = previous {
            if previous.ms == ms {
                previous.seq + 1
            } else {
                0
            }
        } else {
            0
        };
        Ok(StreamID { ms, seq })
    }
}

impl TryFrom<&str> for StreamID {
    type Error = io::Error;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let (ms, seq) = parse_stream_id(value)?;
        Ok(Self { ms, seq })
    }
}

impl std::fmt::Display for StreamID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl From<StreamID> for String {
    fn from(id: StreamID) -> String {
        id.to_string()
    }
}

fn parse_stream_id(id: &str) -> io::Result<(u64, u64)> {
    let mut parts = id.splitn(2, '-');
    let ms = parts
        .next()
        .ok_or_else(|| invalid_data_err("Invalid ID format"))?
        .parse::<u64>()
        .map_err(|_| invalid_data_err("Invalid milliseconds in stream ID"))?;

    let seq = parts
        .next()
        .ok_or_else(|| invalid_data_err("Invalid ID format"))?
        .parse::<u64>()
        .map_err(|_| invalid_data_err("Invalid sequence in stream ID"))?;

    Ok((ms, seq))
}
