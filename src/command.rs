use futures::io;

use crate::resp::RespValue;

#[derive(Debug, Clone)]
pub enum ConfigCommand {
    Get(String),
    _Set(String, String),
}
#[derive(Debug, Clone)]
pub enum ReplconfCommand {
    ListeningPort(String),
    #[allow(dead_code)]
    Capa(String),
    Getack(String),
    Ack(String),
}
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum PushDirection {
    LPush,
    RPush,
    LPushX,
    RPushX,
    LInsertBefore,
    LInsertAfter,
}

#[derive(Debug, Clone)]
pub enum RespCommand {
    ConfigCommand(ConfigCommand),
    Echo(String),
    Get(String),
    Incr(String),
    Info(String),
    Keys(String),
    Multi,
    Exec,
    Discard,
    Ping,
    Publish(String, String),
    PSYNC(String, i64),
    #[allow(dead_code)]
    RDB(Option<Vec<u8>>),
    ReplconfCommand(ReplconfCommand),
    Set {
        key: String,
        value: Vec<u8>,
        px: Option<u64>,
    },
    Subscribe(String),
    Type(String),
    Wait(String, String),
    Xadd {
        key: String,
        id: String, // Can be "*" or an explicit "1688512345678-0"
        fields: Vec<(String, String)>,
    },
    Xrange {
        key: String,
        start: Option<String>,
        end: Option<String>,
    },
    #[allow(dead_code)]
    Xread {
        count: Option<u64>,
        block: Option<u64>,
        keys: Vec<String>,
        ids: Vec<String>,
    },
    Rpush {
        key: String,
        values: Vec<Vec<u8>>,
    },
    Llen(String),
    BLPop(Vec<String>, u64),
    Lpop(String, usize),
    Lpush {
        key: String,
        values: Vec<Vec<u8>>,
    },
    Lrange {
        key: String,
        start: isize,
        end: isize,
    },

    Unsubscribe(String),
    PSubscribe,
    PunSubscribe,
    Quit,
}

use std::fmt;

impl fmt::Display for RespCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespCommand::Ping => write!(f, "PING"),
            RespCommand::Echo(_) => write!(f, "ECHO"),
            RespCommand::Subscribe(_) => write!(f, "SUBSCRIBE"),
            RespCommand::Set { .. } => write!(f, "SET"),
            RespCommand::Get { .. } => write!(f, "get"),
            // …add others as needed…
            _ => write!(f, "{:?}", self), // fallback to Debug
        }
    }
}

impl RespCommand {
    pub fn _to_resp(self) -> RespValue {
        match self {
            RespCommand::Ping => RespValue::SimpleString("PONG".into()),
            RespCommand::Echo(s) => RespValue::BulkString(Some(s.into_bytes())),
            _ => RespValue::Error("-1".to_string()),
        }
    }
}

pub struct Command {
    name: String,
    args: Vec<String>,
}

impl Command {
    pub fn new(input: Vec<RespValue>) -> Result<Self, io::Error> {
        if let Some(resp_value) = input.get(0) {
            let name = match resp_value {
                RespValue::SimpleString(s) => s.clone(),
                RespValue::BulkString(s) => convert_bulk_string(s.to_owned())?,
                _ => invalid_data("Unexpected RespValue")?,
            };
            let mut args = Vec::with_capacity(input.len());
            for arg in input.iter().skip(1) {
                let s = match arg {
                    RespValue::BulkString(s) => convert_bulk_string(s.to_owned())?,
                    RespValue::SimpleString(s) => s.clone(),

                    _ => invalid_data("Unexpected RespValue")?,
                };
                args.push(s);
            }
            return Ok(Self { name, args });
        } else {
            invalid_data("Unexpected RespValue")?
        }
    }

    pub fn try_from_resp(value: RespValue) -> Result<RespCommand, io::Error> {
        match value {
            RespValue::RDB(info) => Ok(RespCommand::RDB(info)),
            RespValue::Array(a) => {
                let command = Command::new(a)?;
                match command.name.to_ascii_lowercase().as_str() {
                    "subscribe" => Ok(RespCommand::Subscribe(command.args[0].clone())),
                    "multi" => Ok(RespCommand::Multi),
                    "discard" => Ok(RespCommand::Discard),
                    "exec" => Ok(RespCommand::Exec),
                    "ping" => Ok(RespCommand::Ping),
                    "publish" => Ok(RespCommand::Publish(
                        command.args[0].clone(),
                        command.args[1].clone(),
                    )),

                    "echo" => Ok(RespCommand::Echo(command.args[0].clone())),
                    "get" => Ok(RespCommand::Get(command.args[0].clone())),
                    "set" => parse_set(command),
                    "type" => Ok(RespCommand::Type(command.args[0].clone())),
                    "config" => parse_config(command),
                    "keys" => Ok(RespCommand::Keys(command.args[0].clone())),
                    "incr" => Ok(RespCommand::Incr(command.args[0].clone())),
                    "info" => Ok(RespCommand::Info(command.args[0].clone())),
                    "replconf" => parse_replconf(command),
                    "llen" => Ok(RespCommand::Llen(command.args[0].clone())),
                    "lpop" => parse_pop_command(command),
                    "blpop" => parse_blpop_command(command),
                    "lpush" => parse_push_command(command, PushDirection::LPush),
                    "rpush" => parse_push_command(command, PushDirection::RPush),
                    "lrange" => parse_lrange(command),

                    "psync" => parse_psync(command),
                    "wait" => Ok(RespCommand::Wait(
                        command.args[0].clone(),
                        command.args[1].clone(),
                    )),
                    "xadd" => parse_xadd(command),
                    "xrange" => parse_xrange(command),
                    "xread" => parse_xread(command),
                    "unsubscribe" => Ok(RespCommand::Unsubscribe(command.args[0].clone())),

                    other => invalid_data(format!("Unexpected Command: {}", other)),
                }
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected Top Level Array",
            )),
        }
    }
}

fn parse_pop_command(command: Command) -> io::Result<RespCommand> {
    let key = command.args[0].clone();
    let arg = match command.args.get(1) {
        Some(arg) => arg
            .parse()
            .map_err(|_| invalid_data_err("Unable to parse param"))?,
        None => 1usize,
    };
    Ok(RespCommand::Lpop(key, arg))
}

fn parse_blpop_command(mut command: Command) -> io::Result<RespCommand> {
    let timeout = match command.args.pop() {
        None => return invalid_data("No timeout given"),
        Some(arg) => arg
            .parse::<f64>()
            .map_err(|_| invalid_data_err("Unable to parse param"))?,
    };
    if timeout < 0f64 {
        return invalid_data("Negative Time given");
    }
    let millis = if timeout > 0.0 {
        (timeout * 1000.0).ceil() as u64
    } else {
        0
    };
    Ok(RespCommand::BLPop(command.args, millis))
}
fn parse_push_command(command: Command, lpush: PushDirection) -> io::Result<RespCommand> {
    let key = command.args[0].clone();
    let mut values = command
        .args
        .iter()
        .skip(1)
        .map(|s| s.as_bytes().to_vec())
        .collect::<Vec<Vec<u8>>>();
    match lpush {
        PushDirection::LPush => {
            values.reverse();
            Ok(RespCommand::Lpush { key, values })
        }
        PushDirection::RPush => Ok(RespCommand::Rpush { key, values }),
        _ => invalid_data("Not implemented"),
    }
}

fn parse_lrange(command: Command) -> io::Result<RespCommand> {
    let key = command.args[0].clone();
    let start = command.args[1]
        .parse()
        .map_err(|_| invalid_data_err("start does not exists are is not a number"))?;
    let end = command.args[2]
        .parse()
        .map_err(|_| invalid_data_err("start does not exists are is not a number"))?;
    Ok(RespCommand::Lrange { key, start, end })
}
fn parse_xread(command: Command) -> Result<RespCommand, io::Error> {
    let (optional, rest) = {
        let pos = command
            .args
            .iter()
            .position(|arg| arg.to_lowercase() == "streams")
            .ok_or_else(|| invalid_data_err("missing STREAMS keyword"))?;
        (
            &command.args[0..pos],
            &command.args[pos + 1..command.args.len()],
        )
    };
    let mut optional_iter = optional.into_iter();
    let mut block = None;
    let mut count = None;
    while let Some(arg) = optional_iter.next() {
        match arg.to_ascii_lowercase().as_str() {
            "block" => {
                block = optional_iter
                    .next()
                    .map(|s| s.parse::<u64>())
                    .transpose()
                    .or_else(|e| invalid_data(e.to_string()))?;
            }
            "count" => {
                count = optional_iter
                    .next()
                    .map(|s| s.parse::<u64>())
                    .transpose()
                    .or_else(|e| invalid_data(e.to_string()))?;
            }
            _ => {}
        }
    }
    let (keys, stream_ids) = {
        let length = rest.len() / 2;
        (&rest[..length], &rest[length..])
    };
    if keys.len() != stream_ids.len() {
        return invalid_data("Stream Keys must match StreamIds");
    }
    Ok(RespCommand::Xread {
        count,
        block,
        keys: keys.to_vec(),
        ids: stream_ids.to_vec(),
    })
}

fn parse_xadd(command: Command) -> Result<RespCommand, io::Error> {
    let key = command.args[0].clone();
    let id = command.args[1].clone();
    let rest = &command.args[2..];

    if rest.len() % 2 != 0 {
        return invalid_data("Each field must have a key value pair");
    }
    let fields = rest
        .chunks(2)
        .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
        .collect();

    Ok(RespCommand::Xadd { key, id, fields })
}

fn parse_xrange(command: Command) -> Result<RespCommand, io::Error> {
    let key = command.args[0].clone();
    let mut range = command.args.iter().skip(1);
    let start = range.next().cloned();
    let end: Option<String> = range.next().cloned();
    Ok(RespCommand::Xrange { key, start, end })
}

fn parse_set(command: Command) -> Result<RespCommand, io::Error> {
    let key = command.args[0].clone();
    let value = command.args[1].clone().into_bytes();
    let mut px = None;
    let mut optional_args = command.args.iter().skip(2);
    while let Some(arg) = optional_args.next() {
        match arg.to_lowercase().as_str() {
            "px" => {
                if let Some(px_value) = optional_args.next() {
                    match px_value.parse::<u64>() {
                        Ok(val) => px = Some(val),
                        Err(_) => return invalid_data("PX value must be a positive integer"),
                    }
                } else {
                    return invalid_data("PX value must be a positive integer");
                }
            }
            _ => {}
        }
    }
    Ok(RespCommand::Set { key, value, px })
}

fn parse_replconf(command: Command) -> io::Result<RespCommand> {
    let Some(action) = command.args.get(0) else {
        return invalid_data("Missing Replconf action");
    };
    match action.to_ascii_lowercase().as_str() {
        "listening-port" => {
            let port = command
                .args
                .get(1)
                .ok_or_else(|| invalid_data_err("Missing Port Field"))?;
            Ok(RespCommand::ReplconfCommand(
                ReplconfCommand::ListeningPort(port.clone()),
            ))
        }

        "capa" => {
            let capa = command
                .args
                .get(1)
                .ok_or_else(|| invalid_data_err("Missing Capa fields"))?;
            Ok(RespCommand::ReplconfCommand(ReplconfCommand::Capa(
                capa.clone(),
            )))
        }
        "getack" => {
            let arg = command
                .args
                .get(1)
                .ok_or_else(|| invalid_data_err("Missing arg field"))?;
            Ok(RespCommand::ReplconfCommand(ReplconfCommand::Getack(
                arg.clone(),
            )))
        }
        "ack" => {
            let arg = command
                .args
                .get(1)
                .ok_or_else(|| invalid_data_err("Missing arg field"))?;
            Ok(RespCommand::ReplconfCommand(ReplconfCommand::Ack(
                arg.clone(),
            )))
        }
        _ => invalid_data("Unknown Replconf action"),
    }
}

fn parse_config(command: Command) -> Result<RespCommand, io::Error> {
    let Some(action) = command.args.get(0) else {
        return invalid_data("Missing CONFIG action");
    };

    match action.to_ascii_lowercase().as_str() {
        "get" => {
            let key = command
                .args
                .get(1)
                .ok_or_else(|| invalid_data_err("Missing CONFIG GET key"))?;
            Ok(RespCommand::ConfigCommand(ConfigCommand::Get(key.clone())))
        }
        _ => invalid_data("Unknown CONFIG action"),
    }
}

fn parse_psync(command: Command) -> Result<RespCommand, io::Error> {
    if command.args.len() < 2 {
        return invalid_data("Unknown CONFIG action");
    } else {
        let pos = command.args[1]
            .parse::<i64>()
            .map_err(|_| invalid_data_err("Parsing Error with psync command"))?;
        Ok(RespCommand::PSYNC(command.args[0].clone(), pos))
    }
}

fn invalid_data<T, S: Into<String>>(msg: S) -> Result<T, io::Error> {
    Err(io::Error::new(io::ErrorKind::InvalidData, msg.into()))
}

fn invalid_data_err<S: Into<String>>(msg: S) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn convert_bulk_string(resp_value: Option<Vec<u8>>) -> Result<String, io::Error> {
    if let Some(value) = resp_value {
        let ret = String::from_utf8(value)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))?;
        Ok(ret)
    } else {
        invalid_data("Invalid RespValue")?
    }
}
