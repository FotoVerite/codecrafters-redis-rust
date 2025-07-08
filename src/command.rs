use std::collections::HashMap;

use futures::io;

use crate::resp::{self, RespValue};

#[derive(Debug, Clone)]
pub enum ConfigCommand {
    Get(String),
    Set(String, String),
}
#[derive(Debug, Clone)]
pub enum ReplconfCommand {
    ListeningPort(String),
    Capa(String),
    Getack(String),
    Ack(String),
}

#[derive(Debug, Clone)]
pub enum RespCommand {
    Get(String),
    Ping,
    Echo(String),
    Set {
        key: String,
        value: Vec<u8>,
        px: Option<u64>,
    },
    Type(String),
    ConfigCommand(ConfigCommand),
    Keys(String),
    Info(String),
    ReplconfCommand(ReplconfCommand),
    RDB(Option<Vec<u8>>),
    PSYNC(String, i64),
    Wait(String, String),
    Xadd {
        key: String,
        id: String, // Can be "*" or an explicit "1688512345678-0"
        fields: Vec<(String, String)>,
    },
}

impl RespCommand {
    pub fn to_resp(self) -> RespValue {
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
                    "ping" => Ok(RespCommand::Ping),
                    "echo" => Ok(RespCommand::Echo(command.args[0].clone())),
                    "get" => Ok(RespCommand::Get(command.args[0].clone())),
                    "set" => parse_set(command),
                    "type" => Ok(RespCommand::Type(command.args[0].clone())),
                    "config" => parse_config(command),
                    "keys" => Ok(RespCommand::Keys(command.args[0].clone())),
                    "info" => Ok(RespCommand::Info(command.args[0].clone())),
                    "replconf" => parse_replconf(command),
                    "psync" => parse_psync(command),
                    "wait" => Ok(RespCommand::Wait(
                        command.args[0].clone(),
                        command.args[1].clone(),
                    )),
                    "xadd" => parse_xadd(command),
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

fn parse_xadd(command: Command) -> Result<RespCommand, io::Error> {
    let key = command.args[0].clone();
    let id = command.args[1].clone();
    let mut pairs = command.args.iter().skip(2);
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
            .map_err(|_| invalid_data_err("Parsing Erorr with psync command"))?;
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
