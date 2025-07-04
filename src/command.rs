use futures::io;

use crate::resp::RespValue;

#[derive(Debug)]
pub enum RespCommand {
    Get(String),
    Ping,
    Echo(String),
    Set {
        key: String,
        value: Vec<u8>,
        px: Option<u64>,
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
        let command = match value {
            RespValue::Array(a) => Command::new(a)?,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Expected Top Level Array",
                ))
            }
        };
        return match command.name.to_ascii_lowercase().as_str() {
            "ping" => Ok(RespCommand::Ping),
            "echo" => Ok(RespCommand::Echo(command.args[0].clone())),
            "get" => Ok(RespCommand::Get(command.args[0].clone())),
            "set" => parse_set(command),

            other => invalid_data(format!("Unexpected Command: {}", other)),
        };
    }
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

fn invalid_data<T, S: Into<String>>(msg: S) -> Result<T, io::Error> {
    Err(io::Error::new(io::ErrorKind::InvalidData, msg.into()))
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
