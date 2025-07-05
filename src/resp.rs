use bytes::{BufMut, BytesMut};
use std::{fmt::Write, io};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>), // None = $-1
    Array(Vec<RespValue>),
}

pub struct RespCodec;

impl Decoder for RespCodec {
    type Item = RespValue; // You can use a custom enum if you want structured RESP commands
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Check if we have a full line ending in \r\n
        if src.is_empty() {
            return Ok(None);
        }
        if let Some(chr) = src.get(0) {
            match chr {
                b'+' => return simple_string(src),
                b'-' => return error_string(src),

                b':' => return int_string(src),

                b'$' => return bulk_string(src),
                b'*' => return self.parse_array(src),

                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Unknown RESP type",
                    ))
                }
            }
        }
        // if let Some(pos) = src.iter().position(|w| w == b"\r\n") {
        //     let line = src.split_to(pos + 2);
        //     let line_str = std::str::from_utf8(&line[..line.len() - 2])
        //         .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))?;
        //     return Ok(Some(line_str.to_string()));
        // }
        Ok(None)
    }
}

fn slice_utf8(data: &[u8]) -> Result<&str, io::Error> {
    str::from_utf8(data).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))
}

fn parse_integer(data: &str) -> Result<i64, io::Error> {
    data.parse::<i64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))
}

fn parse_resp_line(src: &mut BytesMut) -> Result<Option<String>, io::Error> {
    if let Some(pos) = src.windows(2).position(|s| s == b"\r\n") {
        let line = src.split_to(pos + 2);
        let line_str = slice_utf8(&line[1..pos])?;
        return Ok(Some(line_str.to_string()));
    }
    Ok(None)
}

fn digest_stream(src: &mut BytesMut, pos: usize) -> Result<Option<Vec<u8>>, io::Error> {
    if src.len() < pos + 2 {
        return Ok(None);
    }
    let line = src.split_to(pos);
    let crlf = src.split_to(2);
    if &crlf[..] != b"\r\n" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Expected CRLF after bulk string",
        ));
    }
    let vec = line.to_vec();
    return Ok(Some(vec));
}
fn simple_string(src: &mut BytesMut) -> Result<Option<RespValue>, io::Error> {
    if let Some(string) = parse_resp_line(src)? {
        let resp: RespValue = RespValue::SimpleString(string);
        return Ok(Some(resp));
    }
    Ok(None)
}

fn error_string(src: &mut BytesMut) -> Result<Option<RespValue>, io::Error> {
    if let Some(string) = parse_resp_line(src)? {
        let resp: RespValue = RespValue::Error(string);
        return Ok(Some(resp));
    }

    Ok(None)
}

fn int_string(src: &mut BytesMut) -> Result<Option<RespValue>, io::Error> {
    if let Some(string) = parse_resp_line(src)? {
        let integer = parse_integer(string.as_str())?;
        let resp = RespValue::Integer(integer);
        return Ok(Some(resp));
    }

    Ok(None)
}

fn bulk_string(src: &mut BytesMut) -> Result<Option<RespValue>, io::Error> {
    if let Some(bytes_string) = parse_resp_line(src)? {
        let bytes = parse_integer(bytes_string.as_str())?;
        if bytes == -1 {
            return Ok(Some(RespValue::BulkString(None)));
        }
        if let Some(bulk) = digest_stream(src, bytes as usize)? {
            let resp = RespValue::BulkString(Some(bulk));
            return Ok(Some(resp));
        }
    }
    Ok(None)
}

impl RespCodec {
    pub fn parse_array(&mut self, src: &mut BytesMut) -> Result<Option<RespValue>, io::Error> {
        if let Some(size_string) = parse_resp_line(src)? {
            let size = parse_integer(size_string.as_str())?;
            let mut ret = Vec::with_capacity(size as usize);
            for _ in 0..size {
                if let Some(val) = self.decode(src)? {
                    ret.push(val);
                } else {
                    return Ok(None);
                }
            }
            return Ok(Some(RespValue::Array(ret)));
        }
        Ok(None)
    }

    pub fn write_array(&mut self, dst: &mut BytesMut, values: Vec<RespValue>) -> Result<(), io::Error> {
        dst.put_u8(b'*');
        dst.extend_from_slice(format!("{}\r\n", values.len()).as_bytes());
        for value in values {
            self.encode(value, dst)?
        }
        Ok(())
    }
}

impl Encoder<RespValue> for RespCodec {
    type Error = io::Error;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RespValue::SimpleString(s) => write_line(dst, b'+', &s),
            RespValue::Error(e) => write_line(dst, b'-', &e),
            RespValue::Integer(i) => write_line(dst, b':', (i.to_string()).as_str()),
            RespValue::BulkString(c) => write_bulk_string(dst, c),
            RespValue::Array(values) => self.write_array(dst, values),
        }
    }
}

fn write_line(dst: &mut BytesMut, prefix: u8, content: &str) -> Result<(), io::Error> {
    dst.put_u8(prefix);
    dst.extend_from_slice(content.as_bytes());
    dst.extend_from_slice(b"\r\n");
    Ok(())
}

fn write_bulk_string(dst: &mut BytesMut, option: Option<Vec<u8>>) -> Result<(), io::Error> {
    match option {
        Some(data) => {
            // Write: $<length>\r\n<data>\r\n
            let len = data.len();
            dst.extend_from_slice(format!("${}\r\n", len).as_bytes());
            dst.extend_from_slice(&data);
            dst.extend_from_slice(b"\r\n");
        }
        None => {
            // Write: $-1\r\n
            dst.extend_from_slice(b"$-1\r\n");
        }
    }
    Ok(())
}
