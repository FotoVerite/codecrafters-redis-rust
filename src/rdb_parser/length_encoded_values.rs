use std::{
    fmt,
    io::{self, Read},
};

#[derive(Debug, Clone)]
pub enum LengthEncodedValue {
    String(Vec<u8>),
    Integer(u64),
    // Optionally split by bit width
    // You can extend this for compressed, LZF, etc.
}

impl fmt::Display for LengthEncodedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            LengthEncodedValue::String(_) => "String",
            LengthEncodedValue::Integer(_) => "Int",
        };
        write!(f, "{}", name)
    }
}
pub enum ValueEncoding {
    String(usize),
    Int8,
    Int16,
    Int32,
    CompressedString {
        compressed_len: usize,
        original_len: usize,
    },
}

impl LengthEncodedValue {
    pub fn parse_value<R: Read>(reader: &mut R) -> io::Result<Self> {
        let length = Self::parse_length(reader)?;
        match length {
            ValueEncoding::String(size) => {
                let mut value = vec![0u8; size];
                reader.read_exact(&mut value);
                Ok(LengthEncodedValue::String(value))
            }
            ValueEncoding::Int8 => {
                let mut value = vec![0u8; 1];
                reader.read_exact(&mut value);
                Ok(LengthEncodedValue::Integer(value[0] as u64))
            }
            ValueEncoding::Int16 => {
                let mut buf = [0u8; 2];
                reader.read_exact(&mut buf)?;
                Ok(LengthEncodedValue::Integer(u16::from_be_bytes(buf) as u64))
            }
            ValueEncoding::Int32 => {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                Ok(LengthEncodedValue::Integer(u32::from_be_bytes(buf) as u64))
            }
            _ => {
                return Err(invalid_data_err(&format!("Compressed String")));
            }
        }
    }

    pub fn parse_string<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
        let length = Self::parse_value(reader)?;
        match length {
            LengthEncodedValue::String(value) => Ok(value),
            other => {
                return Err(invalid_data_err(&format!(
                    "Expected String value got {}",
                    other
                )));
            }
        }
    }

    pub fn _parse_int<R: Read>(reader: &mut R) -> io::Result<usize> {
        let length = Self::parse_value(reader)?;
        match length {
            LengthEncodedValue::Integer(int) => Ok(int as usize),
            other => {
                return Err(invalid_data_err(&format!(
                    "Expected Int value got {}",
                    other
                )));
            }
        }
    }
    pub fn parse_length_encoded_int<R: Read>(reader: &mut R) -> io::Result<usize> {
        let length = Self::parse_length(reader)?;
        match length {
            ValueEncoding::String(value) => {
                Ok(value)
            },
            _ => {
                return Err(invalid_data_err(&format!(
                    "Expected int as String Value got int encoding",
                )));
            }
        }
    }

    pub fn parse_length<R: Read>(reader: &mut R) -> io::Result<ValueEncoding> {
        let mut first_byte = [0u8; 1];
        reader.read_exact(&mut first_byte)?;
        let b = first_byte[0];
        return match b {
            0x00..=0x3F => Ok(ValueEncoding::String((b & 0x3F) as usize)),
            0x40..=0x7F => {
                let mut next_byte = [0u8; 1];
                reader.read_exact(&mut next_byte)?;
                let length = ((b & 0x3F) as usize) << 8 | (next_byte[0] as usize);
                Ok(ValueEncoding::String(length))
            }
            0x80..=0xBF => {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                Ok(ValueEncoding::String(u32::from_be_bytes(buf) as usize))
            }
            0xC0 => Ok(ValueEncoding::Int8),
            0xC1 => Ok(ValueEncoding::Int16),
            0xC2 => Ok(ValueEncoding::Int32),
            _ => {
                return Err(invalid_data_err(&format!(
                    "unknown integer encoding prefix: {}",
                    b
                )));
            }
        };
    }
}

fn invalid_data_err<S: Into<String>>(msg: S) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}
