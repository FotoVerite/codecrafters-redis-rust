use std::{fs::File, io::{self, BufRead, BufReader, Read}};

pub enum LengthEncodedValue {
    String(String),
    Integer(u64),
}

impl LengthEncodedValue {
    pub fn from_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut optcode = [0u8; 1];
        reader.read_exact(&mut optcode)?;

        match optcode[0] >> 6 {
            0b00 => {
                let length = (optcode[0] & 0x3F) as usize;
                Ok(LengthEncodedValue::String(read_string(reader, length)?))
            }
            0b01 => {
                let mut next_byte = [0u8; 1];
                reader.read_exact(&mut next_byte)?;
                let length = (((optcode[0] & 0x3F) as usize) << 8) | (next_byte[0] as usize);
                Ok(LengthEncodedValue::String(read_string(reader, length)?))
            }
            0b10 => {
                // 32-bit length, need 4 more bytes
                let mut next_bytes = [0u8; 4];
                reader.read_exact(&mut next_bytes)?;
                let length = u32::from_be_bytes(next_bytes) as usize;
                Ok(LengthEncodedValue::String(read_string(reader, length)?))
            }
            0b11 => {
                // special encoding (integer, compressed) — can error or skip for now
                let prefix = (optcode[0] & 0x3F) as usize;
                Ok(LengthEncodedValue::Integer(read_integer(reader, prefix)?))
            }
            _ => unreachable!(),
        }
    }
}

fn read_string<R: Read>(reader: &mut R, length: usize) -> Result<String, io::Error> {
    let mut string_buffer = vec![0u8; length];
    reader.read_exact(&mut string_buffer)?;
    String::from_utf8(string_buffer).map_err(|_| invalid_data_err("Invalid String"))
}

fn read_integer<R: Read>(reader: &mut R, prefix: usize) -> Result<u64, io::Error> {
    match prefix {
        0 => {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf)?;
            Ok(buf[0] as u64)
        },
        1 => {
            let mut buf = [0u8; 2];
            reader.read_exact(&mut buf)?;
            Ok(u16::from_le_bytes(buf) as u64)
        }
        2 => {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            Ok(u32::from_le_bytes(buf) as u64)
        },
        3 => {
            // Implement compressed string (or stub)
            // e.g. read length then read compressed data, decompress here
            unimplemented!("Compressed string decoding not implemented");
        }
        _ => Err(invalid_data_err(&format!("unknown integer encoding prefix: {}", prefix))),
    }
}

fn invalid_data_err<S: Into<String>>(msg: S) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}
