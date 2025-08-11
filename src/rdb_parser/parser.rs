use std::{
    collections::{HashMap, HashSet}, fs::File, io::{self, BufRead, BufReader, Read}, path::Path
};

use crate::rdb_parser::{
    config::RdbConfig,
    length_encoded_values::LengthEncodedValue,
    optcode::{RdbOpcode, parse_opcode},
};

#[derive(Debug, Clone)]
pub struct ReturnValue {
    pub db_count: usize,
    pub key_values: HashMap<Vec<u8>, (LengthEncodedValue, String, Option<u64>)>,
}

impl RdbConfig {
    pub fn load(&self) -> io::Result<ReturnValue> {
        let path = Path::new(&self.dir).join(&self.dbfilename);
        if !path.exists() {
            return Ok(ReturnValue { db_count: 1, key_values: HashMap::new()});
        }
        let mut dbs = HashSet::new();
        let mut key_values = HashMap::new();
        let raw = std::fs::read(&path)?;

        eprintln!("--- full RDB dump ({} bytes) ---", raw.len());
        for (i, chunk) in raw.chunks(16).enumerate() {
            // print a hex offset
            eprint!("{:08X}: ", i * 16);
            for byte in chunk {
                eprint!("{:02X} ", byte);
            }
            eprintln!();
        }
        eprintln!("--------------------------------");
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        self.check_header(&mut reader)?;
        let _ = self.get_version(&mut reader)?;
        let mut expiry: Option<u64> = None;

        loop {
            let mut op = [0u8; 1];
            if let Err(e) = reader.read_exact(&mut op) {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    break; // clean EOF
                } else {
                    return Err(e);
                }
            }
            let opcode = op[0];
            let rdb_instruction = parse_opcode(opcode);
            match rdb_instruction {
                RdbOpcode::End => break,
                RdbOpcode::SelectDb => {
                    let db = LengthEncodedValue::parse_length_encoded_int(&mut reader)?;
                    dbs.insert(db);
                }
                RdbOpcode::ResizeDb => {
                    let _size = LengthEncodedValue::parse_length_encoded_int(&mut reader)?;
                    let _expire = LengthEncodedValue::parse_length_encoded_int(&mut reader)?;
                }
                RdbOpcode::Aux => {
                    let _key = LengthEncodedValue::parse_string(&mut reader)?;
                    let _value = LengthEncodedValue::parse_value(&mut reader)?;
                }
                RdbOpcode::KeyValue(type_code) => {
                    let key = LengthEncodedValue::parse_string(&mut reader)?;
                    let value = LengthEncodedValue::parse_value(&mut reader)?;
                    let value_type = match type_code {
                        0x00 => "string",
                        0x01 => "list",
                        0x02 => "set",
                        0x03 => "sorted_set",
                        0x04 => "hash",
                        0x0A => "ziplist",
                        0x0B => "set",
                        0x0D => "hash",

                        _ => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("invalid type {}", type_code),
                            ));
                        }
                    };
                    key_values.insert(key, (value, value_type.to_string(), expiry));
                    expiry = None;
                }
                RdbOpcode::ExpireTimeSec => {
                    let mut secs = [0u8; 4];
                    reader.read_exact(&mut secs)?;
                    expiry = Some(u32::from_be_bytes(secs) as u64 * 1000);
                }
                RdbOpcode::ExpireTimeMs => {
                    let mut ms = [0u8; 8];
                    reader.read_exact(&mut ms)?;
                    expiry = Some(u64::from_le_bytes(ms));
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Unexpected opcode (really a length prefix) 0x{:02X}",
                            opcode
                        ),
                    ));
                }
            }
        }

    

        Ok(ReturnValue {
            db_count: dbs.len(),
            key_values,
        })
    }

    fn check_header(&self, reader: &mut BufReader<File>) -> Result<(), io::Error> {
        let mut buffer = [0u8; 5];
        reader.read_exact(&mut buffer)?;
        if buffer != "REDIS".as_bytes() && buffer != "mySQL".as_bytes() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid Database",
            ));
        }
        Ok(())
    }

    fn get_version(&self, reader: &mut BufReader<File>) -> Result<usize, io::Error> {
        let mut buffer = [0u8; 4];
        reader.read_exact(&mut buffer)?;
        let version_str = std::str::from_utf8(&buffer)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid version bytes"))?;
        // Parse the string to a usize
        let version: usize = version_str
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid version number"))?;
        Ok(version)
    }
}

fn _peek_bytes<R: Read>(reader: &mut BufReader<R>, n: usize) -> std::io::Result<()> {
    let buf = reader.fill_buf()?; // Get a slice to the currently buffered bytes

    let to_show = &buf[..std::cmp::min(n, buf.len())];
    println!("Peeked bytes: {:02X?}", to_show);

    // Do NOT consume yet, so bytes remain in buffer for next reads.

    Ok(())
}

fn _consume_bytes<R: Read>(reader: &mut BufReader<R>, n: usize) {
    // After you're sure you want to move the cursor forward:
    reader.consume(n);
}

