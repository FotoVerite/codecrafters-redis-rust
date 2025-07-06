use std::{
    fs::File,
    io::{self, BufRead, BufReader, Read},
    path::Path,
};

use crate::rdb::{config::RdbConfig, length_encoded_values::LengthEncodedValue};

impl RdbConfig {
    pub fn load(&self) -> Result<Vec<(String, Vec<u8>)>, io::Error> {
        let mut ret = vec![];
        let path = Path::new(&self.dir).join(&self.dbfilename);
        if !path.exists() {
            return Ok(vec![]);
        }
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

            match opcode {
                0xFF => {
                    break
                }
                0xFA => {
                    // AUX field
                    let _key = LengthEncodedValue::from_reader(&mut reader)?;
                    let _value = LengthEncodedValue::from_reader(&mut reader)?;
                    // maybe log or ignore these
                }
                0xFE => {
                    // SELECT DB opcode
                    let _ = LengthEncodedValue::from_reader(&mut reader)?;
                    // Optionally extract integer or string (it should be integer)

                    // skip or store db_num
                }
                0xFD => {
                    // EXPIRE: 4‑byte seconds (big endian)
                    let mut secs = [0u8; 4];
                    reader.read_exact(&mut secs)?;
                }
                0xFC => {
                    // PEXPIRE: 8‑byte milliseconds (big endian)
                    let mut ms = [0u8; 8];
                    reader.read_exact(&mut ms)?;
                }
                0xFB => {
                    let mut next_byte = [0u8; 2];
                    reader.read_exact(&mut next_byte)?;
                }

                0x00 => {
                    let key = {
                        match self.decode_string(&mut reader)? {
                            LengthEncodedValue::String(string) => string,
                            LengthEncodedValue::Integer(integer) => integer.to_string(),
                        }
                    };
                    let value = {
                        match self.decode_string(&mut reader)? {
                            LengthEncodedValue::String(string) => string.into_bytes(),
                            LengthEncodedValue::Integer(integer) => {
                                integer.to_string().into_bytes()
                            }
                        }
                    };
                    ret.push((key, value))
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
        Ok(ret)
    }

    fn check_header(&self, reader: &mut BufReader<File>) -> Result<(), io::Error> {
        let mut buffer = [0u8; 5];
        reader.read_exact(&mut buffer)?;
        if buffer != "REDIS".as_bytes() {
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

    fn decode_string(&self, reader: &mut BufReader<File>) -> Result<LengthEncodedValue, io::Error> {
        LengthEncodedValue::from_reader(reader)
    }
}

fn peek_bytes<R: Read>(reader: &mut BufReader<R>, n: usize) -> std::io::Result<()> {
    let buf = reader.fill_buf()?; // Get a slice to the currently buffered bytes

    let to_show = &buf[..std::cmp::min(n, buf.len())];
    println!("Peeked bytes: {:02X?}", to_show);

    // Do NOT consume yet, so bytes remain in buffer for next reads.

    Ok(())
}

fn consume_bytes<R: Read>(reader: &mut BufReader<R>, n: usize) {
    // After you're sure you want to move the cursor forward:
    reader.consume(n);
}
