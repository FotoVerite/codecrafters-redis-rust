pub enum RdbOpcode {
    SelectDb,
    ResizeDb,
    Aux,
    ExpireTimeMs,
    ExpireTimeSec,
    KeyValue(u8), // generic, maybe further specify by type
    End,
    Unknown,
}

pub fn parse_opcode(opcode: u8) -> RdbOpcode {
    match opcode {
        0x00..=0x04 => RdbOpcode::KeyValue(opcode),
        0x09..=0x0D => RdbOpcode::KeyValue(opcode),
        0xFA => RdbOpcode::Aux,
        0xFB => RdbOpcode::ResizeDb,
        0xFC => RdbOpcode::ExpireTimeMs,
        0xFD => RdbOpcode::ExpireTimeSec,
        0xFE => RdbOpcode::SelectDb,
        0xFF => RdbOpcode::End,
        _ => RdbOpcode::Unknown,
    }
}

// loop {
//
//
//         0xFD => {
//             // EXPIRE: 4‑byte seconds (big endian)
//             let mut secs = [0u8; 4];
//             reader.read_exact(&mut secs)?;
//             eprintln!("EXPIRETIME_SEC raw bytes: {:02X?}", secs); // Debug output

//             expiry = Some(u32::from_be_bytes(secs) as u64 * 1000);
//             continue;
//         }
//         0xFC => {
//             // PEXPIRE: 8‑byte milliseconds (big endian)
//             let mut ms = [0u8; 8];
//             reader.read_exact(&mut ms)?;
//             eprintln!("EXPIRETIME_MS raw bytes: {:02X?}", ms); // Debug output

//             expiry = Some(u64::from_le_bytes(ms));
//             continue;
//         }
//         0xFB => {
//             dbg!("FB");
//             let mut next_byte = [0u8; 2];
//             reader.read_exact(&mut next_byte)?;
//         }

//         0x00 | 0x01 | 0x02 | 0x03 | 0x04 => {
//             dbg!("key");
//             let key = {
//                 match self.decode_string(&mut reader)? {
//                     LengthEncodedValue::String(string) => string,
//                     LengthEncodedValue::Integer(integer) => integer.to_le_bytes().to_vec(),
//                 }
//             };
//             dbg!("value");

//             let value = match opcode {
//                 0x00 => match self.decode_string(&mut reader)? {
//                     LengthEncodedValue::String(s) => s,
//                     LengthEncodedValue::Integer(i) => i.to_le_bytes().to_vec(),
//                 },
//                 _ => {
//                     eprintln!(
//                         "Skipping unimplemented value type 0x{:02X} for key {:?}",
//                         opcode, key
//                     );
//                     vec![]
//                 }
//             };
//             eprintln!("key: {:?}", key);
//             eprintln!("expiry: {:?}", expiry);

//             let now = SystemTime::now()
//                 .duration_since(UNIX_EPOCH)
//                 .expect("Time went backwards")
//                 .as_millis() as u64;
//             if (expiry.is_some() && now < expiry.unwrap()) || expiry.is_none() {
//                 ret.push((key, value, expiry));
//             }
//             expiry = None;
//         }
//         _ => {
//             return Err(io::Error::new(
//                 io::ErrorKind::InvalidData,
//                 format!(
//                     "Unexpected opcode (really a length prefix) 0x{:02X}",
//                     opcode
//                 ),
//             ));
//         }
//     }
// }
