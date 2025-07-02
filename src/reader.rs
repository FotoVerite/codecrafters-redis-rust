use std::{
    io::{BufReader, Read},
    iter,
    net::TcpStream,
};

pub struct Reader {
    pub stream: BufReader<TcpStream>, // buffered stream to read from
    pub buffer: Vec<u8>,              // accumulated bytes read but not yet parsed
                                      // maybe cursor or parse state if needed
}

impl Reader {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufReader::new(stream),
            buffer: vec![],
        }
    }

    pub fn fill_buffer(&mut self) -> std::io::Result<usize> {
        // How many bytes to try to read at once (adjust as needed)
        let to_read = 1024;

        // Current length before reading
        let current_len = self.buffer_len();

        // Resize buffer to add space for new bytes
        self.buffer.resize(current_len + to_read, 0);

        // Read into the newly allocated space
        let n = self.stream.read(&mut self.buffer[current_len..])?;

        // Resize buffer again to actual number of bytes read
        self.buffer.truncate(current_len + n);

        Ok(n)
    }

    pub fn read_line(&mut self) -> std::io::Result<Option<String>> {
        if let Some(pos) = self.buffer.windows(2).position(|w| w == b"\r\n") {
            let vec = {
                let slice = self.consume(pos)?;
                self.consume(1)?;
                match slice {
                    Some(s) => s,
                    None => return Ok(None),
                }
            };
            let ret = String::from_utf8(vec)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

            return Ok(Some(ret));
        }
        return Ok(None);
    }

    fn read_bytes(&mut self, n: usize) -> std::io::Result<Option<&[u8]>> {
        let read = &self.buffer[0..n];
        Ok(Some(read))
    }

    fn consume(&mut self, n: usize) -> std::io::Result<Option<Vec<u8>>> {
        let drained: Vec<u8> = self.buffer.drain(0..n).collect();
        Ok(Some(drained))
    }

    fn buffer_len(&self) -> usize {
        self.buffer.len()
    }
}
