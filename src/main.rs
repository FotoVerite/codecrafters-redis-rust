#![allow(unused_imports)]
mod reader;

use reader::Reader;
use std::{
    io::{BufReader, Result, Write},
    net::{TcpListener, TcpStream},
};
fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    //Uncomment this block to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
               let _ = parse_stream(_stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    fn parse_stream(_stream: TcpStream) -> Result<()> {
        let mut reader = Reader::new(_stream);
        loop {

            if let Some(command) = reader.read_line()? {
                if command.contains("PING") {
                    reader.stream.get_mut().write_all(b"+PONG\r\n")?;
                }
            } else {
                let n = reader.fill_buffer()?;
                if n == 0 {
                    break;
                }
            }
        }
        Ok(())
    }
}
