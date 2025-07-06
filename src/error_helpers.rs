use std::io;

pub(crate) fn invalid_data<T, S: Into<String>>(msg: S) -> Result<T, io::Error> {
    Err(io::Error::new(io::ErrorKind::InvalidData, msg.into()))
}

pub(crate) fn invalid_data_err<S: Into<String>>(msg: S) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}