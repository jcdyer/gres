use std::fmt;
use std::io;
use std::str::Utf8Error;
use std::error::Error;

#[derive(Debug)]
pub enum PgError {
    Io(io::Error),
    Utf8(Utf8Error),
    Error(String),
    Unauthenticated,
    Other,
}

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PgError::Io(ref err) => err.fmt(f),
            PgError::Utf8(ref err) => err.fmt(f),
            PgError::Error(ref string) => write!(f, "Error: {:?}", string),
            PgError::Unauthenticated => write!(f, "Unauthenticated"),
            PgError::Other => write!(f, "An unknown error occured"),
        }
    }
}

impl Error for PgError {
    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            PgError::Io(ref err) => Some(err),
            PgError::Utf8(ref err) => Some(err),
            PgError::Error(..) => None,
            PgError::Unauthenticated => None,
            PgError::Other => None,
        }
    }
}

impl From<io::Error> for PgError {
    fn from(err: io::Error) -> PgError {
        PgError::Io(err)
    }
}

impl From<Utf8Error> for PgError {
    fn from(err: Utf8Error) -> PgError {
        PgError::Utf8(err)
    }
}
