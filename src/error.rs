use std::{fmt, num::ParseIntError};
use std::io;
use std::str::Utf8Error;
use std::error::Error;

use gres_protocol::ProtocolError;

#[derive(Debug)]
pub enum PgError {
    Io(io::Error),
    Utf8(Utf8Error),
    IntParse(ParseIntError),
    ProtocolError(ProtocolError),
    Error(String),
    //ServerError(NoticeBody),
    Unauthenticated,
    Other,
}

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PgError::Io(ref err) => err.fmt(f),
            PgError::Utf8(ref err) => err.fmt(f),
            PgError::IntParse(ref err) => err.fmt(f),
            PgError::ProtocolError(ref err) => err.fmt(f),
            PgError::Error(ref string) => write!(f, "Error: {}", string),
            // PgError::ServerError(err) => write!(f, "ServerError: {:?}", err),
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
            PgError::IntParse(ref err) => Some(err),
            PgError::ProtocolError(ref err) => Some(err),
            PgError::Error(..) => None,
            // PgError::ServerError(err) => None,
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
impl From<ParseIntError> for PgError {
    fn from(err: ParseIntError) -> PgError {
        PgError::IntParse(err)
    }
}

impl From<ProtocolError> for PgError {
    fn from(err: ProtocolError) -> PgError {
        PgError::ProtocolError(err)
    }

}
/* impl From<NoticeBody<'a>> for PgError {
    fn from(err: NoticeBody<'a>) -> PgError {
        PgError::ServerError(err.make_static())
    }
}*/