use std::{num::ParseIntError, str::Utf8Error};

pub mod messages;

#[derive(Clone, Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("error converting utf8 data: {0}")]
    Utf8(#[from] Utf8Error),
    #[error("error parsing integer: {0}")]
    ParseInt(#[from] ParseIntError),
    #[error("error occurred: {0}")]
    Error(String),
    #[error("unknown error")]
    Other,
}


pub type Result<T, E = ProtocolError> = ::std::result::Result<T, E>;
