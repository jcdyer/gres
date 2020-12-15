extern crate crypto;
use std::result;
pub use connection::Connection;

pub mod auth;
pub mod connection;
pub mod error;

pub type Result<T> = result::Result<T, error::PgError>;
