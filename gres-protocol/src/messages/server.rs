use self::erg::*;
use crate::ProtocolError;
use crate::Result;
use std::{collections::HashMap, str::from_utf8};


#[derive(Debug, PartialEq)]
pub enum ServerMsg<'a> {
    ErrorResponse(NoticeBody<'a>),
    NoticeResponse(NoticeBody<'a>),
    Auth(AuthMsg<'a>),
    ReadyForQuery,
    CommandComplete(&'a str),
    ParamStatus(&'a str, &'a str),
    BackendKeyData(u32, u32),
    RowDescription(Vec<FieldDescription<'a>>), // TBD
    DataRow(Vec<&'a str>),                     // TBD
    Unknown(&'a str, &'a [u8]),                // TBD
    ParseComplete,
    BindComplete,
    CloseComplete,
}

impl<'a> ServerMsg<'a> {
    pub fn from_slice(message: &[u8]) -> Result<ServerMsg> {
        let length = 1 + slice_to_u32(&message[1..5]) as usize;
        if message.len() != length {
            return Err(ProtocolError::Error(format!(
                "Wrong length for message.  Expected {}.  Found {}.",
                length,
                message.len()
            )));
        }
        let identifier = from_utf8(&message[..1])?;
        let (_, extra) = message.split_at(5);
        match identifier {
            "I" => {
                if extra.is_empty() {
                    Ok(ServerMsg::ParseComplete)
                } else {
                    Err(ProtocolError::Error(format!(
                        "Extra value after param status: {:?}",
                        extra
                    )))
                }
            }
            "R" => AuthMsg::from_slice(extra).map(ServerMsg::Auth),
            "S" => {
                // Parameter Status
                let mut param_iter = extra.split(|c| c == &0); // split on nulls
                let name = from_utf8(param_iter.next().unwrap())?;
                let value = from_utf8(param_iter.next().unwrap())?;
                let nothing = param_iter.next().unwrap(); // The second null is the terminator
                if !nothing.is_empty() {
                    Err(ProtocolError::Error(format!(
                        "Extra value after param status: {:?}",
                        nothing
                    )))
                } else {
                    Ok(ServerMsg::ParamStatus(name, value))
                }
            }
            "K" => {
                // BackendKeyData
                let pid = slice_to_u32(&extra[..4]);
                let key = slice_to_u32(&extra[4..]);
                Ok(ServerMsg::BackendKeyData(pid, key))
            }
            "T" => {
                println!("{:?}", extra);
                // Row Description
                let field_count = slice_to_u16(&extra[..2]);
                let mut extra = &extra[2..];
                let mut fields = vec![];

                for _ in 0..field_count {
                    let (name, bytes, rem) = FieldDescription::take_field(extra).unwrap();
                    let fd = FieldDescription::new(name, bytes).unwrap();
                    fields.push(fd);
                    extra = rem;
                }
                if extra == &b""[..] {
                    Ok(ServerMsg::RowDescription(fields))
                } else {
                    Err(ProtocolError::Error(format!(
                        "Unexpected extra data in row description: {:?}",
                        extra
                    )))
                }
            }
            "D" => {
                // Data Row
                let field_count = slice_to_u16(&extra[..2]);
                let mut extra = &extra[2..];
                let mut fields = vec![];
                for _ in 0..field_count {
                    let (string, more) = take_sized_string(extra).unwrap();
                    fields.push(string);
                    extra = more;
                }
                if extra == &b""[..] {
                    Ok(ServerMsg::DataRow(fields))
                } else {
                    Err(ProtocolError::Error(format!(
                        "Unexpected extra data in data row: {:?}",
                        extra
                    )))
                }
            }
            "C" => {
                // Command Complete
                let (command_tag, _, extra) = take_cstring_plus_fixed(extra, 0).unwrap();
                if extra == &b""[..] {
                    Ok(ServerMsg::CommandComplete(command_tag))
                } else {
                    Err(ProtocolError::Error(format!(
                        "Unexpected extra data in command complate: {:?}",
                        extra
                    )))
                }
            }
            "Z" => {
                // ReadyForQuery
                Ok(ServerMsg::ReadyForQuery)
            }
            "N" => {
                // NoticeResponse

                Ok(ServerMsg::NoticeResponse(NoticeBody::from_bytes(extra)?))
            }
            "E" => {
                // ErrorResponse
                Ok(ServerMsg::ErrorResponse(NoticeBody::from_bytes(extra)?))
            }
            "1" => {
                // ParseComplete
                if extra.is_empty() {
                    Ok(ServerMsg::ParseComplete)
                } else {
                    Err(ProtocolError::Error(format!("Extra data: {:?}", extra)))
                }
            }
            "2" => {
                // BindComplete
                if extra.is_empty() {
                    Ok(ServerMsg::BindComplete)
                } else {
                    Err(ProtocolError::Error(format!("Extra data: {:?}", extra)))
                }
            }
            "3" => {
                // CloseComplete
                if extra.is_empty() {
                    Ok(ServerMsg::CloseComplete)
                } else {
                    Err(ProtocolError::Error(format!("Extra data: {:?}", extra)))
                }
            }
            _ => Ok(ServerMsg::Unknown(identifier, extra)),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum AuthMsg<'a> {
    Ok,
    Kerberos,
    Cleartext,
    Md5(&'a [u8]),
    ScmCredential,
    Gss,
    Sspi,
    GssContinue(&'a [u8]),
    Unknown,
}

impl<'a> AuthMsg<'a> {
    pub fn from_slice(extra: &'a [u8]) -> Result<AuthMsg> {
        match slice_to_u32(&extra[0..4]) {
            0 => Ok(AuthMsg::Ok),
            2 => Ok(AuthMsg::Kerberos),
            3 => Ok(AuthMsg::Cleartext),
            5 => {
                let salt = &extra[4..8];
                Ok(AuthMsg::Md5(salt))
            }
            6 => Ok(AuthMsg::ScmCredential),
            7 => Ok(AuthMsg::Gss),
            8 => {
                let gss_data = &extra[4..8];
                Ok(AuthMsg::GssContinue(gss_data))
            }
            9 => Ok(AuthMsg::Sspi),
            1 | 4 | 10..=255 => Ok(AuthMsg::Unknown),
            _ => Err(ProtocolError::Other),
        }
    }
}

pub fn take_msg(input: &[u8]) -> Result<(&[u8], &[u8])> {
    if input.len() < 5 {
        Err(ProtocolError::Error(format!("Input too short: {:?}", input)))
    } else {
        let length = 1 + slice_to_u32(&input[1..5]) as usize;
        if input.len() < length {
            Err(ProtocolError::Error(format!("Message too short: {:?}", input)))
        } else {
            Ok(input.split_at(length))
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FieldFormat {
    Text,
    Binary,
}

mod erg {
    use crate::{ProtocolError, Result};
    use std::{convert::TryInto, str::from_utf8};

    pub fn find_first<T: Eq>(input: &[T], matched: &T) -> Option<usize> {
        input.iter().position(|val| val == matched)
    }

    pub fn slice_to_u32(input: &[u8]) -> u32 {
        u32::from_be_bytes(input.try_into().expect("expected four bytes"))
    }

    pub fn slice_to_u16(input: &[u8]) -> u16 {
        u16::from_be_bytes(input.try_into().expect("expected two bytes"))
    }

    pub fn take_cstring_plus_fixed<'a>(
        input: &'a [u8],
        fixed: usize,
    ) -> Result<(&'a str, &'a [u8], &'a [u8])> {
        let strlen = find_first(input, &0);
        match strlen {
            Some(strlen) => {
                let string = from_utf8(&input[..strlen])?;
                let fixed_data = &input[strlen + 1..strlen + 1 + fixed];
                let extra = &input[strlen + 1 + fixed..];
                Ok((string, fixed_data, extra))
            }
            None => Err(ProtocolError::Error("null byte not found".to_string())),
        }
    }
    pub fn take_sized_string<'a>(input: &'a [u8]) -> Result<(&'a str, &'a [u8])> {
        let size = slice_to_u32(&input[..4]) as usize;
        let data = from_utf8(&input[4..4 + size])?;
        let extra = &input[4 + size..];
        Ok((data, extra))
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct FieldDescription<'a> {
    field_name: &'a str,
    format: FieldFormat,
}

impl<'a> FieldDescription<'a> {
    fn take_field(input: &'a [u8]) -> Result<(&'a str, &'a [u8], &'a [u8])> {
        take_cstring_plus_fixed(input, 18)
    }

    fn new(name: &'a str, fixed_data: &'a [u8]) -> Result<FieldDescription<'a>> {
        let format = match slice_to_u16(&fixed_data[16..18]) {
            0 => FieldFormat::Text,
            1 => FieldFormat::Binary,
            _ => return Err(ProtocolError::Error("Invalid field format".to_string())),
        };
        Ok(FieldDescription {
            field_name: name,
            format,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Severity {
    Error,
    Fatal,
    Panic,
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

impl Severity {
    fn new(s: &str) -> Option<Severity> {
        Some(match s {
            "ERROR" => Severity::Error,
            "FATAL" => Severity::Fatal,
            "PANIC" => Severity::Panic,
            "WARNING" => Severity::Warning,
            "NOTICE" => Severity::Notice,
            "DEBUG" => Severity::Debug,
            "INFO" => Severity::Info,
            "LOG" => Severity::Log,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Position<'a> {
    Public(usize),
    Internal { position: usize, query: &'a str },
}

impl<'a> Position<'a> {
    pub fn position(&self) -> usize {
        use Position::*;
        match self {
            Public(pos) => *pos,
            Internal { position, .. } => *position,
        }
    }

    pub fn query(&self) -> Option<&str> {
        match self {
            Position::Internal { query, .. } => Some(*query),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct NoticeBody<'a> {
    severity_loc: &'a str,
    severity: Option<Severity>,
    code: &'a str,
    message: &'a str,
    detail: Option<&'a str>,
    hint: Option<&'a str>,
    position: Option<Position<'a>>,
    more: Vec<(char, &'a str)>,
}

impl<'a> NoticeBody<'a> {
    fn from_bytes(mut bytes: &'a [u8]) -> Result<NoticeBody<'a>> {
        if bytes.is_empty() {
            return Err(ProtocolError::Error(format!("No terminator in {:?}", bytes)));
        }

        let mut parts: HashMap<char, &str> = HashMap::new();
        let mut more = Vec::new();
        while bytes.get(0) != Some(&0) {
            let indicator = bytes[0].into();
            let (msg, _, end) = take_cstring_plus_fixed(&bytes[1..], 0).unwrap();
            if ['S', 'V', 'C', 'M', 'D', 'H', 'P', 'p', 'q'].contains(&indicator) {
                parts.insert(indicator, msg);
            } else {
                more.push((indicator, msg))
            }
            bytes = end;
            if bytes.is_empty() {
                return Err(ProtocolError::Error(format!("No terminator in {:?}", bytes)));
            }
        }

        let position: Option<Position> = if let Some(pos) = parts.get(&'P') {
            Some(Position::Public(pos.parse()?))
        } else if let Some(pos) = parts.get(&'p') {
            Some(Position::Internal {
                position: pos.parse()?,
                query: parts.get(&'q').copied().unwrap_or_default(),
            })
        } else {
            None
        };

        Ok(NoticeBody {
            severity_loc: parts[&'S'],
            severity: parts.get(&'V').copied().and_then(Severity::new),
            code: parts[&'C'],
            message: parts[&'M'],
            detail: parts.get(&'D').copied(),
            hint: parts.get(&'H').copied(),
            position,
            more,
        })
    }

    pub fn message(&self) -> &str {
        self.message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_take_msg() {
        let buffer = [69, 0, 0, 0, 5, 1, 72, 0, 0, 0, 12, 1, 2, 3, 4, 5, 6, 7, 8];
        let (first, rest) = take_msg(&buffer).unwrap();
        assert_eq!(first, [69, 0, 0, 0, 5, 1]);
        let (second, nothing) = take_msg(rest).unwrap();
        assert_eq!(second, [72, 0, 0, 0, 12, 1, 2, 3, 4, 5, 6, 7, 8]);
        assert!(nothing.is_empty());
    }

    #[test]
    fn test_server_startup_response_parsing() {
        let buffer = b"R\x00\x00\x00\x08\x00\x00\x00\x00S\x00\x00\x00\x16application_name\x00\x00S\x00\x00\x00\x19client_encoding\x00UTF8\x00S\x00\x00\x00\x17DateStyle\x00ISO, MDY\x00S\x00\x00\x00\x19integer_datetimes\x00on\x00S\x00\x00\x00\x1bIntervalStyle\x00postgres\x00S\x00\x00\x00\x15is_superuser\x00off\x00S\x00\x00\x00\x19server_encoding\x00UTF8\x00S\x00\x00\x00\x19server_version\x009.6.1\x00S\x00\x00\x00 session_authorization\x00cliff\x00S\x00\x00\x00#standard_conforming_strings\x00on\x00S\x00\x00\x00\x18TimeZone\x00US/Eastern\x00K\x00\x00\x00\x0c\x00\x00\x17\xbb\x15b\xfb1Z\x00\x00\x00\x05I";
        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::Auth(AuthMsg::Ok));
        assert_eq!(buffer.len(), 314);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::ParamStatus("application_name", ""));
        assert_eq!(buffer.len(), 291);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::ParamStatus("client_encoding", "UTF8"));
        assert_eq!(buffer.len(), 265);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::ParamStatus("DateStyle", "ISO, MDY"));
        assert_eq!(buffer.len(), 241);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::ParamStatus("integer_datetimes", "on"));
        assert_eq!(buffer.len(), 215);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::ParamStatus("IntervalStyle", "postgres"));
        assert_eq!(buffer.len(), 187);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::ParamStatus("is_superuser", "off"));
        assert_eq!(buffer.len(), 165);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::ParamStatus("server_encoding", "UTF8"));
        assert_eq!(buffer.len(), 139);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::ParamStatus("server_version", "9.6.1"));
        assert_eq!(buffer.len(), 113);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(
            msg,
            ServerMsg::ParamStatus("session_authorization", "cliff")
        );
        assert_eq!(buffer.len(), 80);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(
            msg,
            ServerMsg::ParamStatus("standard_conforming_strings", "on")
        );
        assert_eq!(buffer.len(), 44);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::ParamStatus("TimeZone", "US/Eastern"));
        assert_eq!(buffer.len(), 19);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert!(matches!(msg, ServerMsg::BackendKeyData(..)));
        assert_eq!(buffer.len(), 6);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::ReadyForQuery);
        assert_eq!(buffer.len(), 0);

        assert!(take_msg(buffer).is_err())
    }

    #[test]
    fn test_server_query_response_parsing() {
        let buffer = b"T\x00\x00\x00 \x00\x01version\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x19\xff\xff\xff\xff\xff\xff\x00\x00D\x00\x00\x00_\x00\x01\x00\x00\x00UPostgreSQL 9.6.1 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 6.2.1 20160830, 64-bitC\x00\x00\x00\rSELECT 1\x00Z\x00\x00\x00\x05I";

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(
            msg,
            ServerMsg::RowDescription(vec![FieldDescription {
                field_name: "version",
                format: FieldFormat::Text,
            }])
        );
        assert_eq!(buffer.len(), 116);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::DataRow(vec!["PostgreSQL 9.6.1 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 6.2.1 20160830, 64-bit"]));
        assert_eq!(buffer.len(), 20);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::CommandComplete("SELECT 1"));
        assert_eq!(buffer.len(), 6);

        let (next, buffer) = take_msg(buffer).unwrap();
        let msg = ServerMsg::from_slice(next).unwrap();
        assert_eq!(msg, ServerMsg::ReadyForQuery);
        assert_eq!(buffer.len(), 0);
    }
}
