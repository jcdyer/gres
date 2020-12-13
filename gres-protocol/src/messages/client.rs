use std::io::{Cursor, Write};

pub trait Message {
    fn id(&self) -> Option<u8>;

    fn length(&self) -> usize;

    fn write_body<W: Write>(&self, _writer: &mut W) -> ::std::io::Result<()> {
        Ok(())
    }

    fn get_body(&self) -> Vec<u8> {
        let mut w = Cursor::new(Vec::with_capacity(self.length()));
        self.write_body(&mut w)
            .expect("writing to a Cursor<Vec<u8>> never errors");
        w.into_inner()
    }

    fn to_bytes(&self) -> Vec<u8> {
        let body = self.get_body();
        let length = (body.len() + 4) as u32;
        let length_bytes = length.to_be_bytes();

        let mut bytes = Vec::with_capacity(5 + body.len());
        if let Some(id) = self.id() {
            bytes.push(id);
        }
        bytes.extend(&length_bytes);
        bytes.extend(body);
        bytes
    }
}

// Message types

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupMessage<'a> {
    pub user: &'a str,
    pub database: Option<&'a str>,
    pub params: Vec<(String, String)>,
}

fn write_string<W: Write>(writer: &mut W, s: &str) -> ::std::io::Result<()> {
    writer.write_all(s.as_bytes())?;
    writer.write_all(&[0])?;
    Ok(())
}

impl<'a> Message for StartupMessage<'a> {
    fn id(&self) -> Option<u8> {
        None
    }
    fn length(&self) -> usize {
        4 + 5
            + self.user.len()
            + 1
            + self.database.map(|db| 9 + db.len() + 1).unwrap_or(0)
            + self
                .params
                .iter()
                .map(|(name, value)| name.len() + 1 + value.len() + 1)
                .sum::<usize>()
            + 1
    }

    fn write_body<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&[0, 3, 0, 0])?;
        write_string(writer, "user")?;
        write_string(writer, self.user)?;
        if let Some(db) = self.database {
            write_string(writer, "database")?;
            write_string(writer, db)?;
        }
        for &(ref param, ref value) in &self.params {
            write_string(writer, param)?;
            write_string(writer, value)?;
        }
        writer.write_all(&[0])
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PasswordMessage<'a> {
    pub hash: &'a str,
}

impl<'a> Message for PasswordMessage<'a> {
    fn id(&self) -> Option<u8> {
        Some(b'p')
    }

    fn length(&self) -> usize {
        self.hash.len() + 1
    }

    fn write_body<W: Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
        write_string(writer, self.hash)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Terminate;

impl Message for Terminate {
    fn id(&self) -> Option<u8> {
        Some(b'X')
    }

    fn length(&self) -> usize {
        0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Query {
    pub query: String,
}

impl Message for Query {
    fn id(&self) -> Option<u8> {
        Some(b'Q')
    }

    fn length(&self) -> usize {
        self.query.len() + 1
    }

    fn write_body<W: Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
        write_string(writer, &self.query)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseMessage<'a> {
    pub name: &'a str,
    pub sql: &'a str,
    pub param_types: &'a [i32],
}

impl<'a> Message for ParseMessage<'a> {
    fn id(&self) -> Option<u8> {
        Some(b'P')
    }

    fn length(&self) -> usize {
        self.name.len() + self.sql.len() + 4 * self.param_types.len() + 4
    }

    fn write_body<W: Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
        write_string(writer, self.name)?;
        write_string(writer, self.sql)?;
        writer.write_all(&(self.param_types.len() as u16).to_be_bytes())?;
        for param_type in self.param_types {
            writer.write_all(&param_type.to_be_bytes())?;
        }
        Ok(())
    }
}

#[repr(u16)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Format {
    Text = 0,
    Binary = 1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindMessage<'a> {
    pub portal: &'a str,
    pub prepared_statement: &'a str,
    pub param_format_codes: &'a [Format],
    pub param_values: &'a [Vec<u8>],
    pub result_format_codes: &'a [Format],
}

impl<'a> Message for BindMessage<'a> {
    fn id(&self) -> Option<u8> {
        Some(b'B')
    }
    fn length(&self) -> usize {
        self.portal.len()
            + 1
            + self.prepared_statement.len()
            + 1
            + 2
            + 2 * self.param_format_codes.len()
            + 2
            + self
                .param_values
                .iter()
                .map(|val| 4 + val.len())
                .sum::<usize>()
            + 2
            + 2 * self.result_format_codes.len()
    }

    fn write_body<W: Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
        write_string(writer, self.portal)?;
        write_string(writer, self.prepared_statement)?;

        writer.write_all(&(self.param_format_codes.len() as u16).to_be_bytes())?;
        for code in self.param_format_codes {
            writer.write_all(&(*code as u16).to_be_bytes())?;
        }

        writer.write_all(&(self.param_values.len() as u16).to_be_bytes())?;
        for param in self.param_values {
            writer.write_all(&(param.len() as u32).to_be_bytes())?;
            writer.write_all(param)?;
        }
        writer.write_all(&(self.result_format_codes.len() as u16).to_be_bytes())?;
        for code in self.result_format_codes {
            writer.write_all(&(*code as u16).to_be_bytes())?;
        }

        Ok(())
    }
}

pub struct ExecuteMessage<'a> {
    portal: &'a str,
    max_rows: u32,
}

impl<'a> Message for ExecuteMessage<'a> {
    fn id(&self) -> Option<u8> {
        Some(b'E')
    }

    fn length(&self) -> usize {
        5 + self.portal.len()
    }

    fn write_body<W: Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
        write_string(writer, self.portal)?;
        writer.write_all(&self.max_rows.to_be_bytes())
    }
}

pub struct SyncMessage;

impl Message for SyncMessage {
    fn id(&self) -> Option<u8> {
        Some(b'S')
    }

    fn length(&self) -> usize {
        0
    }

    fn write_body<W>(&self, _writer: &mut W) -> ::std::io::Result<()> {
        Ok(())
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseType {
    PreparedStatement = b'S',
    Portal = b'P',
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloseMessage<'a> {
    pub close_type: CloseType,
    pub name: &'a str,
}

impl<'a> Message for CloseMessage<'a> {
    fn id(&self) -> Option<u8> {
        Some(b'C')
    }

    fn length(&self) -> usize {
        2 + self.name.len()
    }

    fn write_body<W: Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
        writer.write_all(&[self.close_type as u8])?;
        write_string(writer, self.name)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminate() {
        assert_eq!(Terminate.to_bytes(), b"\x58\0\0\0\x04".to_vec());
    }

    #[test]
    fn test_startup_message() {
        let msg = StartupMessage {
            user: "cliff",
            database: None,
            params: vec![
                ("name".to_string(), "Theseus".to_string()),
                ("vessel".to_string(), "ship".to_string()),
            ],
        };
        assert_eq!(
            msg.to_bytes(),
            b"\0\0\0\x2d\x00\x03\x00\x00user\0cliff\0name\0Theseus\0vessel\0ship\0\0".to_vec()
        );
    }

    #[test]
    fn test_password_message() {
        let msg = PasswordMessage {
            hash: "open sesame",
        };
        assert_eq!(msg.to_bytes(), b"p\0\0\0\x10open sesame\0".to_vec());
    }

    #[test]
    fn test_query_message() {
        let msg = Query {
            query: "SELECT 1".to_string(),
        };
        assert_eq!(msg.to_bytes(), b"Q\0\0\0\x0dSELECT 1\0".to_vec());
    }
}
