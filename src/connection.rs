use std::collections::vec_deque::VecDeque;
use std::io::{Read, Write};
use std::net;
use std::time::Duration;
use Result;
use error::PgError;
use message::{Message, StartupMessage, Query};
use servermsg::{take_msg, ServerMsg, AuthMsg};

#[derive(Debug, Eq, PartialEq, Clone)]
enum ConnectionState {
    New, 
    AwaitingAuthResponse, 
    Authenticated,
    AuthenticationRejected,
    ReadyForQuery,
    AwaitingQueryResponse,
    AwaitingDataRows,
    Closed,
}
    

#[derive(Debug)]
pub struct Connection {
    user: String,
    database: String,
    host: String,
    port: u16,
    socket: net::TcpStream,
    state: ConnectionState,
}

impl Connection {
    fn initiate_connection(&mut self) -> Result<()> {
        let startup = StartupMessage {
            user: &self.user,
            database: Some(&self.database),
            params: vec!(),
        };
        let bytes_to_send = startup.to_bytes();
        try!(self.socket.write_all(&bytes_to_send)); 
        self.state = ConnectionState::AwaitingAuthResponse;
        Ok(())
    }

    fn handle_startup(&mut self) -> Result<()> {
        let mut buf = Vec::with_capacity(1024);
        let mut message_queue = VecDeque::new();
        try!(self.read_from_socket(&mut buf));
        let mut remainder = &buf[..];
        while remainder.len() > 0 {
            let (bytes, excess) = try!(take_msg(remainder));
            let msg = try!(ServerMsg::from_slice(bytes));
            message_queue.push_back(msg);
            remainder = excess;
        }
        println!("VecDeque: {:?}", message_queue);
        loop {
            match self.state.clone() {
                ConnectionState::AwaitingAuthResponse => try!(self.handle_auth(&mut message_queue)),
                ConnectionState::AuthenticationRejected => break,
                ConnectionState::Authenticated => try!(self.handle_server_info(&mut message_queue)),
                ConnectionState::ReadyForQuery => {
                    try!(self.handle_ready_for_query(&mut message_queue));
                    break;
                },
                state => return Err(PgError::Error(format!("Unhandled state: {:?}", state))),
            }
        } 
        Ok(())
    }

    fn handle_auth<'a>(&mut self, message_queue: &mut VecDeque<ServerMsg<'a>>) -> Result<()> {
        match message_queue.pop_front() {
            Some(ServerMsg::Auth(AuthMsg::Ok)) => {
                self.state = ConnectionState::Authenticated;
                Ok(())
            },
            Some(ServerMsg::Auth(AuthMsg::Md5(salt))) => {
                Ok(())
            },
            Some(ServerMsg::Auth(method)) => {
                Err(PgError::Error(format!("Unimplemented authentication method, {:?}", method)))
            },
            Some(ServerMsg::ErrorResponse(err)) => try!(self.handle_error(err)),
            Some(msg) => Err(PgError::Error(format!("Unexpected Message: {:?}", msg))),
            None => Err(PgError::Error("Unexpected end of messages".to_string())),
        }
    }

    fn handle_server_info<'a>(&mut self, message_queue: &mut VecDeque<ServerMsg<'a>>) -> Result<()> {
        match message_queue.pop_front() {
            Some(ServerMsg::ReadyForQuery) => {
                self.state = ConnectionState::ReadyForQuery;
                Ok(())
            },
            Some(ServerMsg::ErrorResponse(err)) => try!(self.handle_error(err)),
            Some(_) => Ok(()),
            None => return Err(PgError::Error("Unexpected end of messages".to_string())),
        }
    }

    fn handle_error<T>(&mut self, err: Vec<&str>) -> Result<T> {
        let message = err.get(3).unwrap();
        Err(PgError::Error(message.to_string()))
    }

    fn handle_ready_for_query<'a>(&mut self, message_queue: &mut VecDeque<ServerMsg<'a>>) -> Result<()> {
        match message_queue.pop_front() {
            Some(msg) => Err(PgError::Error(format!("Unexpected message after ReadyForQuery: {:?}", msg))),
            None => Ok(()),
        }
    }

    pub fn new(user: &str, host: &str, database: Option<&str>) -> Result<Connection> {
        let database = match database {
            Some(db) => db.to_string(),
            None => user.to_string(),
        };
        let user = user.to_string();
        let host = host.to_string();
        let port = 5432;
        let socket = try!(net::TcpStream::connect((host.as_str(), port)));
        try!(socket.set_read_timeout(Some(Duration::new(0, 1))));
        try!(socket.set_nodelay(true));

        let mut conn = Connection {
            user: user.clone(),
            database: database.clone(),
            host: host,
            port: port,
            socket: socket,
            state: ConnectionState::New,
        };
        try!(conn.initiate_connection());
        try!(conn.handle_startup());
        match conn.state {
            ConnectionState::ReadyForQuery => Ok(conn),
            ConnectionState::AuthenticationRejected => Err(PgError::Unauthenticated),
            state => Err(PgError::Error(format!("Unexpected state: {:?}", state))),
        }
    }

    fn read_from_socket(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        while buf.len() == 0 {
            match self.socket.read_to_end(buf) {
                Ok(_) => continue,
                Err(ioerr) => if let Some(11) = ioerr.raw_os_error() {
                    continue;
                } else {
                    try!(Err(ioerr));
                },
            }
        }
        Ok(buf.len())
    }

    pub fn query(&mut self, sql: &str) -> Result<Vec<Vec<String>>> {
        let query = Query { query: sql.to_string() };
        try!(self.socket.write_all(&query.to_bytes()));
        self.state = ConnectionState::AwaitingQueryResponse;
        let mut buf: Vec<u8> = vec!();
        try!(self.read_from_socket(&mut buf));
        let mut remainder = &buf[..];
        let mut data = vec![];
            
        while remainder.len() > 0 {
            let (bytes, excess) = try!(take_msg(remainder));
            let msg = try!(ServerMsg::from_slice(bytes));
            remainder = excess;
            match msg {
                ServerMsg::DataRow(vec) => {
                    let mut row = vec![];
                    for each in vec {
                        row.push(each.to_string());
                    }
                    data.push(row);
                },
                ServerMsg::RowDescription(_) => {
                    self.state = ConnectionState::AwaitingDataRows;
                },
                ServerMsg::CommandComplete(_) => {},
                ServerMsg::ReadyForQuery => {
                    if remainder.len() > 0 {
                        return Err(PgError::Error(
                            format!("Received data after ReadyForQuery: {:?}", remainder)
                        ));
                    };
                    self.state = ConnectionState::ReadyForQuery;
                },
                other => return Err(PgError::Error(format!("unexpected data: {:?}", other))),
            }
        }
        Ok(data)
    }

    /*
    pub fn close(self) -> Result<()> {
        self.socket.write_all(&Terminate.to_bytes()).unwrap();
        Ok(())
    }
    */
}

#[cfg(test)]
mod tests {
    use std::env;
    use super::Connection;

    #[test]
    fn test_connect() {
        let user_string = env::var("USER").unwrap();
        let user = user_string.as_ref();
        let host = "127.0.0.1";
        let database = Some(user);
        let conn = Connection::new(user, host, database);
        assert!(conn.is_ok());
    }

    #[test]
    fn test_query_with_bad_creds() {
        let user = "notauser";
        let host = "127.0.0.1";
        let database = Some("notadb");
        let conn = Connection::new(user, host, database);
        assert!(conn.is_err());
    }

    #[test]
    fn test_query() {
        let user_string = env::var("USER").unwrap();
        let user = user_string.as_ref();
        let host = "127.0.0.1";
        let mut conn = Connection::new(user, host, Some(user)).expect("Could not establish connection");
        let data = conn.query("SELECT VERSION();").unwrap();
        assert_eq!(data.len(), 1);
        let ref result = data[0][0];
        assert_eq!(&result[..10], "PostgreSQL");
    }
}
