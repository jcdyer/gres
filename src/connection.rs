use crate::{
    auth,
    error::PgError,
    message::{
        BindMessage, CloseMessage, CloseType, Message, ParseMessage, PasswordMessage, Query,
        StartupMessage, Terminate,
    },
    servermsg::{take_msg, AuthMsg, NoticeBody, ServerMsg},
    Result,
};


use std::{
    cell::Cell,
    collections::vec_deque::VecDeque,
    io::{Read, Write},
    net,
    time::Duration,
};

#[derive(Copy, Debug, Eq, PartialEq, Clone)]
enum ConnectionState {
    New,
    AwaitingAuthResponse,
    Authenticated,
    AuthenticationRejected,
    ReadyForQuery,
    AwaitingQueryResponse,
    AwaitingDataRows,
    Disconnected,
}

#[derive(Debug)]
pub struct Connection {
    user: String,
    database: String,
    password: Option<String>,
    host: String,
    port: u16,
    socket: net::TcpStream,
    state: Cell<ConnectionState>,
    query_number: Cell<u32>,
}

impl Connection {
    fn initiate_connection(&self) -> Result<()> {
        let startup = StartupMessage {
            user: &self.user,
            database: Some(&self.database),
            params: vec![],
        };
        self.send_message(&startup)?;
        self.state.set(ConnectionState::AwaitingAuthResponse);
        Ok(())
    }

    fn handle_startup(&self) -> Result<()> {
        while match self.state.get() {
            ConnectionState::ReadyForQuery => false,
            ConnectionState::AuthenticationRejected => false,
            _ => true,
        } {
            println!("AQUI");
            let mut buf = Vec::with_capacity(1024);
            let mut message_queue = VecDeque::new();
            self.read_from_socket(&mut buf)?;
            let mut remainder = &buf[..];
            while !remainder.is_empty() {
                let (bytes, excess) = take_msg(remainder)?;
                let msg = ServerMsg::from_slice(bytes)?;
                message_queue.push_back(msg);
                remainder = excess;
            }
            println!("VecDeque: {:?}", message_queue);
            while !message_queue.is_empty() {
                match self.state.get() {
                    ConnectionState::AwaitingAuthResponse => {
                        self.handle_auth(&mut message_queue)?
                    }
                    ConnectionState::AuthenticationRejected => false,
                    ConnectionState::Authenticated => {
                        self.handle_server_info(&mut message_queue)?
                    }
                    ConnectionState::ReadyForQuery => {
                        self.handle_ready_for_query(&mut message_queue)?;
                        break;
                    }
                    state => {
                        return Err(PgError::Error(format!(
                            "Invalid startup state: {:?}",
                            state
                        )))
                    }
                };
            }
        }
        Ok(())
    }

    fn handle_auth<'a>(&self, message_queue: &mut VecDeque<ServerMsg<'a>>) -> Result<bool> {
        let msg = message_queue.pop_front();
        match msg {
            Some(ServerMsg::Auth(AuthMsg::Ok)) => {
                self.state.set(ConnectionState::Authenticated);
                Ok(false)
            }
            Some(ServerMsg::Auth(AuthMsg::Md5(salt))) => {
                let password = &self.password.clone().unwrap_or_default();
                let passhash = auth::build_md5_hash(&self.user, password, salt);
                let password_message = PasswordMessage { hash: &passhash };
                self.send_message(&password_message)?;
                Ok(true)
            }
            Some(ServerMsg::Auth(method)) => Err(PgError::Error(format!(
                "Unimplemented authentication method, {:?}",
                method
            ))),
            Some(ServerMsg::ErrorResponse(err)) => self.handle_auth_error(err)?,
            Some(msg) => Err(PgError::Error(format!(
                "Unexpected non-auth message: {:?}",
                msg
            ))),
            None => Err(PgError::Error("No message received".to_string())),
        }
    }

    fn handle_server_info<'a>(&self, message_queue: &mut VecDeque<ServerMsg<'a>>) -> Result<bool> {
        match message_queue.pop_front() {
            Some(ServerMsg::ReadyForQuery) => {
                self.state.set(ConnectionState::ReadyForQuery);
                Ok(false)
            }
            Some(ServerMsg::ErrorResponse(err)) => self.handle_error(err)?,
            Some(_) => Ok(false),
            None => Ok(true),
        }
    }

    fn handle_auth_error<T>(&self, err: NoticeBody<'_>) -> Result<T> {
        self.state.set(ConnectionState::AuthenticationRejected);
        self.handle_error(err)
    }
    fn handle_error<T>(&self, err: NoticeBody<'_>) -> Result<T> {
        Err(PgError::Error(err.message().to_string()))
    }

    fn handle_ready_for_query<'a>(
        &self,
        message_queue: &mut VecDeque<ServerMsg<'a>>,
    ) -> Result<bool> {
        match message_queue.pop_front() {
            Some(msg) => Err(PgError::Error(format!(
                "Unexpected message after ReadyForQuery: {:?}",
                msg
            ))),
            None => Ok(false),
        }
    }

    pub fn new(
        user: &str,
        password: Option<&str>,
        host: &str,
        database: Option<&str>,
    ) -> Result<Connection> {
        let database = match database {
            Some(db) => db.to_string(),
            None => user.to_string(),
        };
        let password = match password {
            Some(pass) => Some(pass.to_string()),
            None => None,
        };
        let user = user.to_string();
        let host = host.to_string();
        let port = 5432;
        let socket = net::TcpStream::connect((host.as_str(), port))?;
        socket.set_read_timeout(Some(Duration::new(0, 1)))?;
        socket.set_nodelay(true)?;
        let conn = Connection {
            user,
            password,
            database,
            host,
            port,
            socket,
            state: Cell::new(ConnectionState::New),
            query_number: Cell::new(0),
        };
        conn.initiate_connection()?;
        conn.handle_startup()?;
        match conn.state.get() {
            ConnectionState::ReadyForQuery => Ok(conn),
            ConnectionState::AuthenticationRejected => Err(PgError::Unauthenticated),
            state => Err(PgError::Error(format!("Unexpected state: {:?}", state))),
        }
    }

    fn send_message<M: Message + std::fmt::Debug>(&self, message: &M) -> Result<()> {
        let buf = dbg!(message).to_bytes();
        dbg!(dbg!(&buf).len());
        ::log::trace!("sending {:?}", buf);
        (&self.socket).write_all(&buf)?;
        Ok(())
    }

    // This looks wrong?
    fn read_from_socket(&self, buf: &mut Vec<u8>) -> Result<usize> {
        while buf.is_empty() {
            match (&self.socket).read_to_end(buf) {
                Ok(_) => continue,
                Err(ioerr) => {
                    if let Some(11) = ioerr.raw_os_error() {
                        continue;
                    } else {
                        return Err(ioerr.into());
                    }
                }
            }
        }
        ::log::trace!("received {:?}", buf);
        Ok(buf.len())
    }

    pub fn simple_query(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        let query = Query {
            query: sql.to_string(),
        };
        self.send_message(&query)?;
        self.state.set(ConnectionState::AwaitingQueryResponse);
        let mut buf: Vec<u8> = vec![];
        self.read_from_socket(&mut buf)?;
        let mut remainder = &buf[..];
        let mut data = vec![];

        while !remainder.is_empty() {
            let (bytes, excess) = take_msg(remainder)?;
            let msg = ServerMsg::from_slice(bytes)?;
            remainder = excess;
            match msg {
                ServerMsg::DataRow(vec) => {
                    let mut row = vec![];
                    for each in vec {
                        row.push(each.to_string());
                    }
                    data.push(row);
                }
                ServerMsg::RowDescription(_) => {
                    self.state.set(ConnectionState::AwaitingDataRows);
                }
                ServerMsg::CommandComplete(_) => {}
                ServerMsg::ReadyForQuery => {
                    if !remainder.is_empty() {
                        return Err(PgError::Error(format!(
                            "Received data after ReadyForQuery: {:?}",
                            remainder
                        )));
                    };
                    self.state.set(ConnectionState::ReadyForQuery);
                }
                ServerMsg::NoticeResponse(r) => ::log::info!("{:?}", r),
                other => return Err(PgError::Error(format!("unexpected data: {:?}", other))),
            }
        }
        Ok(data)
    }
    pub fn query<'a>(
        &'a self,
        sql: &str,
        _params: impl IntoIterator<Item = Param<'a>>,
    ) -> Result<Vec<Vec<String>>> {
        let _qh = self.prepare(sql)?;
        todo!()
    }

    pub fn prepare<'a>(&'a self, sql: &str) -> Result<QueryHandle<'a>> {
        let query_number = self.query_number.get();
        let query_name = query_number.to_string();
        self.query_number.set(query_number + 1);
        let parse_message = ParseMessage {
            name: &query_name,
            sql,
            param_types: &[],
        };
        self.send_message(&parse_message)?;
        let mut buf: Vec<u8> = vec![];
        self.read_from_socket(&mut buf)?;
        let (msg, rest) = take_msg(&buf)?;
        if rest.is_empty() {
            match ServerMsg::from_slice(msg)? {
                ServerMsg::ParseComplete => Ok(QueryHandle {
                    query_name,
                    conn: self,
                }),
                ServerMsg::ErrorResponse(err) => {
                    Err(PgError::Error(format!("ServerError: {:?}", err)))
                }
                msg => Err(PgError::Error(format!("Unexpected message: {:?}", msg))),
            }
        } else {
            Err(PgError::Other)
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let msg = Terminate;
        match self.send_message(&msg) {
            Ok(_) => {}
            Err(error) => {
                println!(
                    "WARNING: An error occurred ending the session with the server: {:?}",
                    error
                );
            }
        };
        self.state.set(ConnectionState::Disconnected);
    }
}

pub enum Param<'a> {
    String(&'a str),
    Binary(&'a [u8]),
    Integer(i64),
    Boolean(bool),
    Null,
}

pub struct QueryHandle<'conn> {
    conn: &'conn Connection,
    query_name: String,
}

impl<'conn> QueryHandle<'conn> {
    pub fn bind<'a>(
        &'a self,
        portal_name: String,
        params: &[Vec<u8>],
    ) -> Result<Portal<'a, 'conn>> {
        let msg = BindMessage {
            portal: &portal_name,
            prepared_statement: &self.query_name,
            param_format_codes: &[],
            param_values: params,
            result_format_codes: &[],
        };
        self.conn.send_message(&msg)?;

        // TODO: Query the portal parameters

        Ok(Portal {
            query_handle: self,
            portal_name,
            row_format: None,
        })
    }
}

impl<'conn> Drop for QueryHandle<'conn> {
    fn drop(&mut self) {
        let msg = CloseMessage {
            close_type: CloseType::PreparedStatement,
            name: &self.query_name,
        };
        let _ = self.conn.send_message(&msg);
        let mut reply = Vec::new();
        match self.conn.read_from_socket(&mut reply) {
            Err(err) => ::log::error!("Error: {}", err),
            Ok(_) => {
                let mut remainder: &[u8] = &reply;
                while !remainder.is_empty() {
                    let msg = take_msg(&remainder)
                        .map(|(body, extra)|{
                            remainder = extra;
                            body
                        })
                        .and_then(ServerMsg::from_slice);
                    match msg {
                        Ok(ServerMsg::CloseComplete) => {}
                        Ok(ServerMsg::ErrorResponse(notice_body)) => {
                            eprintln!(
                                "Error while dropping queryhandle for {:?}: {:?}",
                                self.query_name,
                                notice_body,
                            );

                            eprintln!("Error ")
                        }
                        Ok(ServerMsg::NoticeResponse(notice_body)) => {
                            ::log::info!("Notice: {:?}", notice_body);
                        }
                        Ok(msg) => {
                            eprintln!(
                                "Unexpected response while dropping queryhandle for {:?}: {:?}",
                                self.query_name,
                                msg,
                            )
                        }
                        Err(err) => {
                            eprintln!(
                                "Error receiving response while dropping queryhandle for {:?}: {}",
                                self.query_name, err
                            );
                            break;
                        }
                    }
                }
            }
        }
        todo!()
    }
}

struct RowFormat {

}

pub struct Portal<'qh, 'conn> {
    query_handle: &'qh QueryHandle<'conn>,
    portal_name: String,
    row_format: Option<RowFormat>,
}


#[cfg(test)]
mod tests {
    use std::env;

    use super::Connection;

    fn init_log() {
        pretty_env_logger::init();
    }

    #[test]
    fn test_connect() {
        let user_string = env::var("USER").unwrap();
        let user = user_string.as_ref();
        let password = Some(user);
        let host = "127.0.0.1";
        let database = Some(user);
        let conn = Connection::new(user, password, host, database);
        assert!(conn.is_ok());
    }

    #[test]
    fn test_query_with_bad_creds() {
        let user = "notauser";
        let pass = Some(user);
        let host = "127.0.0.1";
        let database = Some("notadb");
        let conn = Connection::new(user, pass, host, database);
        assert!(conn.is_err());
    }

    #[test]
    fn test_query() {
        let user_string = env::var("USER").unwrap();
        let user = user_string.as_ref();
        let pass = Some(user);
        let host = "127.0.0.1";
        let conn =
            Connection::new(user, pass, host, Some(user)).expect("Could not establish connection");
        let data = conn.simple_query("SELECT VERSION();").unwrap();
        assert_eq!(data.len(), 1);
        let result = &data[0][0];
        assert_eq!(&result[..10], "PostgreSQL");
    }

    #[test]
    fn test_crud() {
        init_log();
        let user_string = env::var("USER").unwrap();
        let user = user_string.as_ref();
        let pass = Some(user);
        let host = "127.0.0.1";
        let conn =
            Connection::new(user, pass, host, Some(user)).expect("Could not establish connection");

        conn.simple_query("DROP TABLE IF EXISTS pg_rust_test_crud;")
            .expect("query should not fail");

        let create_response = conn.simple_query(
            "CREATE TABLE pg_rust_test_crud (
            id INTEGER PRIMARY KEY NOT NULL,
            name VARCHAR(32) NOT NULL,
            age INTEGER NOT NULL
        );",
        );

        eprintln!("create response {:?}", create_response);
        assert!(create_response.is_ok());

        let insert_response = conn.simple_query(
            r#"INSERT INTO pg_rust_test_crud (id, name, age) VALUES
            (1, 'uman', 42),
            (2, 'rocio', 42),
            (3, 'paulo', 5)
        ;"#,
        );

        eprintln!("insert response{:?}", insert_response);
        assert!(insert_response.is_ok());

        let select_response = conn.simple_query(
            "SELECT id, name
            FROM pg_rust_test_crud
            WHERE age < 18;",
        );

        eprintln!("select response {:?}", select_response);
        assert!(select_response.is_ok());

        let delete_response = conn.simple_query("DELETE FROM pg_rust_test_crud WHERE id = 2;");
        eprintln!("delete response {:?}", delete_response);
        assert!(delete_response.is_ok());

        let drop_response = conn.simple_query("DROP TABLE pg_rust_test_crud;");
        eprintln!("drop response {:?}", drop_response);
        assert!(drop_response.is_ok());
    }
}
