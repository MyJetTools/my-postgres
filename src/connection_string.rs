const PREFIX: &str = "postgresql://";

pub struct Position {
    from: usize,
    to: usize,
}

pub struct ConnectionString<'s> {
    conn_string: &'s [u8],
    user_name: Position,
    password: Position,
    host: Position,
    port: Position,
    db_name: Position,
    ssl_require: bool,
}

impl<'s> ConnectionString<'s> {
    pub fn parse(conn_string: &'s [u8]) -> Self {
        let user_name_start = PREFIX.len();

        let pos = find_pos(conn_string, user_name_start + 1, b':');
        if pos.is_none() {
            panic!("Invalid connection string");
        }

        let user_name_end = pos.unwrap();

        let password_start = user_name_end + 1;

        let pos = find_pos(conn_string, password_start + 1, b'@');
        if pos.is_none() {
            panic!("Invalid connection string");
        }

        let password_end = pos.unwrap();

        // Reading host
        let host_start = password_end + 1;

        let pos = find_pos(conn_string, host_start + 1, b':');
        if pos.is_none() {
            panic!("Invalid connection string");
        }

        let host_end = pos.unwrap();

        // Reading port
        let port_start = host_end + 1;

        let pos = find_pos(conn_string, port_start + 1, b'/');
        if pos.is_none() {
            panic!("Invalid connection string");
        }

        let port_end = pos.unwrap();

        let db_name_start = port_end + 1;

        let mut ssl_require = false;

        let db_name_end = if let Some(pos_end) = find_pos(conn_string, port_start + 1, b'?') {
            let query = std::str::from_utf8(&conn_string[pos_end + 1..]).unwrap();
            for itm in query.split('&') {
                let mut parts = itm.split('=');
                let key = parts.next().unwrap();
                let value = parts.next().unwrap();

                if key == "sslmode" {
                    ssl_require = value == "require";
                }
            }

            pos_end
        } else {
            conn_string.len()
        };

        Self {
            conn_string,
            user_name: Position {
                from: user_name_start,
                to: user_name_end,
            },
            password: Position {
                from: password_start,
                to: password_end,
            },
            host: Position {
                from: host_start,
                to: host_end,
            },
            port: Position {
                from: port_start,
                to: port_end,
            },
            db_name: Position {
                from: db_name_start,
                to: db_name_end,
            },
            ssl_require,
        }
    }

    pub fn get_field_value(&'s self, pos: &Position) -> &'s str {
        return std::str::from_utf8(&self.conn_string[pos.from..pos.to]).unwrap();
    }

    pub fn to_string(&self, app_name: &str) -> String {
        if self.ssl_require {
            format!(
                "host={} port={} dbname={} user={} password={} application_name={} sslmode=require",
                self.get_field_value(&self.host),
                self.get_field_value(&self.port),
                self.get_field_value(&self.db_name),
                self.get_field_value(&self.user_name),
                self.get_field_value(&self.password),
                app_name
            )
        } else {
            format!(
                "host={} port={} dbname={} user={} password={} application_name={}",
                self.get_field_value(&self.host),
                self.get_field_value(&self.port),
                self.get_field_value(&self.db_name),
                self.get_field_value(&self.user_name),
                self.get_field_value(&self.password),
                app_name
            )
        }
    }
}

pub fn find_pos(src: &[u8], start_pos: usize, search_byte: u8) -> Option<usize> {
    for i in start_pos..src.len() {
        if src[i] == search_byte {
            return Some(i);
        }
    }

    None
}

pub fn format(conn_string: &str, app_name: &str) -> String {
    let conn_string = conn_string.trim();

    if conn_string.starts_with(PREFIX) {
        let conn_string = ConnectionString::parse(conn_string.as_bytes());
        return conn_string.to_string(app_name);
    }

    format!("{} application_name={}", conn_string, app_name)
}

#[cfg(test)]
mod test {

    use super::ConnectionString;

    #[test]
    fn test_connstring_with_no_additional_params() {
        let connection_string = ConnectionString::parse(
            "postgresql://username@username:password@localhost:5432/dbname".as_bytes(),
        );

        assert_eq!(
            "username@username",
            connection_string.get_field_value(&connection_string.user_name)
        );

        assert_eq!(
            "password",
            connection_string.get_field_value(&connection_string.password)
        );

        assert_eq!(
            "localhost",
            connection_string.get_field_value(&connection_string.host)
        );

        assert_eq!(
            "5432",
            connection_string.get_field_value(&connection_string.port)
        );

        assert_eq!(
            "dbname",
            connection_string.get_field_value(&connection_string.db_name)
        );

        assert_eq!(false, connection_string.ssl_require,);
    }

    #[test]
    fn test_parsing_csharp_like_connection_string() {
        let connection_string = ConnectionString::parse(
            "postgresql://username@username:password@localhost:5432/dbname?sslmode=require"
                .as_bytes(),
        );

        assert_eq!(
            "username@username",
            connection_string.get_field_value(&connection_string.user_name)
        );

        assert_eq!(
            "password",
            connection_string.get_field_value(&connection_string.password)
        );

        assert_eq!(
            "localhost",
            connection_string.get_field_value(&connection_string.host)
        );

        assert_eq!(
            "5432",
            connection_string.get_field_value(&connection_string.port)
        );

        assert_eq!(
            "dbname",
            connection_string.get_field_value(&connection_string.db_name)
        );

        assert_eq!(true, connection_string.ssl_require,);
    }

    #[test]
    fn test_parsing_csharp_like_connection_string_with_timeout() {
        let connection_string = ConnectionString::parse(
            "postgresql://admin:example@10.0.0.3:5432/mydbname?connect_timeout=10".as_bytes(),
        );

        assert_eq!(
            "admin",
            connection_string.get_field_value(&connection_string.user_name)
        );

        assert_eq!(
            "example",
            connection_string.get_field_value(&connection_string.password)
        );

        assert_eq!(
            "10.0.0.3",
            connection_string.get_field_value(&connection_string.host)
        );

        assert_eq!(
            "5432",
            connection_string.get_field_value(&connection_string.port)
        );

        assert_eq!(
            "mydbname",
            connection_string.get_field_value(&connection_string.db_name)
        );

        assert_eq!(false, connection_string.ssl_require,);
    }
}
