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
    port: u32,
    db_name: Position,
    ssl_require: bool,
}

impl<'s> ConnectionString<'s> {
    pub fn parse(conn_string: ConnectionStringFormat<'s>) -> Self {
        match conn_string {
            ConnectionStringFormat::ReadyToUse(_) => {
                panic!("We should not go here");
            }
            ConnectionStringFormat::AsUrl(conn_string) => {
                Self::parse_url_connection_string(conn_string.as_bytes())
            }
            ConnectionStringFormat::SemiColumnSeparated(conn_string) => {
                Self::parse_column_separated(conn_string.as_bytes())
            }
        }
    }

    pub fn get_db_name(&self) -> &str {
        self.get_field_value(&self.db_name)
    }

    fn parse_column_separated(conn_string: &'s [u8]) -> Self {
        let mut user_name = None;
        let mut password = None;
        let mut db_name = None;
        let mut host = None;
        let mut port: u32 = 5432;
        let mut ssl_require = false;

        let mut pos = 0;
        while pos < conn_string.len() {
            let eq_pos = find_pos(conn_string, pos + 1, b'=');

            if eq_pos == None {
                break;
            }

            let eq_pos = eq_pos.unwrap();

            let end_of_value = find_pos(conn_string, eq_pos + 1, b';');

            let end_of_value = match end_of_value {
                Some(end_of_value) => end_of_value,
                None => conn_string.len(),
            };

            let key = std::str::from_utf8(&conn_string[pos..eq_pos])
                .unwrap()
                .trim()
                .to_lowercase();

            match key.as_str() {
                "server" => {
                    host = Some(Position {
                        from: eq_pos + 1,
                        to: end_of_value,
                    });
                }
                "userid" => {
                    user_name = Some(Position {
                        from: eq_pos + 1,
                        to: end_of_value,
                    });
                }
                "user id" => {
                    user_name = Some(Position {
                        from: eq_pos + 1,
                        to: end_of_value,
                    });
                }
                "password" => {
                    password = Some(Position {
                        from: eq_pos + 1,
                        to: end_of_value,
                    });
                }

                "port" => {
                    let value = std::str::from_utf8(&conn_string[eq_pos + 1..end_of_value])
                        .unwrap()
                        .trim();

                    port = value.parse::<u32>().unwrap();
                }

                "database" => {
                    db_name = Some(Position {
                        from: eq_pos + 1,
                        to: end_of_value,
                    });
                }

                "sslmode" => {
                    let value = std::str::from_utf8(&conn_string[eq_pos + 1..end_of_value])
                        .unwrap()
                        .trim()
                        .to_lowercase();
                    ssl_require = value == "require";
                }

                _ => {}
            }

            pos = end_of_value + 1;
        }

        Self {
            conn_string: conn_string,
            user_name: user_name.unwrap(),
            password: password.unwrap(),
            host: host.unwrap(),
            port,
            db_name: db_name.unwrap(),
            ssl_require,
        }
    }

    fn parse_url_connection_string(conn_string: &'s [u8]) -> Self {
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

        let port = std::str::from_utf8(&conn_string[port_start..port_end])
            .unwrap()
            .parse::<u32>()
            .unwrap();

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
            port,
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
                &self.port,
                self.get_field_value(&self.db_name),
                self.get_field_value(&self.user_name),
                self.get_field_value(&self.password),
                app_name
            )
        } else {
            format!(
                "host={} port={} dbname={} user={} password={} application_name={}",
                self.get_field_value(&self.host),
                &self.port,
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

    let conn_string_format = ConnectionStringFormat::parse_and_detect(conn_string);

    if conn_string_format.is_ready_to_use() {
        return format!("{} application_name={}", conn_string, app_name);
    }

    let conn_string = ConnectionString::parse(conn_string_format);

    conn_string.to_string(app_name)
}

pub enum ConnectionStringFormat<'s> {
    ReadyToUse(&'s str),
    AsUrl(&'s str),
    SemiColumnSeparated(&'s str),
}

impl<'s> ConnectionStringFormat<'s> {
    pub fn parse_and_detect(conn_string: &'s str) -> ConnectionStringFormat {
        if conn_string.trim().starts_with(PREFIX) {
            return ConnectionStringFormat::AsUrl(conn_string);
        }

        let mut spaces = 0;

        let mut semicolons = 0;

        let as_bytes = conn_string.as_bytes();

        for i in 0..as_bytes.len() {
            if as_bytes[i] == b' ' {
                spaces += 1;
            } else if as_bytes[i] == b';' {
                semicolons += 1;
            }
        }

        if spaces > semicolons {
            return ConnectionStringFormat::ReadyToUse(conn_string);
        }

        return ConnectionStringFormat::SemiColumnSeparated(conn_string);
    }

    pub fn is_ready_to_use(&'s self) -> bool {
        match self {
            ConnectionStringFormat::ReadyToUse(_) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_conn_string_with_no_additional_params() {
        let conn_string_format = ConnectionStringFormat::parse_and_detect(
            "postgresql://username@username:password@localhost:5432/dbname",
        );

        let connection_string = ConnectionString::parse(conn_string_format);

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

        assert_eq!(5432, connection_string.port);

        assert_eq!(
            "dbname",
            connection_string.get_field_value(&connection_string.db_name)
        );

        assert_eq!(false, connection_string.ssl_require,);
    }

    #[test]
    fn test_parsing_csharp_like_connection_string() {
        let conn_string_format = ConnectionStringFormat::parse_and_detect(
            "postgresql://username@username:password@localhost:5432/dbname?sslmode=require",
        );
        let connection_string = ConnectionString::parse(conn_string_format);

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

        assert_eq!(5432, connection_string.port);

        assert_eq!(
            "dbname",
            connection_string.get_field_value(&connection_string.db_name)
        );

        assert_eq!(true, connection_string.ssl_require,);
    }

    #[test]
    fn test_parsing_url_like_connection_string_with_timeout() {
        let conn_string_format = ConnectionStringFormat::parse_and_detect(
            "postgresql://admin:example@10.0.0.3:5432/my_dbname?connect_timeout=10",
        );
        let connection_string = ConnectionString::parse(conn_string_format);

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

        assert_eq!(5432, connection_string.port);

        assert_eq!(
            "my_dbname",
            connection_string.get_field_value(&connection_string.db_name)
        );

        assert_eq!(false, connection_string.ssl_require,);
    }

    #[test]
    fn test_parsing_semicolon_separated_connection_string() {
        let conn_string_format = ConnectionStringFormat::parse_and_detect(
            "Server=localhost;UserId=usr;Password=password;Database=payments;sslmode=require;Port=5566",
        );
        let connection_string = ConnectionString::parse(conn_string_format);

        assert_eq!(
            "usr",
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

        assert_eq!(5566, connection_string.port);

        assert_eq!(
            "payments",
            connection_string.get_field_value(&connection_string.db_name)
        );

        assert_eq!(true, connection_string.ssl_require,);
    }
}
