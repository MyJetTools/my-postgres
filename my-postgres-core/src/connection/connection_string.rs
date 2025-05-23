use crate::POSTGRES_DEFAULT_PORT;

const PREFIX: &str = "postgresql://";

pub struct Position {
    from: usize,
    to: usize,
}

pub enum ValueWithOverride {
    Value(Position),
    Override(String),
}

impl Into<ValueWithOverride> for Position {
    fn into(self) -> ValueWithOverride {
        ValueWithOverride::Value(self)
    }
}

pub struct PostgresConnectionString {
    conn_string: Vec<u8>,
    user_name: Position,
    password: Position,
    host: ValueWithOverride,
    port: u16,
    db_name: Position,
    ssl_require: bool,
    ssh: Option<Position>,
}

impl PostgresConnectionString {
    pub fn from_str(src: &str) -> Self {
        let conn_string = ConnectionStringFormat::parse_and_detect(src);
        Self::parse(conn_string)
    }
    pub fn parse(conn_string: ConnectionStringFormat) -> Self {
        match conn_string {
            ConnectionStringFormat::ReadyToUse(conn_string) => {
                Self::parse_space_separated(conn_string.as_bytes().to_vec())
            }
            ConnectionStringFormat::AsUrl(conn_string) => {
                Self::parse_url_connection_string(conn_string.as_bytes().to_vec())
            }
            ConnectionStringFormat::SemiColumnSeparated(conn_string) => {
                Self::parse_column_separated(conn_string.as_bytes().to_vec())
            }
        }
    }

    pub fn get_db_name(&self) -> &str {
        self.get_field_value(&self.db_name)
    }

    pub fn get_ssh(&self) -> Option<&str> {
        let result = self.get_field_value(self.ssh.as_ref()?);
        Some(result)
    }

    pub fn get_ssl_require(&self) -> bool {
        self.ssl_require
    }

    pub fn get_port(&self) -> u16 {
        self.port
    }

    pub fn get_host(&self) -> &str {
        match &self.host {
            ValueWithOverride::Value(position) => self.get_field_value(position),
            ValueWithOverride::Override(value) => value.as_str(),
        }
    }

    pub fn set_host(&mut self, host: String) {
        self.host = ValueWithOverride::Override(host);
    }

    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }

    fn parse_space_separated(conn_string: Vec<u8>) -> Self {
        let mut user_name = None;
        let mut password = None;
        let mut db_name = None;
        let mut host = None;
        let mut port: u16 = POSTGRES_DEFAULT_PORT;
        let mut ssl_require = false;
        let mut ssh = None;

        let mut pos = 0;
        while pos < conn_string.len() {
            let eq_pos = find_pos(conn_string.as_slice(), pos + 1, b'=');

            if eq_pos == None {
                break;
            }

            let eq_pos = eq_pos.unwrap();

            let end_of_value = find_pos(conn_string.as_slice(), eq_pos + 1, b' ');

            let end_of_value = match end_of_value {
                Some(end_of_value) => end_of_value,
                None => conn_string.len(),
            };

            let key = std::str::from_utf8(&conn_string[pos..eq_pos])
                .unwrap()
                .trim()
                .to_lowercase();

            match key.as_str() {
                "host" => {
                    host = Some(Position {
                        from: eq_pos + 1,
                        to: end_of_value,
                    });
                }
                "user" => {
                    user_name = Some(Position {
                        from: eq_pos + 1,
                        to: end_of_value,
                    });
                }
                "ssh" => {
                    ssh = Some(Position {
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

                    port = value.parse::<u16>().unwrap();
                }

                "dbname" => {
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
            host: host.unwrap().into(),
            port,
            db_name: db_name.unwrap(),
            ssl_require,
            ssh,
        }
    }

    #[cfg(feature = "with-ssh")]
    pub fn get_ssh_config(
        &self,
        ssh_config_builder: Option<crate::ssh::SshConfigBuilder>,
    ) -> Option<crate::ssh::PostgresSshConfig> {
        let ssh_target_builder = ssh_config_builder?;
        let ssh_line = self.get_ssh()?;

        let ssh_target = ssh_target_builder.build(ssh_line);
        ssh_target.into()
    }

    fn parse_column_separated(conn_string: Vec<u8>) -> Self {
        let mut user_name = None;
        let mut password = None;
        let mut db_name = None;
        let mut host = None;
        let mut ssh = None;
        let mut port: u16 = POSTGRES_DEFAULT_PORT;
        let mut ssl_require = false;

        let mut pos = 0;
        while pos < conn_string.len() {
            let eq_pos = find_pos(conn_string.as_slice(), pos + 1, b'=');

            if eq_pos == None {
                break;
            }

            let eq_pos = eq_pos.unwrap();

            let end_of_value = find_pos(conn_string.as_slice(), eq_pos + 1, b';');

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
                "ssh" => {
                    ssh = Some(Position {
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

                    port = value.parse::<u16>().unwrap();
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

                "ssl mode" => {
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
            host: host.unwrap().into(),
            port,
            db_name: db_name.unwrap(),
            ssl_require,
            ssh,
        }
    }

    fn parse_url_connection_string(conn_string: Vec<u8>) -> Self {
        let user_name_start = PREFIX.len();

        let pos = find_pos(conn_string.as_slice(), user_name_start + 1, b':');
        if pos.is_none() {
            panic!("Invalid connection string");
        }

        let user_name_end = pos.unwrap();

        let password_start = user_name_end + 1;

        let pos = find_pos(conn_string.as_slice(), password_start + 1, b'@');
        if pos.is_none() {
            panic!("Invalid connection string");
        }

        let password_end = pos.unwrap();

        // Reading host
        let host_start = password_end + 1;

        let pos = find_pos(conn_string.as_slice(), host_start + 1, b':');
        if pos.is_none() {
            panic!("Invalid connection string");
        }

        let host_end = pos.unwrap();

        // Reading port
        let port_start = host_end + 1;

        let pos = find_pos(conn_string.as_slice(), port_start + 1, b'/');
        if pos.is_none() {
            panic!("Invalid connection string");
        }

        let port_end = pos.unwrap();

        let port = std::str::from_utf8(&conn_string[port_start..port_end])
            .unwrap()
            .parse::<u16>()
            .unwrap();

        let db_name_start = port_end + 1;

        let mut ssl_require = false;

        let db_name_end =
            if let Some(pos_end) = find_pos(conn_string.as_slice(), port_start + 1, b'?') {
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
            }
            .into(),
            port,
            db_name: Position {
                from: db_name_start,
                to: db_name_end,
            },
            ssl_require,
            ssh: None,
        }
    }

    pub fn get_field_value(&self, pos: &Position) -> &str {
        return std::str::from_utf8(&self.conn_string[pos.from..pos.to]).unwrap();
    }

    pub fn to_string(&self, app_name: &str) -> String {
        if self.ssl_require {
            format!(
                "host={} port={} dbname={} user={} password={} application_name={} sslmode=require",
                self.get_host(),
                &self.port,
                self.get_field_value(&self.db_name),
                self.get_field_value(&self.user_name),
                self.get_field_value(&self.password),
                app_name
            )
        } else {
            format!(
                "host={} port={} dbname={} user={} password={} application_name={}",
                self.get_host(),
                &self.port,
                self.get_field_value(&self.db_name),
                self.get_field_value(&self.user_name),
                self.get_field_value(&self.password),
                app_name
            )
        }
    }

    pub fn to_string_with_host_as_unix_socket(&self, host_path: &str, app_name: &str) -> String {
        format!(
            "host={} dbname={} user={} password={} application_name={}",
            host_path,
            self.get_field_value(&self.db_name),
            self.get_field_value(&self.user_name),
            self.get_field_value(&self.password),
            app_name
        )
    }

    pub fn to_string_new_host_port(&self, host: &str, port: u16, app_name: &str) -> String {
        format!(
            "host={} port={} dbname={} user={} password={} application_name={}",
            host,
            port,
            self.get_field_value(&self.db_name),
            self.get_field_value(&self.user_name),
            self.get_field_value(&self.password),
            app_name
        )
    }

    pub fn to_string_with_new_db_name(&self, app_name: &str, db_name: &str) -> String {
        let mut result = format!(
            "host={} port={} dbname={} user={} password={} application_name={}",
            self.get_host(),
            &self.port,
            db_name,
            self.get_field_value(&self.user_name),
            self.get_field_value(&self.password),
            app_name
        );

        if self.ssl_require {
            result.push_str(" sslmode=require");
        }

        if let Some(ssh) = self.get_ssh() {
            result.push_str(" ssh=");
            result.push_str(ssh);
        }

        result
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

    let conn_string = PostgresConnectionString::parse(conn_string_format);

    conn_string.to_string(app_name)
}

pub enum ConnectionStringFormat<'s> {
    ReadyToUse(&'s str),
    AsUrl(&'s str),
    SemiColumnSeparated(&'s str),
}

impl<'s> ConnectionStringFormat<'s> {
    pub fn parse_and_detect(conn_string: &'s str) -> ConnectionStringFormat<'s> {
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

        let connection_string = PostgresConnectionString::parse(conn_string_format);

        assert_eq!(
            "username@username",
            connection_string.get_field_value(&connection_string.user_name)
        );

        assert_eq!(
            "password",
            connection_string.get_field_value(&connection_string.password)
        );

        assert_eq!("localhost", connection_string.get_host());

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
        let connection_string = PostgresConnectionString::parse(conn_string_format);

        assert_eq!(
            "username@username",
            connection_string.get_field_value(&connection_string.user_name)
        );

        assert_eq!(
            "password",
            connection_string.get_field_value(&connection_string.password)
        );

        assert_eq!("localhost", connection_string.get_host());

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
        let connection_string = PostgresConnectionString::parse(conn_string_format);

        assert_eq!(
            "admin",
            connection_string.get_field_value(&connection_string.user_name)
        );

        assert_eq!(
            "example",
            connection_string.get_field_value(&connection_string.password)
        );

        assert_eq!("10.0.0.3", connection_string.get_host());

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
        let connection_string = PostgresConnectionString::parse(conn_string_format);

        assert_eq!(
            "usr",
            connection_string.get_field_value(&connection_string.user_name)
        );

        assert_eq!(
            "password",
            connection_string.get_field_value(&connection_string.password)
        );

        assert_eq!("localhost", connection_string.get_host());

        assert_eq!(5566, connection_string.port);

        assert_eq!(
            "payments",
            connection_string.get_field_value(&connection_string.db_name)
        );

        assert_eq!(true, connection_string.ssl_require,);
    }

    #[test]
    fn test_parsing_semicolon_separated_connection_string_with_spaces() {
        let conn_string_format = ConnectionStringFormat::parse_and_detect(
            "Server=localhost;User Id=usr;Password=password;Database=payments;Ssl Mode=require;Port=5566",
        );
        let connection_string = PostgresConnectionString::parse(conn_string_format);

        assert_eq!(
            "usr",
            connection_string.get_field_value(&connection_string.user_name)
        );

        assert_eq!(
            "password",
            connection_string.get_field_value(&connection_string.password)
        );

        assert_eq!("localhost", connection_string.get_host());

        assert_eq!(5566, connection_string.port);

        assert_eq!(
            "payments",
            connection_string.get_field_value(&connection_string.db_name)
        );

        assert_eq!(true, connection_string.ssl_require,);
    }
}
