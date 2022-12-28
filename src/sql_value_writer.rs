use rust_extensions::date_time::DateTimeAsMicroseconds;

pub enum SqlValue<'s> {
    Ignore,
    Null,
    Value {
        sql_type: Option<&'static str>,
        value: &'s dyn SqlValueWriter<'s>,
    },
}

#[derive(Debug)]
pub struct SqlValueToWrite<'s> {
    pub value: &'s (dyn tokio_postgres::types::ToSql + Sync),
}

pub trait SqlValueWriter<'s> {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValueToWrite<'s>>,
        sql_type: Option<&'static str>,
    );

    fn get_default_operator(&self) -> &str;
}

impl<'s> SqlValueWriter<'s> for String {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        params.push(SqlValueToWrite { value: self });
        sql.push('$');
        sql.push_str(params.len().to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for &'s str {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        params.push(SqlValueToWrite { value: self });
        sql.push('$');
        sql.push_str(params.len().to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for DateTimeAsMicroseconds {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValueToWrite<'s>>,
        sql_type: Option<&'static str>,
    ) {
        if let Some(sql_type) = sql_type {
            if sql_type == "bigint" {
                sql.push_str(self.unix_microseconds.to_string().as_str());
                return;
            }

            if sql_type == "timestamp" {
                sql.push('\'');
                sql.push_str(self.to_rfc3339().as_str());
                sql.push('\'');
                return;
            }

            panic!("Unknown sql type: {}", sql_type);
        }

        panic!("DateTimeAsMicroseconds requires sql_type");
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for bool {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        match self {
            true => sql.push_str("true"),
            false => sql.push_str("false"),
        }
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for u8 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for i8 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for u16 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for f32 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for f64 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for i16 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for u32 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for i32 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for u64 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for i64 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlValueWriter<'s> for tokio_postgres::types::IsNull {
    fn write(
        &'s self,
        sql: &mut String,
        _params: &mut Vec<SqlValueToWrite<'s>>,
        _sql_type: Option<&'static str>,
    ) {
        match self {
            tokio_postgres::types::IsNull::Yes => {
                sql.push_str("NULL");
            }
            tokio_postgres::types::IsNull::No => {
                sql.push_str("NOT NULL");
            }
        }
    }

    fn get_default_operator(&self) -> &str {
        " IS "
    }
}

impl<'s, T: SqlValueWriter<'s>> SqlValueWriter<'s> for Vec<T> {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValueToWrite<'s>>,
        sql_type: Option<&'static str>,
    ) {
        if self.len() == 1 {
            self.get(0).unwrap().write(sql, params, sql_type);
            return;
        }

        if self.len() > 0 {
            sql.push('(');

            let mut no = 0;
            for itm in self {
                if no > 0 {
                    sql.push_str(",");
                }
                itm.write(sql, params, sql_type);
                no += 1;
            }

            sql.push(')');
        }
    }

    fn get_default_operator(&self) -> &str {
        if self.len() == 0 {
            return "";
        } else if self.len() == 1 {
            return "=";
        } else {
            return " IN ";
        }
    }
}
