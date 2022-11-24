use rust_extensions::date_time::DateTimeAsMicroseconds;

pub enum SqlValue<'s> {
    Ignore,
    Null,
    Value {
        options: Option<Vec<&'static str>>,
        value: &'s dyn SqlValueWriter<'s>,
    },
}

pub trait SqlValueWriter<'s> {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        options: Option<&Vec<&'static str>>,
    );
}

impl<'s> SqlValueWriter<'s> for String {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        params.push(self);
        sql.push('$');
        sql.push_str(params.len().to_string().as_str());
    }
}

impl<'s> SqlValueWriter<'s> for &'s str {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        params.push(self);
        sql.push('$');
        sql.push_str(params.len().to_string().as_str());
    }
}

impl<'s> SqlValueWriter<'s> for DateTimeAsMicroseconds {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        options: Option<&Vec<&'static str>>,
    ) {
        if let Some(options) = options {
            for option in options {
                if *option == "bigint" {
                    sql.push('\'');
                    sql.push_str(self.unix_microseconds.to_string().as_str());
                    sql.push('\'');
                }
            }
        }

        sql.push('\'');
        sql.push_str(self.to_rfc3339().as_str());
        sql.push('\'');
    }
}

impl<'s> SqlValueWriter<'s> for bool {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        match self {
            true => sql.push_str("true"),
            false => sql.push_str("false"),
        }
    }
}

impl<'s> SqlValueWriter<'s> for u8 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlValueWriter<'s> for i8 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlValueWriter<'s> for u16 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlValueWriter<'s> for f32 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlValueWriter<'s> for f64 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlValueWriter<'s> for i16 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlValueWriter<'s> for u32 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlValueWriter<'s> for i32 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlValueWriter<'s> for u64 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}

impl<'s> SqlValueWriter<'s> for i64 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        _options: Option<&Vec<&'static str>>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
}
