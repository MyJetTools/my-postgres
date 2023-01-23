use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{SqlValue, SqlValueMetadata};

pub trait SqlWhereValueWriter<'s> {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValue<'s>>,
        metadata: &Option<SqlValueMetadata>,
    );

    fn get_default_operator(&self) -> &str;
}

impl<'s> SqlWhereValueWriter<'s> for String {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        params.push(SqlValue::Ref(self));
        sql.push('$');
        sql.push_str(params.len().to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlWhereValueWriter<'s> for &'s str {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        params.push(SqlValue::Ref(self));
        sql.push('$');
        sql.push_str(params.len().to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlWhereValueWriter<'s> for DateTimeAsMicroseconds {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        metadata: &Option<SqlValueMetadata>,
    ) {
        if let Some(metadata) = &metadata {
            if let Some(sql_type) = metadata.sql_type {
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
        }

        panic!("DateTimeAsMicroseconds requires sql_type");
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlWhereValueWriter<'s> for bool {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
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

impl<'s> SqlWhereValueWriter<'s> for u8 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlWhereValueWriter<'s> for i8 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlWhereValueWriter<'s> for u16 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlWhereValueWriter<'s> for f32 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlWhereValueWriter<'s> for f64 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlWhereValueWriter<'s> for i16 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }
    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlWhereValueWriter<'s> for u32 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlWhereValueWriter<'s> for i32 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlWhereValueWriter<'s> for u64 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlWhereValueWriter<'s> for i64 {
    fn write(
        &'s self,
        sql: &mut String,
        _: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(self.to_string().as_str());
    }

    fn get_default_operator(&self) -> &str {
        "="
    }
}

impl<'s> SqlWhereValueWriter<'s> for tokio_postgres::types::IsNull {
    fn write(
        &'s self,
        sql: &mut String,
        _params: &mut Vec<SqlValue<'s>>,
        _metadata: &Option<SqlValueMetadata>,
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

impl<'s, T: SqlWhereValueWriter<'s>> SqlWhereValueWriter<'s> for Vec<T> {
    fn write(
        &'s self,
        sql: &mut String,
        params: &mut Vec<SqlValue<'s>>,
        metadata: &Option<SqlValueMetadata>,
    ) {
        if self.len() == 1 {
            self.get(0).unwrap().write(sql, params, metadata);
            return;
        }

        if self.len() > 0 {
            sql.push('(');

            let mut no = 0;
            for itm in self {
                if no > 0 {
                    sql.push_str(",");
                }
                itm.write(sql, params, metadata);
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
