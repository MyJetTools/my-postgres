use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::SqlValueMetadata;

pub trait SelectPartValue {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        metadata: &Option<SqlValueMetadata>,
    );
}

impl SelectPartValue for String {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl<'s> SelectPartValue for &'s str {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl SelectPartValue for usize {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl SelectPartValue for i64 {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl SelectPartValue for u64 {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl SelectPartValue for i32 {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl SelectPartValue for u32 {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl SelectPartValue for i16 {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl SelectPartValue for u16 {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl SelectPartValue for i8 {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl SelectPartValue for u8 {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl SelectPartValue for f64 {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl SelectPartValue for f32 {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl SelectPartValue for bool {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        _metadata: &Option<SqlValueMetadata>,
    ) {
        sql.push_str(field_names.get(0).unwrap());
    }
}

impl SelectPartValue for DateTimeAsMicroseconds {
    fn fill_select_part(
        sql: &mut String,
        field_names: &[&str],
        metadata: &Option<SqlValueMetadata>,
    ) {
        let field_name = field_names.get(0).unwrap();
        if let Some(metadata) = metadata {
            if let Some(sql_type) = metadata.sql_type {
                if sql_type == "timestamp" {
                    sql.push_str("(extract(EPOCH FROM ");
                    sql.push_str(field_name);
                    sql.push_str(") * 1000000)::bigint \"");
                    sql.push_str(field_name);
                    sql.push('"');
                    return;
                }

                if sql_type == "bigint" {
                    sql.push_str(field_name);
                    return;
                }

                panic!("Unknown sql_type: {}", sql_type);
            }
        }

        panic!("sql_type is required for DateTimeAsMicroseconds");
    }
}
