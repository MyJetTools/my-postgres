use std::collections::BTreeMap;

use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{table_schema::TableColumnType, SqlValueMetadata};

use super::RenderFullWhereCondition;

pub trait SqlWhereValueProvider {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool;

    fn render_value(&self) -> bool;
}

impl SqlWhereValueProvider for String {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }

        let index = params.push(self.into());
        sql.push('$');
        sql.push_str(index.to_string().as_str());
        true
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl<'s> SqlWhereValueProvider for &'s str {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }

        let index = params.push((*self).into());
        sql.push('$');
        sql.push_str(index.to_string().as_str());

        true
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for DateTimeAsMicroseconds {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }

        if let Some(metadata) = &metadata {
            match metadata.sql_type {
                TableColumnType::BigInt => {
                    sql.push_str(self.unix_microseconds.to_string().as_str());

                    return true;
                }
                TableColumnType::Timestamp => {
                    sql.push('\'');
                    sql.push_str(self.to_rfc3339().as_str());
                    sql.push('\'');
                    return true;
                }

                _ => {
                    panic!("Unknown sql type: {:?}", metadata.sql_type);
                }
            }
        }

        panic!("DateTimeAsMicroseconds requires sql_type");
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for bool {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }

        match self {
            true => sql.push_str("true"),
            false => sql.push_str("false"),
        }

        true
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for u8 {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }
        sql.push_str(self.to_string().as_str());

        true
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for i8 {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }
        sql.push_str(self.to_string().as_str());

        true
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for u16 {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }
        sql.push_str(self.to_string().as_str());

        true
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for f32 {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }
        sql.push_str(self.to_string().as_str());

        true
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for f64 {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }
        sql.push_str(self.to_string().as_str());

        true
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for i16 {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }
        sql.push_str(self.to_string().as_str());

        true
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for u32 {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }
        sql.push_str(self.to_string().as_str());

        true
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for i32 {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }
        sql.push_str(self.to_string().as_str());

        true
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for u64 {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }
        sql.push_str(self.to_string().as_str());

        true
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for i64 {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, "=", metadata);
        }
        sql.push_str(self.to_string().as_str());

        true
    }

    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for tokio_postgres::types::IsNull {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, " IS ", metadata);
        }

        match self {
            tokio_postgres::types::IsNull::Yes => {
                sql.push_str("NULL");
            }
            tokio_postgres::types::IsNull::No => {
                sql.push_str("NOT NULL");
            }
        }
        true
    }
    fn render_value(&self) -> bool {
        true
    }
}

impl SqlWhereValueProvider for crate::IsNull {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        _params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if let Some(full_where_condition) = full_where_condition {
            full_where_condition.render_param_name(sql, " IS ", metadata);
        }

        match self {
            crate::IsNull::Yes => {
                sql.push_str("NULL");
            }
            crate::IsNull::No => {
                sql.push_str("NOT NULL");
            }
        }
        true
    }
    fn render_value(&self) -> bool {
        true
    }
}

impl<T: SqlWhereValueProvider> SqlWhereValueProvider for Vec<T> {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        params: &mut crate::sql::SqlValues,
        metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if self.len() == 0 {
            return false;
        }

        if let Some(full_where_condition) = full_where_condition {
            if self.len() == 1 {
                self.get(0).unwrap().fill_where_value(
                    Some(full_where_condition),
                    sql,
                    params,
                    metadata,
                );
                return true;
            }
            full_where_condition.render_param_name(sql, " IN ", metadata);
        }

        sql.push('(');

        for (no, itm) in self.iter().enumerate() {
            if no > 0 {
                sql.push(',');
            }
            itm.fill_where_value(None, sql, params, metadata);
        }
        sql.push(')');

        true
    }

    fn render_value(&self) -> bool {
        self.len() > 0
    }
}

impl SqlWhereValueProvider for BTreeMap<String, String> {
    fn fill_where_value(
        &self,
        full_where_condition: Option<RenderFullWhereCondition>,
        sql: &mut String,
        params: &mut crate::sql::SqlValues,
        _metadata: &Option<SqlValueMetadata>,
    ) -> bool {
        if self.len() == 0 {
            return false;
        }

        if let Some(full_condition) = &full_where_condition {
            if full_condition.condition_no > 0 {
                sql.push_str(" AND ");
            }

            if self.len() > 1 {
                sql.push('(');
            }

            let mut condition_no = 0;
            for (key, value) in self {
                value.fill_where_value(
                    Some(RenderFullWhereCondition {
                        condition_no,
                        column_name: key.as_str().into(),
                        json_prefix: vec![full_condition.column_name],
                    }),
                    sql,
                    params,
                    &None,
                );

                condition_no += 1;
            }

            if self.len() > 1 {
                sql.push(')');
            }
        }

        true
    }

    fn render_value(&self) -> bool {
        self.len() > 0
    }
}
