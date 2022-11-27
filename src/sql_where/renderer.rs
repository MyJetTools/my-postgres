use crate::SqlValueWriter;

pub struct WhereRenderer {
    no: usize,
}

impl WhereRenderer {
    pub fn new() -> Self {
        Self { no: 0 }
    }

    fn add_delimiter(&mut self, sql: &mut String) {
        if self.no > 0 {
            sql.push_str(" AND ");
        } else {
            sql.push(' ');
            self.no += 1;
        }
    }

    pub fn add_value<'s, TSqlValueWriter: SqlValueWriter<'s>>(
        &'s mut self,
        sql: &mut String,
        name: &'s str,
        op: &'s str,
        value: &'s TSqlValueWriter,
        params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        sql_type: Option<&'static str>,
    ) {
        self.add_delimiter(sql);
        sql.push_str(name);
        sql.push_str(op);
        value.write(sql, params, sql_type);
    }

    pub fn add_optional_value<'s, TSqlValueWriter: SqlValueWriter<'s>>(
        &'s mut self,
        sql: &mut String,
        name: &'s str,
        op: &'s str,
        value: &'s Option<TSqlValueWriter>,
        params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        sql_type: Option<&'static str>,
    ) {
        if let Some(value) = value {
            self.add_value(sql, name, op, value, params, sql_type);
        }
    }

    pub fn add_vec<'s, TSqlValueWriter: SqlValueWriter<'s>>(
        &'s mut self,
        sql: &mut String,
        name: &'static str,
        values: &'s Vec<TSqlValueWriter>,
        params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        sql_type: Option<&'static str>,
    ) {
        if values.len() == 0 {
            return;
        }

        if values.len() == 1 {
            self.add_delimiter(sql);
            sql.push_str(name);
            sql.push('=');
            values.get(0).unwrap().write(sql, params, sql_type);
            return;
        }

        self.add_delimiter(sql);
        sql.push_str(name);
        sql.push_str(" IN (");
        let mut no = 0;
        for value in values {
            if no > 0 {
                sql.push(',');
            }
            no += 1;
            value.write(sql, params, sql_type);
        }

        sql.push(')');
    }

    pub fn add_opt_of_vec<'s, TSqlValueWriter: SqlValueWriter<'s>>(
        &'s mut self,
        sql: &mut String,
        name: &'static str,
        values: &'s Option<Vec<TSqlValueWriter>>,
        params: &mut Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
        sql_type: Option<&'static str>,
    ) {
        if let Some(values) = values {
            self.add_vec(sql, name, values, params, sql_type);
        }
    }
}
