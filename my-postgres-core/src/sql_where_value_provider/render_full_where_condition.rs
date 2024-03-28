use crate::SqlValueMetadata;

pub struct RenderFullWhereCondition<'s> {
    pub condition_no: usize,
    pub column_name: &'s str,
    pub json_prefix: Vec<&'s str>,
}

impl<'s> RenderFullWhereCondition<'s> {
    pub fn render_param_name(
        &self,
        sql: &mut String,
        default_op: &str,
        metadata: &Option<SqlValueMetadata>,
    ) {
        if self.condition_no > 0 {
            sql.push_str(" AND ");
        }

        if self.json_prefix.len() > 0 {
            for (no, prefix) in self.json_prefix.iter().enumerate() {
                if no == 0 {
                    sql.push('"');
                } else {
                    sql.push('\'');
                }

                sql.push_str(prefix);
                if no == 0 {
                    sql.push('"');
                } else {
                    sql.push('\'');
                }
                sql.push_str("->>")
            }

            sql.push('\'');
            sql.push_str(self.column_name);
            sql.push('\'');
        } else {
            sql.push_str(self.column_name);
        }

        push_where_operator(sql, default_op, metadata);
    }
}

pub fn push_where_operator(sql: &mut String, default: &str, metadata: &Option<SqlValueMetadata>) {
    if let Some(metadata) = metadata {
        if let Some(operator_override) = metadata.operator {
            sql.push_str(operator_override);
            return;
        }
    }

    sql.push_str(default);
}
