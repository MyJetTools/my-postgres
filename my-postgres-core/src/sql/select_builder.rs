use rust_extensions::StrOrString;

use crate::{sql_select::SelectEntity, sql_where::SqlWhereModel, DbColumnName, SqlValueMetadata};

use super::{SqlData, SqlValues};

pub enum SelectFieldValue {
    LineNo(usize),
    Field {
        column_name: DbColumnName,
        wrap_column_name: Option<&'static str>,
    },
    FieldWithCast {
        column_name: DbColumnName,
        cast_to: &'static str,
    },
    Json(DbColumnName),
    DateTimeAsBigint(DbColumnName),
    DateTimeAsTimestamp(DbColumnName),
    GroupByField {
        statement: StrOrString<'static>,
        column_name: DbColumnName,
    },
}

impl SelectFieldValue {
    pub fn create_as_field(column_name: DbColumnName, metadata: &Option<SqlValueMetadata>) -> Self {
        if let Some(metadata) = metadata {
            return Self::Field {
                column_name,
                wrap_column_name: metadata.wrap_column_name,
            };
        }

        Self::Field {
            column_name,
            wrap_column_name: None,
        }
    }

    pub fn unwrap_as_line_no(&self) -> usize {
        match self {
            SelectFieldValue::LineNo(line_no) => *line_no,
            SelectFieldValue::Field {
                column_name,
                wrap_column_name,
            } => panic!(
                "Value is Field: {:?} with wrap_column_name: {:?}",
                column_name, wrap_column_name
            ),
            SelectFieldValue::FieldWithCast {
                column_name,
                cast_to,
            } => {
                panic!("Value is Field: {:?} with Cast to {}", column_name, cast_to)
            }
            SelectFieldValue::Json(field_name) => panic!("Value is Json: {:?}", field_name),
            SelectFieldValue::DateTimeAsBigint(field_name) => {
                panic!("Value is DateTimeAsBigint: {:?}", field_name)
            }
            SelectFieldValue::DateTimeAsTimestamp(field_name) => {
                panic!("Value is DateTimeAsTimestamp: {:?}", field_name)
            }
            SelectFieldValue::GroupByField { column_name, .. } => {
                panic!("Value is GroupByField: {:?}", column_name)
            }
        }
    }

    pub fn unwrap_as_field(&self) -> &DbColumnName {
        match self {
            SelectFieldValue::LineNo(line_no) => panic!("Value is LineNo: {}", line_no),
            SelectFieldValue::Field { column_name, .. } => column_name,
            SelectFieldValue::FieldWithCast {
                column_name,
                cast_to: _,
            } => column_name,
            SelectFieldValue::Json(db_column_name) => panic!("Value is Json: {:?}", db_column_name),
            SelectFieldValue::DateTimeAsBigint(db_column_name) => {
                panic!("Value is DateTimeAsBigint: {:?}", db_column_name)
            }
            SelectFieldValue::DateTimeAsTimestamp(db_column_name) => {
                panic!("Value is DateTimeAsTimestamp: {:?}", db_column_name)
            }
            SelectFieldValue::GroupByField { column_name, .. } => {
                panic!("Value is GroupByField: {:?}", column_name)
            }
        }
    }

    pub fn unwrap_as_json(&self) -> &DbColumnName {
        match self {
            SelectFieldValue::LineNo(line_no) => panic!("Value is LineNo: {}", line_no),
            SelectFieldValue::Field {
                column_name,
                wrap_column_name,
            } => panic!(
                "Value is Field: {:?} with wrap_column_name: {:?}",
                column_name, wrap_column_name
            ),
            SelectFieldValue::FieldWithCast {
                column_name,
                cast_to,
            } => {
                panic!("Value is Field: {:?} with Cast to {}", column_name, cast_to)
            }
            SelectFieldValue::Json(field_name) => field_name,
            SelectFieldValue::DateTimeAsBigint(field_name) => {
                panic!("Value is DateTimeAsBigint: {:?}", field_name)
            }
            SelectFieldValue::DateTimeAsTimestamp(field_name) => {
                panic!("Value is DateTimeAsTimestamp: {:?}", field_name)
            }
            SelectFieldValue::GroupByField { column_name, .. } => {
                panic!("Value is GroupByField: {:?}", column_name)
            }
        }
    }

    pub fn unwrap_as_date_time_as_bigint(&self) -> &DbColumnName {
        match self {
            SelectFieldValue::LineNo(line_no) => panic!("Value is LineNo: {}", line_no),
            SelectFieldValue::Field {
                column_name,
                wrap_column_name,
            } => panic!(
                "Value is Field: {:?} with wrap_column_name: {:?}",
                column_name, wrap_column_name
            ),
            SelectFieldValue::FieldWithCast {
                column_name,
                cast_to,
            } => {
                panic!("Value is Field: {:?} with Cast to {}", column_name, cast_to)
            }
            SelectFieldValue::Json(field_name) => panic!("Value is Json: {:?}", field_name),
            SelectFieldValue::DateTimeAsBigint(field_name) => field_name,
            SelectFieldValue::DateTimeAsTimestamp(field_name) => {
                panic!("Value is DateTimeAsTimestamp: {:?}", field_name)
            }
            SelectFieldValue::GroupByField { column_name, .. } => {
                panic!("Value is GroupByField: {:?}", column_name)
            }
        }
    }

    pub fn unwrap_as_date_time_as_timestamp(&self) -> &DbColumnName {
        match self {
            SelectFieldValue::LineNo(line_no) => panic!("Value is LineNo: {}", line_no),
            SelectFieldValue::Field {
                column_name,
                wrap_column_name,
            } => panic!(
                "Value is Field: {:?} with wrap_column_name: {:?}",
                column_name, wrap_column_name
            ),
            SelectFieldValue::FieldWithCast {
                column_name,
                cast_to,
            } => {
                panic!("Value is Field: {:?} with Cast to {}", column_name, cast_to)
            }
            SelectFieldValue::Json(field_name) => panic!("Value is Json: {:?}", field_name),
            SelectFieldValue::DateTimeAsBigint(field_name) => {
                panic!("Value is DateTimeAsBigint: {:?}", field_name)
            }
            SelectFieldValue::GroupByField { column_name, .. } => {
                panic!("Value is GroupByField: {:?}", column_name)
            }
            SelectFieldValue::DateTimeAsTimestamp(field_name) => field_name,
        }
    }
}

pub struct SelectBuilder {
    pub bulk_where_no: Option<i64>,
    items: Vec<SelectFieldValue>,
    order_by_columns: Option<&'static str>,
    group_by_columns: Option<&'static str>,
}

impl SelectBuilder {
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            order_by_columns: None,
            group_by_columns: None,
            bulk_where_no: None,
        }
    }

    pub fn from_select_model<TSelectEntity: SelectEntity>() -> Self {
        let mut builder = Self::new();
        TSelectEntity::fill_select_fields(&mut builder);

        builder.group_by_columns = TSelectEntity::get_group_by_fields();
        builder.order_by_columns = TSelectEntity::get_order_by_fields();

        builder
    }

    pub fn push(&mut self, value: SelectFieldValue) {
        self.items.push(value)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn get(&self, index: usize) -> Option<&SelectFieldValue> {
        self.items.get(index)
    }

    pub fn fill_select_fields(&self, sql: &mut String) {
        fill_select_fields(sql, &self.items)
    }

    pub fn to_sql_string<TSqlWhereModel: SqlWhereModel>(
        &self,
        table_name: &str,
        where_model: Option<&TSqlWhereModel>,
    ) -> SqlData {
        let mut sql = String::new();
        let mut values = SqlValues::new();
        build_select(
            &mut sql,
            &mut values,
            table_name,
            self.items.as_slice(),
            where_model,
            self.order_by_columns,
            self.group_by_columns,
            self.bulk_where_no,
        );

        SqlData { sql, values }
    }

    pub fn build_select_sql<TSqlWhereModel: SqlWhereModel>(
        &self,
        sql: &mut String,
        values: &mut SqlValues,
        table_name: &str,
        where_model: Option<&TSqlWhereModel>,
    ) {
        build_select(
            sql,
            values,
            table_name,
            self.items.as_slice(),
            where_model,
            self.order_by_columns,
            self.group_by_columns,
            self.bulk_where_no,
        );
    }
}

pub fn build_select<TSqlWhereModel: SqlWhereModel>(
    sql: &mut String,
    values: &mut SqlValues,
    table_name: &str,
    items: &[SelectFieldValue],
    where_model: Option<&TSqlWhereModel>,
    order_by_columns: Option<&'static str>,
    group_by_columns: Option<&'static str>,
    bulk_where_no: Option<i64>,
) {
    sql.push_str("SELECT ");

    if let Some(bulk_where_no) = bulk_where_no {
        sql.push_str(bulk_where_no.to_string().as_str());
        sql.push_str("::int as where_no,");
    }

    fill_select_fields(sql, items);

    sql.push_str(" FROM ");
    sql.push_str(table_name);

    if let Some(where_model) = where_model {
        if where_model.has_conditions() {
            sql.push_str(" WHERE ");
            where_model.fill_where_component(sql, values);
        }
    }

    if let Some(order_by_fields) = order_by_columns {
        sql.push_str(order_by_fields);
    }

    if let Some(group_by_fields) = group_by_columns {
        sql.push_str(group_by_fields);
    }

    if let Some(where_model) = where_model {
        where_model.fill_limit_and_offset(sql);
    }
}

pub fn fill_select_fields(sql: &mut String, items: &[SelectFieldValue]) {
    let mut no = 0;
    for value in items {
        if no > 0 {
            sql.push_str(",");
        }

        match value {
            SelectFieldValue::Field {
                column_name,
                wrap_column_name,
            } => {
                if let Some(wrap_column_name) = wrap_column_name {
                    let mut sides = wrap_column_name.split("${}");

                    if let Some(value) = sides.next() {
                        sql.push_str(value);
                    }

                    sql.push_str(&column_name.db_column_name);

                    if let Some(value) = sides.next() {
                        sql.push_str(value);
                    }
                } else {
                    sql.push_str(&column_name.db_column_name);
                }
            }
            SelectFieldValue::FieldWithCast {
                column_name,
                cast_to,
            } => {
                sql.push_str(&column_name.db_column_name);
                sql.push_str("::");
                sql.push_str(cast_to);
            }
            SelectFieldValue::Json(field_name) => {
                sql.push_str(field_name.db_column_name);
                sql.push_str(" #>> '{}' as \"");
                sql.push_str(field_name.field_name);
                sql.push('"');
            }
            SelectFieldValue::DateTimeAsTimestamp(field_name) => {
                sql.push_str("(extract(EPOCH FROM ");
                sql.push_str(field_name.db_column_name);
                sql.push_str(") * 1000000)::bigint as \"");
                crate::utils::fill_adjusted_column_name(field_name.db_column_name, sql);
                sql.push('"');
            }
            SelectFieldValue::DateTimeAsBigint(field_name) => {
                sql.push_str(field_name.db_column_name);
            }
            SelectFieldValue::LineNo(line_no) => {
                sql.push_str(format!("{}::int as \"line_no\"", line_no).as_str());
            }
            SelectFieldValue::GroupByField {
                column_name,
                statement,
            } => {
                sql.push_str(
                    format!("{} as \"{}\"", statement.as_str(), column_name.field_name).as_str(),
                );
            }
        }

        no += 1;
    }
}
