use crate::{
    sql::{SelectBuilder, SqlValues},
    sql_select::SelectEntity,
    sql_where::SqlWhereModel,
};

pub struct UnionModel<TSelectEntity: SelectEntity, TWhereModel: SqlWhereModel> {
    pub where_model: TWhereModel,
    pub items: Vec<TSelectEntity>,
}

pub fn compile_union_select<TSelectEntity: SelectEntity, TWhereModel: SqlWhereModel>(
    sql: &mut String,
    values: &mut SqlValues,
    table_name: &str,
    where_models: &[TWhereModel],
) {
    let mut where_no = 0;

    for where_model in where_models {
        if where_no > 0 {
            sql.push_str(" UNION (");
        } else {
            sql.push_str("(");
        }

        let mut select_builder = SelectBuilder::from_select_model::<TSelectEntity>();
        select_builder.bulk_where_no = Some(where_no);
        select_builder.build_select_sql(sql, values, table_name, Some(where_model));

        sql.push(')');

        where_no += 1;
    }
}
