use my_postgres::macros::*;
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(SelectDbEntity, InsertDbEntity, TableSchema, Debug)]
pub struct MyDbModel {
    #[primary_key(0)]
    pub client_id: String,
    #[sql_type("timestamp")]
    #[generate_select_model("DocumentDtoWithNoContent")]
    #[generate_update_model(name:"DeletePostgresModel", param_type="update")]
    #[generate_update_model(name:"UpdatePostgresModel", param_type="update")]
    pub uploaded_at: DateTimeAsMicroseconds,
}

#[cfg(test)]
mod tests {
    use my_postgres::{
        sql::{SelectBuilder, SqlValues},
        sql_select::SelectEntity,
        sql_update::SqlUpdateModel,
        sql_where::{NoneWhereModel, SqlWhereModel},
    };
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use crate::dto::test_generated_models::test_timestamp::UpdatePostgresModel;

    use super::{DeletePostgresModel, DocumentDtoWithNoContent};

    #[test]
    fn test() {
        let mut select_builder = SelectBuilder::new();
        DocumentDtoWithNoContent::fill_select_fields(&mut select_builder);

        let mut sql = String::new();
        let mut sql_values = SqlValues::new();
        select_builder.build_select_sql(&mut sql, &mut sql_values, "Test", Some(&NoneWhereModel));

        let mut sql = String::new();
        let mut sql_values = SqlValues::new();
        let delete_model = DeletePostgresModel {
            uploaded_at: DateTimeAsMicroseconds::now(),
        };
        delete_model.fill_where_component(&mut sql, &mut sql_values);

        let sql_data = delete_model.build_delete_sql("Test");

        println!("{}", sql_data.sql);
    }

    #[test]
    fn test_update_case() {
        let mut sql = String::new();
        let mut sql_values = SqlValues::new();
        let update_model = UpdatePostgresModel {
            uploaded_at: DateTimeAsMicroseconds::now(),
        };
        update_model.fill_update_values(&mut sql, &mut sql_values);

        update_model.build_update_sql_part(&mut sql, &mut sql_values);

        println!("{}", sql);
    }
}
