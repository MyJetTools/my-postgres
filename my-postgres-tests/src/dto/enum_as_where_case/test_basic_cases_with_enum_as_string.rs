use my_postgres::macros::{
    DbEnumAsString, InsertDbEntity, SelectDbEntity, UpdateDbEntity, WhereDbModel,
};

#[derive(SelectDbEntity, InsertDbEntity, UpdateDbEntity)]
pub struct KeyValue {
    #[primary_key]
    pub client_id: MyEnumAsString,
    #[primary_key]
    pub key: String,
    pub value: String,
}

#[derive(DbEnumAsString)]
pub enum MyEnumAsString {
    #[enum_case("TestCase1")]
    Case1,
    #[enum_case("TestCase2")]
    Case2,
}

#[cfg(test)]
mod tests {

    use my_postgres::{
        sql::{SelectBuilder, UsedColumns},
        sql_select::SelectEntity,
        UpdateConflictType,
    };

    use super::*;

    #[derive(WhereDbModel)]
    pub struct TestWhereModel {
        a: i64,
    }

    #[test]
    fn test_insert_case() {
        let model = KeyValue {
            client_id: MyEnumAsString::Case1,
            key: "key1".to_string(),
            value: "value1".to_string(),
        };

        let sql = my_postgres::sql::build_insert_sql(
            &model,
            "test_table_name",
            &mut UsedColumns::as_none(),
        );

        assert_eq!(
            sql.sql,
            "INSERT INTO test_table_name(client_id,key,value) VALUES ($1,$2,$3)"
        );

        assert_eq!(sql.values.len(), 3);
        assert_eq!(sql.values.get(0).unwrap().as_str().unwrap(), "TestCase1");
        assert_eq!(sql.values.get(1).unwrap().as_str().unwrap(), "key1");
        assert_eq!(sql.values.get(2).unwrap().as_str().unwrap(), "value1");
    }

    #[test]
    fn test_bulk_insert_or_update() {
        let models = vec![
            KeyValue {
                client_id: MyEnumAsString::Case1,
                key: "key1".to_string(),
                value: "value1".to_string(),
            },
            KeyValue {
                client_id: MyEnumAsString::Case2,
                key: "key2".to_string(),
                value: "value2".to_string(),
            },
        ];

        let sql = my_postgres::sql::build_bulk_insert_or_update_sql(
            "test",
            &UpdateConflictType::OnPrimaryKeyConstraint("pk_name".into()),
            &models,
        );

        assert_eq!(
            sql.sql,
            "INSERT INTO test(client_id,key,value) VALUES ($1,$2,$3),($4,$5,$6) ON CONFLICT ON CONSTRAINT pk_name DO UPDATE SET value=EXCLUDED.value"
        );

        assert_eq!(sql.values.len(), 6);

        assert_eq!(sql.values.get(0).unwrap().as_str().unwrap(), "TestCase1");
        assert_eq!(sql.values.get(1).unwrap().as_str().unwrap(), "key1");
        assert_eq!(sql.values.get(2).unwrap().as_str().unwrap(), "value1");

        assert_eq!(sql.values.get(3).unwrap().as_str().unwrap(), "TestCase2");
        assert_eq!(sql.values.get(4).unwrap().as_str().unwrap(), "key2");
        assert_eq!(sql.values.get(5).unwrap().as_str().unwrap(), "value2");
    }

    #[test]
    fn test_select_sql_with_no_where() {
        let select_builder = SelectBuilder::from_select_model::<KeyValue>();
        let sql = select_builder.to_sql_string::<TestWhereModel>("table_name", None);

        assert_eq!("SELECT client_id,key,value FROM table_name", sql.sql);
        assert_eq!(sql.values.len(), 0);
    }

    #[test]
    fn test_select_fields() {
        let mut builder = SelectBuilder::new();
        KeyValue::fill_select_fields(&mut builder);

        assert_eq!(builder.len(), 3);

        assert_eq!(
            builder.get(0).unwrap().unwrap_as_field().db_column_name,
            "client_id"
        );
        assert_eq!(
            builder.get(1).unwrap().unwrap_as_field().db_column_name,
            "key"
        );
        assert_eq!(
            builder.get(2).unwrap().unwrap_as_field().db_column_name,
            "value"
        );
    }

    #[test]
    fn test_select_sql_with_basic_where_case() {
        let where_model = TestWhereModel { a: 6 };

        let select_builder = SelectBuilder::from_select_model::<KeyValue>();
        let sql = select_builder.to_sql_string("table_name", Some(&where_model));

        assert_eq!(
            "SELECT client_id,key,value FROM table_name WHERE a=6",
            sql.sql
        );
        assert_eq!(sql.values.len(), 0);
    }

    #[test]
    fn test_update_case() {
        let entity = KeyValue {
            client_id: MyEnumAsString::Case1,
            key: "key1".to_string(),
            value: "value1".to_string(),
        };

        let sql = my_postgres::sql::build_update_sql(&entity, "test");

        assert_eq!(
            sql.sql,
            "UPDATE test SET value=$1 WHERE client_id=$2 AND key=$3"
        );

        assert_eq!(sql.values.len(), 3);

        assert_eq!(sql.values.get(0).unwrap().as_str().unwrap(), "value1");
        assert_eq!(sql.values.get(1).unwrap().as_str().unwrap(), "TestCase1");
        assert_eq!(sql.values.get(2).unwrap().as_str().unwrap(), "key1");
    }
}
