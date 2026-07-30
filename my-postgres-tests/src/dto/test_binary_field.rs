use my_postgres::macros::*;

#[derive(TableSchema)]
pub struct EntityWithBinaryField {
    #[primary_key(0)]
    #[generate_update_model(name: "PayloadUpdateModel", param_type: "where")]
    pub id: i32,

    #[generate_select_model(name: "PayloadSelectModel")]
    #[generate_update_model(name: "PayloadUpdateModel", param_type: "update")]
    pub payload: Vec<u8>,
    pub signature: Option<Vec<u8>>,

    #[sql_type("json")]
    pub as_json_array: Vec<u8>,
}

#[derive(SelectDbEntity, InsertDbEntity, UpdateDbEntity)]
pub struct BinaryKeyValue {
    #[primary_key]
    pub id: String,
    pub payload: Vec<u8>,
    pub signature: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {

    use my_postgres::{
        sql::{SelectBuilder, UsedColumns},
        table_schema::TableSchemaProvider,
    };

    use super::*;

    #[test]
    fn test_vec_of_u8_is_a_bytea_column() {
        let columns = EntityWithBinaryField::get_columns();

        assert_eq!(columns[1].name.name.as_str(), "payload");
        assert_eq!(columns[1].sql_type.as_db_type_str(), "bytea");
        assert!(!columns[1].is_nullable);

        assert_eq!(columns[2].name.name.as_str(), "signature");
        assert_eq!(columns[2].sql_type.as_db_type_str(), "bytea");
        assert!(columns[2].is_nullable);

        assert_eq!(columns[3].name.name.as_str(), "as_json_array");
        assert_eq!(columns[3].sql_type.as_db_type_str(), "json");
    }

    #[test]
    fn test_auto_generated_models_with_binary_field() {
        let select_builder = SelectBuilder::from_select_model::<PayloadSelectModel>();

        let mut sql = String::new();
        select_builder.fill_select_fields(&mut sql);

        assert_eq!("payload", sql);

        let update_model = PayloadUpdateModel {
            id: 5,
            payload: vec![1u8, 2u8],
        };

        let sql = my_postgres::sql::build_update_sql(&update_model, "test_table_name");

        assert_eq!(sql.sql, "UPDATE test_table_name SET payload=$1 WHERE id=5");
        assert_eq!(sql.values.get(0).unwrap().as_binary().unwrap(), &[1, 2]);
    }

    #[test]
    fn test_select_of_binary_field() {
        let select_builder = SelectBuilder::from_select_model::<BinaryKeyValue>();

        let mut sql = String::new();
        select_builder.fill_select_fields(&mut sql);

        assert_eq!("id,payload,signature", sql);
    }

    #[test]
    fn test_insert_of_binary_field() {
        let model = BinaryKeyValue {
            id: "id1".to_string(),
            payload: vec![1u8, 2u8, 3u8],
            signature: Some(vec![4u8, 5u8]),
        };

        let sql = my_postgres::sql::build_insert_sql(
            &model,
            "test_table_name",
            &mut UsedColumns::as_none(),
        );

        assert_eq!(
            sql.sql,
            "INSERT INTO test_table_name(id,payload,signature) VALUES ($1,$2,$3)"
        );

        assert_eq!(sql.values.len(), 3);
        assert_eq!(sql.values.get(0).unwrap().as_str().unwrap(), "id1");
        assert_eq!(sql.values.get(1).unwrap().as_binary().unwrap(), &[1, 2, 3]);
        assert_eq!(sql.values.get(2).unwrap().as_binary().unwrap(), &[4, 5]);
    }

    #[test]
    fn test_insert_of_none_binary_field() {
        let model = BinaryKeyValue {
            id: "id1".to_string(),
            payload: vec![1u8],
            signature: None,
        };

        let sql = my_postgres::sql::build_insert_sql(
            &model,
            "test_table_name",
            &mut UsedColumns::as_none(),
        );

        assert_eq!(
            sql.sql,
            "INSERT INTO test_table_name(id,payload,signature) VALUES ($1,$2,NULL)"
        );

        assert_eq!(sql.values.len(), 2);
    }

    #[test]
    fn test_update_of_binary_field() {
        let model = BinaryKeyValue {
            id: "id1".to_string(),
            payload: vec![1u8, 2u8, 3u8],
            signature: Some(vec![4u8, 5u8]),
        };

        let sql = my_postgres::sql::build_update_sql(&model, "test_table_name");

        assert_eq!(
            sql.sql,
            "UPDATE test_table_name SET (payload,signature)=($1,$2) WHERE id=$3"
        );

        assert_eq!(sql.values.get(0).unwrap().as_binary().unwrap(), &[1, 2, 3]);
        assert_eq!(sql.values.get(1).unwrap().as_binary().unwrap(), &[4, 5]);
        assert_eq!(sql.values.get(2).unwrap().as_str().unwrap(), "id1");
    }
}
