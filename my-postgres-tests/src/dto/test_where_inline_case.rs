use my_postgres::macros::{where_raw_model, WhereDbModel};

#[where_raw_model("Content3 in ${field_3}")]
pub struct WhereRawModel {
    pub field_3: Vec<i32>,
}

#[derive(WhereDbModel)]
pub struct WhereModel {
    pub field_1: String,
    #[inline_where_model]
    pub inline_filed: WhereRawModel,
    pub field_3: i32,
}

#[cfg(test)]
mod test {

    use my_postgres::sql_where::SqlWhereModel;

    use super::*;

    #[test]
    fn test_we_have_no_value_inside_raw_model() {
        let where_model = WhereModel {
            field_1: "test".to_string(),
            inline_filed: WhereRawModel { field_3: vec![] },
            field_3: 3,
        };

        let mut params = my_postgres::sql::SqlValues::new();
        let mut sql = String::new();

        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!("field_1=$1 AND field_3=3", sql.as_str());
    }

    #[test]
    fn test_we_have_single_value_inside_raw_model() {
        let where_model = WhereModel {
            field_1: "test".to_string(),
            inline_filed: WhereRawModel { field_3: vec![15] },
            field_3: 3,
        };

        let mut params = my_postgres::sql::SqlValues::new();
        let mut sql = String::new();

        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!(
            "field_1=$1 AND (Content3 in (15)) AND field_3=3",
            sql.as_str()
        );
    }
}
