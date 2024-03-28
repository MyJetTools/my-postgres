use my_postgres::macros::{where_raw_model, WhereDbModel};

#[where_raw_model("Content=${field_1} OR Content2=${field_2} OR Content3 in ${field_3}")]
pub struct WhereRawModel {
    pub field_1: String,
    pub field_2: bool,
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
    fn test_generation() {
        let where_model = WhereModel {
            field_1: "test".to_string(),
            inline_filed: WhereRawModel {
                field_1: "test2".to_string(),
                field_2: true,
                field_3: vec![1, 2, 3],
            },
            field_3: 3,
        };

        let mut params = my_postgres::sql::SqlValues::new();
        let mut sql = String::new();

        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!(
            "field_1=$1 AND (Content=$2 OR Content2=true OR Content3 in (1,2,3)) AND field_3=3",
            sql.as_str()
        );
    }
}
