use my_postgres::macros::{DbEnumAsString, WhereDbModel};
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(WhereDbModel)]
pub struct TestWhereModel {
    #[db_column_name("id")]
    pub name: String,

    #[sql_type("timestamp")]
    pub date_time: DateTimeAsMicroseconds,

    #[operator(">")]
    pub i32: i32,

    pub opt_i32: Option<i32>,

    #[ignore_if_none]
    pub ignore_if_null: Option<i32>,

    pub str_enum: MyWhereStringEnum,

    pub str_enum_opt: Option<MyWhereStringEnum>,
    #[ignore_if_none]
    pub str_enum_opt_to_ignore: Option<MyWhereStringEnum>,

    pub str_enum_opt2: Option<MyWhereStringEnum>,

    #[limit]
    pub limit: usize,

    #[operator("like")]
    pub like_value: String,

    #[inside_json("field_name.field_name2")]
    pub my_json_field: String,
}

#[derive(DbEnumAsString)]
pub enum MyWhereStringEnum {
    #[enum_case("CaseTest1")]
    Case1,
    #[enum_case("CaseTest2")]
    Case2,
}

#[cfg(test)]
mod tests {
    use my_postgres::{sql::SqlValues, sql_where::SqlWhereModel};
    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use super::TestWhereModel;

    #[test]
    fn test_generating_where_part() {
        let date_time =
            DateTimeAsMicroseconds::parse_iso_string("2023-06-19T22:07:20.518741+00:00").unwrap();
        let where_model = TestWhereModel {
            name: "test".to_string(),
            date_time,
            i32: 1,
            opt_i32: None,
            ignore_if_null: None,
            str_enum: super::MyWhereStringEnum::Case1,
            limit: 10,
            str_enum_opt: None,
            str_enum_opt_to_ignore: None,
            str_enum_opt2: Some(super::MyWhereStringEnum::Case2),
            like_value: "%test%".to_string(),
            my_json_field: "test".to_string(),
        };

        let mut params = SqlValues::new();
        let mut sql = String::new();
        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!("id=$1 AND date_time='2023-06-19T22:07:20.518741+00:00' AND i32>1 AND opt_i32 IS NULL AND str_enum=$2 AND str_enum_opt IS NULL AND str_enum_opt2=$3 AND like_value like $4 AND my_json_field=$1", sql);

        assert_eq!(params.get(0).unwrap().as_str().unwrap(), "test");
        assert_eq!(params.get(1).unwrap().as_str().unwrap(), "CaseTest1");
        assert_eq!(params.get(2).unwrap().as_str().unwrap(), "CaseTest2");
        assert_eq!(params.get(3).unwrap().as_str().unwrap(), "%test%");
    }
}
