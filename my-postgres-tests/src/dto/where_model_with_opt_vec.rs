use my_postgres::macros::*;

#[derive(DbEnumAsU8)]
pub enum BrokerDto {
    Broker,
    Broker2,
}

//#[derive(WhereDbModel)]
pub struct WhereModel {
    //  #[ignore_if_none]
    pub account_id: Option<Vec<String>>,
    pub field_2: BrokerDto,
}

impl my_postgres::sql_where::SqlWhereModel for WhereModel {
    fn fill_where_component(&self, sql: &mut String, params: &mut my_postgres::sql::SqlValues) {
        use my_postgres::SqlWhereValueProvider;
        let mut condition_no = 0;
        if let Some(value) = &self.account_id {
            if value.fill_where_value(
                Some(my_postgres::RenderFullWhereCondition {
                    column_name: "account_id",
                    condition_no,
                    json_prefix: None,
                }),
                sql,
                params,
                &None,
            ) {
                condition_no += 1;
            }
        }
        if self.field_2.fill_where_value(
            Some(my_postgres::RenderFullWhereCondition {
                column_name: "field_2",
                condition_no,
                json_prefix: None,
            }),
            sql,
            params,
            &None,
        ) {
            condition_no += 1;
        }
    }
    fn has_conditions(&self) -> bool {
        true
    }
    fn get_limit(&self) -> Option<usize> {
        None
    }
    fn get_offset(&self) -> Option<usize> {
        None
    }
}

#[cfg(test)]
mod tests {
    use my_postgres::{sql::SqlValues, sql_where::SqlWhereModel};

    use super::*;

    #[test]
    fn test() {
        let where_model = WhereModel {
            account_id: Some(Vec::new()),
            field_2: BrokerDto::Broker,
        };

        let mut params = SqlValues::new();
        let mut sql = String::new();
        where_model.fill_where_component(&mut sql, &mut params);

        assert_eq!("field_name=$1", sql);
    }
}
