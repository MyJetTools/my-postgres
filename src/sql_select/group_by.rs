pub struct GroupByFields {
    pub fields: Vec<&'static str>,
}

impl GroupByFields {
    pub fn fill_sql(&self, sql: &mut String) {
        sql.push_str(" GROUP BY ");

        let mut no = 0;
        for field in &self.fields {
            if no > 0 {
                sql.push_str(",");
            }
            no += 1;

            sql.push_str(field);
        }
    }
}
