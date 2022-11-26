pub enum OrderByFields {
    Asc(Vec<&'static str>),
    Desc(Vec<&'static str>),
}

impl OrderByFields {
    pub fn fill_sql(&self, sql: &mut String) {
        match self {
            Self::Asc(fields) => {
                sql.push_str(" ORDER BY ");

                for (no, field) in fields.into_iter().enumerate() {
                    if no > 0 {
                        sql.push_str(",");
                    }

                    sql.push_str(field);
                }
            }
            Self::Desc(fields) => {
                sql.push_str(" ORDER BY ");

                for (no, field) in fields.into_iter().enumerate() {
                    if no > 0 {
                        sql.push_str(",");
                    }

                    sql.push_str(field);
                }

                sql.push_str(" DESC");
            }
        }
    }
}
