pub enum OrderByFields<'s> {
    Asc(Vec<&'s str>),
    Desc(Vec<&'s str>),
}

impl<'s> OrderByFields<'s> {
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
