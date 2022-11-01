use crate::SqlValueAsString;

pub fn format_sql<'src, TGetInject: Fn() -> &'static str>(
    sql: &'src str,
    inejct: TGetInject,
) -> SqlValueAsString<'src> {
    if let Some((from, to)) = split_sql(sql) {
        let inject = inejct();
        return SqlValueAsString::String(format!("{}{}{}", from, inject, to));
    }

    SqlValueAsString::Str(sql)
}

fn split_sql<'s>(sql: &'s str) -> Option<(&'s str, &'s str)> {
    let star = sql.find('*')?;

    Some((&sql[..star], &sql[star + 1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_sql() {
        let sql = "SELECT * FROM table";
        let result = format_sql(sql, || "id, name");
        assert_eq!(result.as_str(), "SELECT id, name FROM table");
    }
}
