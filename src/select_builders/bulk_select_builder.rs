pub struct BulkSelectBuilder<'s, TIn> {
    pub input_params: Vec<TIn>,
    pub table_name: &'s str,
    where_line: &'s str,
}

impl<'s, TIn> BulkSelectBuilder<'s, TIn> {
    pub fn new(table_name: &'s str, where_line: &'s str, input_params: Vec<TIn>) -> Self {
        Self {
            table_name,
            input_params,
            where_line,
        }
    }

    pub fn build_sql(&'s self, select_part: &str) -> String {
        let mut result = String::new();

        let mut line_no = 0;
        let params_amount = get_params_amount(&self.where_line);

        for no in 0..self.input_params.len() {
            if line_no > 0 {
                result.push_str("UNION ALL\n");
            }
            result.push_str("SELECT ");
            result.push_str(line_no.to_string().as_str());
            result.push_str("::int as line_no, ");
            result.push_str(select_part);
            result.push_str(" FROM ");
            result.push_str(self.table_name);
            result.push_str(" WHERE ");

            if let Some(params_amount) = params_amount {
                let line =
                    replace_params(self.where_line, params_amount, no * params_amount).unwrap();
                result.push_str(line.as_str());
            } else {
                result.push_str(self.where_line);
            }

            result.push('\n');
            line_no += 1;
        }

        result
    }

    pub fn get_params_data<
        TMap: Fn(&'s TIn, usize) -> &'s (dyn tokio_postgres::types::ToSql + Sync),
    >(
        &'s self,
        mapper: TMap,
    ) -> Option<Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>> {
        let params_amount = get_params_amount(self.where_line)?;

        let mut result = Vec::new();

        for in_param in &self.input_params {
            for no in 1..params_amount + 1 {
                result.push(mapper(in_param, no));
            }
        }

        Some(result)
    }
}

fn get_params_amount(src: &str) -> Option<usize> {
    let mut result = None;

    let mut param_started = None;

    let bytes = src.as_bytes();

    for i in 0..bytes.len() {
        let b = bytes[i];

        if let Some(params_started) = param_started {
            if b >= b'0' && b <= b'9' {
                continue;
            } else {
                let param_no = &src[params_started + 1..i];
                let param_no = param_no.parse::<usize>().unwrap();

                match &mut result {
                    Some(result) => {
                        if *result < param_no {
                            *result = param_no;
                        }
                    }
                    None => {
                        result = Some(param_no);
                    }
                }

                param_started = None;
                continue;
            }
        }

        if b == b'$' {
            param_started = Some(i);
            continue;
        }
    }

    if let Some(params_started) = param_started {
        let param_no = &src[params_started + 1..bytes.len()];
        let param_no = param_no.parse::<usize>().unwrap();

        match &mut result {
            Some(result) => {
                if *result < param_no {
                    *result = param_no;
                }
            }
            None => {
                result = Some(param_no);
            }
        }
    }

    result
}

fn replace_params(
    where_condition: &str,
    params_amount: usize,
    param_no_delta: usize,
) -> Result<String, String> {
    if params_amount == 0 || param_no_delta == 0 {
        return Ok(where_condition.to_string());
    }

    let bytes = where_condition.as_bytes();

    let mut result = String::new();

    let mut param_started = None;

    for i in 0..where_condition.len() {
        let b = bytes[i];

        if let Some(params_started) = param_started {
            if b >= b'0' && b <= b'9' {
                continue;
            } else {
                let param_no = &where_condition[params_started + 1..i];
                let param_no = param_no.parse::<usize>().unwrap();

                if param_no > params_amount {
                    let err = format!(
                        "Max params amount is: {}. But found param no ${} in line {}",
                        params_amount, param_no, where_condition
                    );

                    return Err(err);
                }
                result.push_str(&format!("${}", param_no + param_no_delta));
                result.push(b as char);
                param_started = None;
                continue;
            }
        }

        if b == b'$' {
            param_started = Some(i);
            continue;
        }

        result.push(b as char);
    }

    if let Some(params_started) = param_started {
        let param_no = &where_condition[params_started + 1..bytes.len()];
        let param_no = param_no.parse::<usize>().unwrap();
        if param_no > params_amount {
            let err = format!(
                "Max params amount is: {}. But found param no ${} in line {}",
                params_amount, param_no, where_condition
            );
            return Err(err);
        }
        result.push_str(&format!("${}", param_no + param_no_delta));
    }

    Ok(result)
}

#[cfg(test)]
#[cfg(not(feature = "with-logs-and-telemetry"))]
mod tests {
    use crate::BulkSelectBuilder;

    #[test]
    fn test_replace_with_no_delta() {
        let where_condition = "id = $1 AND name = $2";

        let result = super::replace_params(where_condition, 2, 0).unwrap();

        assert_eq!(result, where_condition);
    }

    #[test]
    fn bug_with_replace_with_param_amounts() {
        let where_condition = "id = $1 AND name = $2";

        let result = super::replace_params(where_condition, 1, 1);

        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_replace_with_delta_1() {
        let where_condition = "id = $1 AND name = $2";

        let result = super::replace_params(where_condition, 2, 1).unwrap();

        assert_eq!(result, "id = $2 AND name = $3");
    }

    #[test]
    fn test_build_sql() {
        struct Param {
            q1: &'static str,
            q2: &'static str,
        }

        let params = vec![Param { q1: "1", q2: "2" }, Param { q1: "3", q2: "4" }];

        let bulk_select = BulkSelectBuilder::new("test", "id = $1 AND name = $2", params);

        let a = bulk_select.get_params_data(|p, no| match no {
            1 => &p.q1,
            2 => &p.q2,
            _ => panic!("Unexpected param no"),
        });

        println!("{:?}", a);

        let result = bulk_select.build_sql("*");
        println!("{}", result);
    }
}
