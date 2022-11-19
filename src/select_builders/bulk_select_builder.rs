use rust_extensions::StrOrString;

use crate::{SelectEntity, ToSqlString};

pub struct BulkSelectBuilder<'s> {
    params_delta: usize,
    lines: Vec<String>,
    params: Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>,
    table_name: &'s str,
    #[cfg(feature = "with-logs-and-telemetry")]
    pub my_telemetry: Option<Vec<my_telemetry::MyTelemetryContext>>,
}

impl<'s> BulkSelectBuilder<'s> {
    pub fn new(table_name: &'s str) -> Self {
        Self {
            params_delta: 0,
            lines: Vec::new(),
            params: Vec::new(),
            table_name,
            #[cfg(feature = "with-logs-and-telemetry")]
            my_telemetry: None,
        }
    }
    pub fn append_line(
        &mut self,
        where_condition: &str,
        params: &[&'s (dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: Option<
            my_telemetry::MyTelemetryContext,
        >,
    ) {
        let where_condition =
            replace_params(where_condition, params.len(), self.params_delta).unwrap();
        self.lines.push(where_condition);

        self.params.extend(params);

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(my_telemetry) = my_telemetry {
            if self.my_telemetry.is_none() {
                self.my_telemetry = Some(Vec::new());
            }
            self.my_telemetry.as_mut().unwrap().push(my_telemetry);
        }

        self.params_delta += params.len();
    }

    fn build_sql(&'s self, select_part: &str) -> String {
        let mut result = String::new();

        let mut line_no = 0;
        for line in &self.lines {
            if line_no > 0 {
                result.push_str("UNION\n");
            }
            result.push_str("SELECT ");
            result.push_str(select_part);
            result.push_str(" FROM ");
            result.push_str(self.table_name);
            result.push_str(" WHERE ");
            result.push_str(line);
            result.push('\n');
            line_no += 1;
        }

        result
    }
}

impl<'s, TSelectEntity: SelectEntity> ToSqlString<TSelectEntity> for BulkSelectBuilder<'s> {
    fn as_sql(&self) -> StrOrString {
        StrOrString::crate_as_string(self.build_sql(TSelectEntity::get_select_fields()))
    }

    fn get_params_data(&self) -> Option<&[&(dyn tokio_postgres::types::ToSql + Sync)]> {
        Some(&self.params)
    }

    #[cfg(feature = "with-logs-and-telemetry")]
    fn get_telemetry(&self) -> Option<&[my_telemetry::MyTelemetryContext]> {
        if let Some(result) = &self.my_telemetry {
            Some(result.as_slice())
        } else {
            None
        }
    }
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
        let mut bulk_select = BulkSelectBuilder::new("test");

        bulk_select.append_line("id = $1 AND name = $2", &[&"1", &"2"]);
        bulk_select.append_line("id = $1 AND name = $2", &[&"3", &"4"]);

        let result = bulk_select.build_sql("*");
        println!("{}", result);
    }
}
