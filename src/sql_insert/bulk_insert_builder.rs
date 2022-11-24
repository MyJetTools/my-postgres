use crate::SqlValue;

use super::SqlInsertModel;

pub fn build_bulk_insert<'s, TInsertModel: SqlInsertModel<'s>>(
    table_name: &str,
    models: &'s [TInsertModel],
) -> (String, Vec<&'s (dyn tokio_postgres::types::ToSql + Sync)>) {
    let mut result = String::new();

    result.push_str("INSERT INTO ");
    result.push_str(table_name);
    result.push_str(" (");

    let fields_amount = TInsertModel::get_fields_amount();

    for no in 0..fields_amount {
        if no > 0 {
            result.push(',');
        }
        result.push_str(TInsertModel::get_field_name(no));
    }

    result.push_str(" VALUES ");
    let model_no = 0;
    let mut params = Vec::new();
    for model in models {
        if model_no > 0 {
            result.push(',');
        }
        result.push('(');

        let mut written_no = 0;

        for no in 0..fields_amount {
            match model.get_field_value(no) {
                SqlValue::Ignore => {}
                SqlValue::Value { value, options } => {
                    if written_no > 0 {
                        result.push(',');
                    }

                    written_no += 1;
                    value.write(&mut result, &mut params, options.as_ref());
                }
                SqlValue::Null => {
                    if written_no > 0 {
                        result.push(',');
                    }

                    written_no += 1;
                    result.push_str("NULL");
                }
            }
        }

        result.push(')');
    }

    (result, params)
}

/*
todo!("Restore unit tests")
#[cfg(test)]
mod tests {
    use crate::code_gens::{insert::InsertCodeGen, SqlValue};

    use super::BulkInsertBuilder;

    #[test]
    fn general_test() {
        let mut builder = BulkInsertBuilder::new();

        builder.start_new_value_line();
        builder.append_field_and_value("Field1", SqlValue::Str("1"));
        builder.append_field("Field2", SqlValue::Str("2"));

        let sql = builder.build("test_table");

        assert_eq!("INSERT INTO test_table (Field1,Field2) VALUES ($1,$2)", sql)
    }
}
 */
