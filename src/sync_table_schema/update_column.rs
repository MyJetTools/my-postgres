use rust_extensions::StrOrString;

use crate::{
    table_schema::{ColumnDifference, TableColumnType, DEFAULT_SCHEMA},
    PostgresConnection,
};

pub async fn update_column(
    conn_string: &PostgresConnection,
    table_name: &str,
    column_name: &str,
    differences: Vec<ColumnDifference>,
) -> Result<(), String> {
    for difference in differences {
        if difference
            .db
            .sql_type
            .equals_to(&difference.required.sql_type)
        {
            try_to_update_column_type(
                conn_string,
                table_name,
                column_name,
                difference.required.sql_type,
            )
            .await?;

            return Ok(());
        }

        if difference.db.is_nullable && !difference.required.is_nullable {
            try_to_update_is_nullable(
                conn_string,
                table_name,
                column_name,
                difference.required.is_nullable,
            )
            .await?;
            return Ok(());
        }

        if !difference.db.is_default_the_same(&difference.required) {
            try_to_update_default(
                conn_string,
                table_name,
                column_name,
                &difference.db.default,
                &difference.required.default,
            )
            .await?;
            return Ok(());
        }
    }

    Ok(())
}

async fn try_to_update_is_nullable(
    conn_string: &PostgresConnection,
    table_name: &str,
    column_name: &str,
    required_to_be_nullable: bool,
) -> Result<(), String> {
    if required_to_be_nullable {
        let sql = format!(
            r#"alter table {DEFAULT_SCHEMA}.{table_name}
        alter column {column_name} drop not null;"#
        );

        conn_string
            .execute_sql(
                &sql.into(),
                None,
                #[cfg(feature = "with-logs-and-telemetry")]
                None,
            )
            .await
            .unwrap();

        return Ok(());
    }

    let sql = format!(
        r#"alter table {DEFAULT_SCHEMA}.{table_name}
    alter column {column_name} set not null;"#
    );

    match conn_string
        .execute_sql(
            &sql.clone().into(),
            None,
            #[cfg(feature = "with-logs-and-telemetry")]
            None,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            return Err(format!("Failed to execute {}. Reason: {:?}", sql, err));
        }
    }
}

async fn try_to_update_column_type(
    conn_string: &PostgresConnection,
    table_name: &str,
    column_name: &str,
    required_type: TableColumnType,
) -> Result<(), String> {
    let db_type = required_type.to_db_type();

    let sql = format!(
        r#"alter table {DEFAULT_SCHEMA}.{table_name}
    alter column {column_name} type {db_type} using test::{db_type};"#
    );

    match conn_string
        .execute_sql(
            &sql.clone().into(),
            None,
            #[cfg(feature = "with-logs-and-telemetry")]
            None,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            return Err(format!("Failed to execute {}. Reason: {:?}", sql, err));
        }
    }
}

async fn try_to_update_default(
    conn_string: &PostgresConnection,
    table_name: &str,
    column_name: &str,
    now_default: &Option<StrOrString<'static>>,
    required_default: &Option<StrOrString<'static>>,
) -> Result<(), String> {
    let sql = if let Some(now_default) = now_default {
        if let Some(required_default) = required_default {
            if required_default.as_str() == now_default.as_str() {
                println!("BUG: We should not be here: #1");
                return Ok(());
            } else {
                format!(
                    r#"alter table {DEFAULT_SCHEMA}.{table_name}
                    alter column {column_name} set default {now_default}"#,
                    now_default = now_default.as_str()
                )
            }
        } else {
            format!(
                r#"alter table {DEFAULT_SCHEMA}.{table_name}
                alter column {column_name} drop default"#
            )
        }
    } else {
        if let Some(required_default) = required_default {
            format!(
                r#"alter table {DEFAULT_SCHEMA}.{table_name}
           alter column {column_name} set default {now_default}"#,
                now_default = required_default.as_str()
            )
        } else {
            println!("BUG: We should not be here: #2");
            return Ok(());
        }
    };

    match conn_string
        .execute_sql(
            &sql.clone().into(),
            None,
            #[cfg(feature = "with-logs-and-telemetry")]
            None,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            return Err(format!("Failed to execute {}. Reason: {:?}", sql, err));
        }
    }
}
