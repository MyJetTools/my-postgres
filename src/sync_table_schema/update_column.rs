use crate::{
    table_schema::{ColumnDifference, TableColumn, TableColumnType, DEFAULT_SCHEMA},
    PostgresConnection,
};

pub struct UpdateColumnError {
    pub column_name: String,
    pub dif: String,
    pub err: String,
}

pub async fn update_column(
    conn_string: &PostgresConnection,
    table_name: &str,
    differences: &[ColumnDifference],
) -> Result<(), UpdateColumnError> {
    for difference in differences {
        if !difference
            .db
            .sql_type
            .equals_to(&difference.required.sql_type)
        {
            #[cfg(not(feature = "with-logs-and-telemetry"))]
            println!(
                "DB: {}. Updating column {}  type: {:?}->{:?}",
                table_name,
                difference.required.name,
                difference.db.get_default(),
                difference.required.get_default(),
            );

            #[cfg(feature = "with-logs-and-telemetry")]
            {
                let mut ctx = std::collections::HashMap::new();

                ctx.insert("table".to_string(), table_name.to_string());
                ctx.insert(
                    "db_type".to_string(),
                    difference.db.sql_type.to_db_type().to_string(),
                );
                ctx.insert(
                    "required_type".to_string(),
                    difference.required.sql_type.to_db_type().to_string(),
                );

                conn_string.get_logger().write_warning(
                    super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
                    format!("Updating Type of column {}.", difference.db.name.as_str()),
                    Some(ctx),
                );
            }

            try_to_update_column_type(
                conn_string,
                table_name,
                difference.db.name.as_str(),
                &difference.db.sql_type,
                &difference.required.sql_type,
            )
            .await?;

            return Ok(());
        }

        if difference.db.is_nullable != difference.required.is_nullable {
            #[cfg(not(feature = "with-logs-and-telemetry"))]
            println!(
                "DB: {}. Updating column {} nullable: {}->{}",
                table_name,
                difference.required.name,
                difference.db.is_nullable,
                difference.required.is_nullable,
            );

            #[cfg(feature = "with-logs-and-telemetry")]
            {
                let mut ctx = std::collections::HashMap::new();

                ctx.insert("table".to_string(), table_name.to_string());
                ctx.insert(
                    "db_nullable".to_string(),
                    difference.db.is_nullable.to_string(),
                );
                ctx.insert(
                    "required_nullable".to_string(),
                    difference.required.is_nullable.to_string(),
                );

                conn_string.get_logger().write_warning(
                    super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
                    format!(
                        "Updating IsNullable of column {}.",
                        difference.db.name.as_str()
                    ),
                    Some(ctx),
                );
            }

            try_to_update_is_nullable(
                conn_string,
                table_name,
                difference.db.name.as_str(),
                difference.db.is_nullable,
                difference.required.is_nullable,
            )
            .await?;
            return Ok(());
        }

        if !difference.db.is_default_the_same(&difference.required) {
            #[cfg(not(feature = "with-logs-and-telemetry"))]
            println!(
                "DB: {}. Updating column {} default: {:?}->{:?}",
                table_name,
                difference.required.name,
                difference.db.get_default(),
                difference.required.get_default(),
            );

            #[cfg(feature = "with-logs-and-telemetry")]
            {
                let mut ctx = std::collections::HashMap::new();

                ctx.insert("table".to_string(), table_name.to_string());
                ctx.insert(
                    "db_default".to_string(),
                    format!("{:?}", difference.db.get_default()),
                );
                ctx.insert(
                    "required_default".to_string(),
                    format!("{:?}", difference.required.get_default()),
                );

                conn_string.get_logger().write_warning(
                    super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
                    format!(
                        "Updating Default of column {}.",
                        difference.db.name.as_str()
                    ),
                    Some(ctx),
                );
            }

            try_to_update_default(
                conn_string,
                table_name,
                &difference.db,
                &difference.required,
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
    db_nullable: bool,
    required_to_be_nullable: bool,
) -> Result<(), UpdateColumnError> {
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
            return Err(UpdateColumnError {
                column_name: column_name.to_string(),
                dif: format!("Nullable update: '{db_nullable}' -> '{required_to_be_nullable}'"),
                err: format!("Failed to execute {sql}. Reason: {:?}", err),
            });
        }
    }
}

async fn try_to_update_column_type(
    conn_string: &PostgresConnection,
    table_name: &str,
    column_name: &str,
    now_type: &TableColumnType,
    required_type: &TableColumnType,
) -> Result<(), UpdateColumnError> {
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
            return Err(UpdateColumnError {
                column_name: column_name.to_string(),
                dif: format!(
                    "Type update: '{}' -> '{}'",
                    now_type.to_db_type(),
                    required_type.to_db_type()
                ),
                err: format!("Failed to execute {}. Reason: {:?}", sql, err),
            });
        }
    }
}

async fn try_to_update_default(
    conn_string: &PostgresConnection,
    table_name: &str,
    db: &TableColumn,
    required: &TableColumn,
) -> Result<(), UpdateColumnError> {
    let sql = if let Some(now_default) = db.default.as_ref() {
        if let Some(required_default) = required.default.as_ref() {
            if required_default.as_str() == now_default.as_str() {
                println!("BUG: We should not be here: #1");
                return Ok(());
            } else {
                format!(
                    r#"alter table {DEFAULT_SCHEMA}.{table_name}
                    alter column {column_name} set default {now_default}"#,
                    column_name = db.name.as_str(),
                    now_default = now_default.as_str()
                )
            }
        } else {
            format!(
                r#"alter table {DEFAULT_SCHEMA}.{table_name}
                alter column {column_name} drop default"#,
                column_name = db.name.as_str(),
            )
        }
    } else {
        if let Some(req_default) = required.get_default() {
            format!(
                r#"alter table {DEFAULT_SCHEMA}.{table_name}
           alter column {column_name} set default {req_default}"#,
                column_name = required.name.as_str(),
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
            return Err(UpdateColumnError {
                column_name: required.name.to_string(),
                dif: format!(
                    "Default update: '{:?}' -> '{:?}'",
                    db.get_default(),
                    required.get_default()
                ),
                err: format!("Failed to execute {}. Reason: {:?}", sql, err),
            });
        }
    }
}
