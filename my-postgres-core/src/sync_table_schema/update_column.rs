use std::time::Duration;

use my_logger::LogEventCtx;
#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;

use crate::{
    table_schema::{ColumnDifference, TableColumn, TableColumnType, DEFAULT_SCHEMA},
    ColumnName, PostgresConnection, RequestContext,
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
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: &MyTelemetryContext,
) -> Result<(), UpdateColumnError> {
    for difference in differences {
        if !difference
            .db
            .sql_type
            .equals_to(&difference.required.sql_type)
        {
            my_logger::LOGGER.write_warning(
                super::TABLE_SCHEMA_SYNCHRONIZATION,
                format!(
                    "Updating Type of column {}.",
                    difference.db.name.name.as_str()
                ),
                LogEventCtx::new()
                    .add("table", table_name.to_string())
                    .add("db_type", difference.db.sql_type.to_db_type().to_string())
                    .add(
                        "required_type",
                        difference.required.sql_type.to_db_type().to_string(),
                    ),
            );

            try_to_update_column_type(
                conn_string,
                table_name,
                &difference.db.name,
                &difference.db.sql_type,
                &difference.required.sql_type,
                sql_timeout,
                #[cfg(feature = "with-logs-and-telemetry")]
                my_telemetry,
            )
            .await?;
        }

        if difference.db.is_nullable != difference.required.is_nullable {
            my_logger::LOGGER.write_warning(
                super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
                format!(
                    "Updating IsNullable of column {}.",
                    difference.db.name.name.as_str()
                ),
                LogEventCtx::new()
                    .add("table", table_name.to_string())
                    .add("db_nullable", difference.db.is_nullable.to_string())
                    .add(
                        "required_nullable",
                        difference.required.is_nullable.to_string(),
                    ),
            );

            try_to_update_is_nullable(
                conn_string,
                table_name,
                &difference.db.name,
                difference.db.is_nullable,
                difference.required.is_nullable,
                sql_timeout,
                #[cfg(feature = "with-logs-and-telemetry")]
                my_telemetry,
            )
            .await?;
        }

        if !difference.db.is_default_the_same(&difference.required) {
            my_logger::LOGGER.write_warning(
                super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
                format!(
                    "Updating Default of column {}.",
                    difference.db.name.name.as_str()
                ),
                LogEventCtx::new()
                    .add("table", table_name.to_string())
                    .add("db_default", format!("{:?}", difference.db.get_default()))
                    .add(
                        "required_default",
                        format!("{:?}", difference.required.get_default()),
                    ),
            );

            try_to_update_default(
                conn_string,
                table_name,
                &difference.db,
                &difference.required,
                sql_timeout,
                #[cfg(feature = "with-logs-and-telemetry")]
                my_telemetry,
            )
            .await?;
        }
    }

    Ok(())
}

async fn try_to_update_is_nullable(
    conn_string: &PostgresConnection,
    table_name: &str,
    column_name: &ColumnName,
    db_nullable: bool,
    required_to_be_nullable: bool,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] ctx: &MyTelemetryContext,
) -> Result<(), UpdateColumnError> {
    let ctx = RequestContext::new(
        sql_timeout,
        format!("ALTER TABLE {} ", table_name),
        #[cfg(feature = "with-logs-and-telemetry")]
        Some(ctx),
    );

    if required_to_be_nullable {
        let sql = format!(
            r#"alter table {DEFAULT_SCHEMA}.{table_name}
        alter column {column_name} drop not null;"#,
            column_name = column_name.to_string()
        );

        conn_string.execute_sql(&sql.into(), &ctx).await.unwrap();

        return Ok(());
    }

    let sql = format!(
        r#"alter table {DEFAULT_SCHEMA}.{table_name}
    alter column {column_name} set not null;"#,
        column_name = column_name.to_string()
    );

    match conn_string.execute_sql(&sql.clone().into(), &ctx).await {
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
    column_name: &ColumnName,
    now_type: &TableColumnType,
    required_type: &TableColumnType,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] ctx: &MyTelemetryContext,
) -> Result<(), UpdateColumnError> {
    let db_type = required_type.to_db_type();

    let sql = format!(
        r#"alter table {DEFAULT_SCHEMA}.{table_name}
    alter column {column_name} type {db_type} using test::{db_type};"#,
        column_name = column_name.to_string()
    );

    let ctx = RequestContext::new(
        sql_timeout,
        format!(
            "ALTER column {} of table {} ",
            column_name.name.as_str(),
            table_name
        ),
        #[cfg(feature = "with-logs-and-telemetry")]
        Some(ctx),
    );

    match conn_string.execute_sql(&sql.clone().into(), &ctx).await {
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
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: &MyTelemetryContext,
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
                    column_name = db.name.to_string(),
                    now_default = now_default.as_str()
                )
            }
        } else {
            format!(
                r#"alter table {DEFAULT_SCHEMA}.{table_name}
                alter column {column_name} drop default"#,
                column_name = db.name.to_string(),
            )
        }
    } else {
        if let Some(req_default) = required.get_default() {
            format!(
                r#"alter table {DEFAULT_SCHEMA}.{table_name}
           alter column {column_name} set default {req_default}"#,
                column_name = required.name.to_string(),
            )
        } else {
            println!("BUG: We should not be here: #2");
            return Ok(());
        }
    };

    let ctx = RequestContext::new(
        sql_timeout,
        format!("Updating default for table {} ", table_name),
        #[cfg(feature = "with-logs-and-telemetry")]
        Some(my_telemetry),
    );

    match conn_string.execute_sql(&sql.clone().into(), &ctx).await {
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
