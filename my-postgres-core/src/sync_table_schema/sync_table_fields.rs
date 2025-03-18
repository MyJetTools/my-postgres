use std::{collections::HashMap, time::Duration};

use my_logger::LogEventCtx;
#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;
use rust_extensions::StrOrString;

use crate::{
    table_schema::{SchemaDifference, TableColumn, TableColumnType, TableSchema, DEFAULT_SCHEMA},
    ColumnName, MyPostgresError, PostgresConnection, RequestContext,
};

pub async fn sync_table_fields(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: &MyTelemetryContext,
) -> Result<bool, MyPostgresError> {
    let db_fields = get_db_fields(
        conn_string,
        table_schema.table_name,
        sql_timeout,
        #[cfg(feature = "with-logs-and-telemetry")]
        my_telemetry,
    )
    .await?;

    if db_fields.is_none() {
        create_table(
            conn_string,
            table_schema,
            sql_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            my_telemetry,
        )
        .await;
        return Ok(true);
    }

    let db_fields = db_fields.as_ref().unwrap();

    let schema_difference = SchemaDifference::new(table_schema, db_fields);

    if schema_difference.to_update.len() > 0 {
        if let Err(err) = super::update_column(
            conn_string,
            &table_schema.table_name,
            schema_difference.to_update.as_slice(),
            sql_timeout,
            #[cfg(feature = "with-logs-and-telemetry")]
            my_telemetry,
        )
        .await
        {
            my_logger::LOGGER.write_warning(
                super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
                format!("Failed to update column {}", err.column_name),
                LogEventCtx::new()
                    .add("table_name", table_schema.table_name)
                    .add("difference", err.dif)
                    .add("column", err.column_name)
                    .add("err", err.err),
            );

            tokio::time::sleep(Duration::from_secs(10)).await;

            panic!("Closing application...");
        }
    }

    if schema_difference.to_add.len() > 0 {
        for column_name in &schema_difference.to_add {
            add_column_to_table(
                conn_string,
                table_schema,
                column_name,
                sql_timeout,
                #[cfg(feature = "with-logs-and-telemetry")]
                my_telemetry,
            )
            .await?;
        }

        return Ok(true);
    }

    Ok(false)
}

async fn create_table(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: &MyTelemetryContext,
) {
    let create_table_sql = table_schema.generate_create_table_script();

    let mut log_ctx = LogEventCtx::new()
        .add("table_name", table_schema.table_name.to_string())
        .add("sql", create_table_sql.to_string());

    if let Some((primary_key_name, _)) = &table_schema.primary_key {
        log_ctx = log_ctx.add("primaryKeyName".to_string(), primary_key_name.to_string());
    }

    my_logger::LOGGER.write_warning(
        super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
        format!(
            "Table not found. Creating table: {}",
            table_schema.table_name
        ),
        log_ctx,
    );

    let ctx = RequestContext::new(
        sql_timeout,
        "create_table".to_string(),
        crate::is_debug(table_schema.table_name, "create_table"),
        #[cfg(feature = "with-logs-and-telemetry")]
        Some(my_telemetry),
    );

    conn_string
        .execute_sql(&create_table_sql.into(), &ctx)
        .await
        .unwrap();
}

async fn get_db_fields(
    conn_string: &PostgresConnection,
    table_name: &str,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] ctx: &MyTelemetryContext,
) -> Result<Option<HashMap<String, TableColumn>>, MyPostgresError> {
    let sql = format!(
        r#"SELECT column_name, column_default, is_nullable, data_type
    FROM information_schema.columns
   WHERE table_schema = '{DEFAULT_SCHEMA}'
     AND table_name   = '{table_name}'
    ORDER BY ordinal_position"#
    );

    let ctx = RequestContext::new(
        sql_timeout,
        "get_db_fields".to_string(),
        crate::is_debug(table_name, "get_db_fields"),
        #[cfg(feature = "with-logs-and-telemetry")]
        Some(ctx),
    );

    let result = conn_string
        .execute_sql_as_vec(
            &sql.into(),
            |db_row| {
                let name: String = db_row.get("column_name");
                let sql_type = match get_sql_type(db_row) {
                    Ok(result) => result,
                    Err(err) => {
                        panic!("Can not get sql type for column {}. Reason: {}", name, err);
                    }
                };
                TableColumn {
                    name: name.into(),
                    sql_type,
                    is_nullable: get_is_nullable(db_row),
                    default: get_column_default(&db_row),
                }
            },
            &ctx,
        )
        .await?;

    if result.is_empty() {
        return Ok(None);
    }

    let result = result
        .into_iter()
        .map(|itm| (itm.name.to_string(), itm))
        .collect();

    Ok(Some(result))
}

async fn add_column_to_table(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
    column_name: &ColumnName,
    sql_timeout: Duration,
    #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: &MyTelemetryContext,
) -> Result<(), MyPostgresError> {
    let add_column_sql = table_schema.generate_add_column_sql(column_name);

    my_logger::LOGGER.write_warning(
        super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
        format!(
            "Adding column by execution sql: {}",
            add_column_sql.as_str()
        ),
        LogEventCtx::new()
            .add("table_name", table_schema.table_name.to_string())
            .add("column_name", column_name.to_string()),
    );

    let ctx = RequestContext::new(
        sql_timeout,
        "add_column_to_table".to_string(),
        crate::is_debug(table_schema.table_name, "add_column_to_table"),
        #[cfg(feature = "with-logs-and-telemetry")]
        Some(my_telemetry),
    );

    conn_string
        .execute_sql(&add_column_sql.into(), &ctx)
        .await?;

    Ok(())
}

fn get_sql_type(db_row: &tokio_postgres::Row) -> Result<TableColumnType, String> {
    let column_type: String = db_row.get("data_type");
    match TableColumnType::from_db_string(&column_type) {
        Some(result) => Ok(result),
        None => {
            return Err(format!("Unknown column type: {}", column_type));
        }
    }
}

fn get_is_nullable(db_row: &tokio_postgres::Row) -> bool {
    let is_nullable: String = db_row.get("is_nullable");
    is_nullable == "YES"
}

fn get_column_default(db_row: &tokio_postgres::Row) -> Option<StrOrString<'static>> {
    let value: Option<String> = db_row.get("column_default");

    let value = value?;

    Some(transform_value(value.as_str()).to_string().into())
}

fn transform_value(src: &str) -> &str {
    if !src.starts_with("'") {
        return src;
    }

    let src = &src[1..];

    match src.find('\'') {
        Some(end_index) => &src[..end_index],
        None => src,
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test() {
        let src = "'2021-01-01'::date";

        let result = super::transform_value(src);

        assert_eq!("2021-01-01", result);
    }
}
