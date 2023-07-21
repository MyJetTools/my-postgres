use crate::{
    table_schema::{PrimaryKeySchema, TableSchema},
    MyPostgresError, PostgresConnection,
};

pub async fn sync_primary_key(
    conn_string: &PostgresConnection,
    table_schema: &TableSchema,
) -> Result<bool, MyPostgresError> {
    if table_schema.primary_key.is_none() {
        #[cfg(not(feature = "with-logs-and-telemetry"))]
        println!(
            "Table {} has no primary key. Skipping synchronization",
            table_schema.table_name
        );

        #[cfg(feature = "with-logs-and-telemetry")]
        conn_string.get_logger().write_info(
            "Table Schema verification".into(),
            format!(
                "No Primary key schema is found for the table {}. Primary key synchronization is skipping",
                table_schema.table_name
            ),
            None,
        );

        return Ok(false);
    }

    let (primary_key_name, primary_key_schema) = table_schema.primary_key.as_ref().unwrap();

    let primary_key_from_db =
        get_primary_key_fields_from_db(conn_string, table_schema.table_name).await?;

    if primary_key_schema.is_same_with(&primary_key_from_db) {
        return Ok(false);
    }

    update_primary_key(
        conn_string,
        &table_schema.table_name,
        primary_key_name,
        primary_key_schema,
        &primary_key_from_db,
    )
    .await;

    Ok(true)
}

async fn update_primary_key(
    conn_string: &PostgresConnection,
    table_name: &str,
    primary_key_name: &str,
    primary_key_schema: &PrimaryKeySchema,
    primary_key_from_db: &PrimaryKeySchema,
) {
    let update_primary_key_sql = primary_key_schema.generate_update_primary_key_sql(
        table_name,
        primary_key_name,
        primary_key_from_db,
    );

    if update_primary_key_sql.is_none() {
        return;
    }

    let update_primary_key_sql = update_primary_key_sql.unwrap();

    #[cfg(not(feature = "with-logs-and-telemetry"))]
    println!(
        "Executing update primary key sql: {:?}",
        update_primary_key_sql
    );

    #[cfg(feature = "with-logs-and-telemetry")]
    conn_string.get_logger().write_warning(
        super::TABLE_SCHEMA_SYNCHRONIZATION.to_string(),
        format!(
            "Executing update primary key sql: {:?}",
            update_primary_key_sql
        ),
        None,
    );

    for sql in update_primary_key_sql {
        conn_string
            .execute_sql(
                &sql.into(),
                "update_primary_key".into(),
                #[cfg(feature = "with-logs-and-telemetry")]
                None,
            )
            .await
            .unwrap();
    }
}

async fn get_primary_key_fields_from_db(
    conn_string: &PostgresConnection,
    table_name: &str,
) -> Result<PrimaryKeySchema, MyPostgresError> {
    // cSpell: disable
    let sql = format!(
        r#"SELECT column_name
        FROM information_schema.key_column_usage
        WHERE constraint_name = (
          SELECT constraint_name
          FROM information_schema.table_constraints
          WHERE table_name = '{table_name}'
          AND constraint_type = 'PRIMARY KEY'
        )
        AND table_name = '{table_name}'
        ORDER BY ordinal_position;"#
    );

    // cSpell: enable

    let result = conn_string
        .execute_sql_as_vec(
            &sql.into(),
            "get_db_fields".into(),
            |db_row| {
                let result: String = db_row.get(0);
                result
            },
            #[cfg(feature = "with-logs-and-telemetry")]
            None,
        )
        .await?;

    Ok(PrimaryKeySchema::from_vec_of_string(result))
}
