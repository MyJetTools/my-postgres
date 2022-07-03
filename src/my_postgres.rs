use my_telemetry::{MyTelemetryContext, TelemetryEvent};
use rust_extensions::date_time::DateTimeAsMicroseconds;
use tokio_postgres::NoTls;

use crate::{DeleteEntity, InsertOrUpdateEntity, SelectEntity};

pub struct MyPostgres {
    client: tokio_postgres::Client,
}

impl MyPostgres {
    pub async fn crate_no_tls(conn_string: &str) -> Self {
        let (client, connection) = tokio_postgres::connect(conn_string, NoTls).await.unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Self { client }
    }

    pub async fn query_single_row<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        select: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Option<TEntity>, tokio_postgres::Error> {
        let start = DateTimeAsMicroseconds::now();
        let result = self.client.query(select, params).await;

        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_telemetry(
                        start,
                        select.to_string(),
                        format!("OK").into(),
                        None,
                        telemetry_context,
                    )
                    .await;
                }
                Err(err) => {
                    write_telemetry(
                        start,
                        select.to_string(),
                        None,
                        format!("{:?}", err).into(),
                        telemetry_context,
                    )
                    .await;
                }
            }
        }

        let rows = result?;

        if let Some(row) = rows.get(0) {
            Ok(Some(TEntity::from_db_row(row)))
        } else {
            Ok(None)
        }
    }

    pub async fn query_rows<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        select: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, tokio_postgres::Error> {
        let start = DateTimeAsMicroseconds::now();

        let result = self.client.query(select, params).await;

        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_telemetry(
                        start,
                        select.to_string(),
                        format!("OK").into(),
                        None,
                        telemetry_context,
                    )
                    .await;
                }
                Err(err) => {
                    write_telemetry(
                        start,
                        select.to_string(),
                        None,
                        format!("{:?}", err).into(),
                        telemetry_context,
                    )
                    .await;
                }
            }
        }
        let result = result?;

        let result = result.iter().map(|itm| TEntity::from_db_row(itm)).collect();
        Ok(result)
    }

    pub async fn bulk_insert_or_update_db_entity<TEntity: InsertOrUpdateEntity>(
        &mut self,
        entities: Vec<TEntity>,
        table_name: &str,
        pk_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), tokio_postgres::Error> {
        let start = DateTimeAsMicroseconds::now();
        let builder = self.client.build_transaction();
        let transaction = builder.start().await?;
        for entity in entities {
            let mut sql_builder = crate::code_gens::insert_or_update::InsertOrUpdateBuilder::new();
            entity.populate(&mut sql_builder);
            let sql = sql_builder.build(table_name, pk_name);
            transaction
                .execute(sql.as_str(), sql_builder.get_values_data())
                .await?;
        }
        let result = transaction.commit().await;

        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_telemetry(
                        start,
                        format!("BulkInsertOrUpdate INTO {}", table_name),
                        format!("OK").into(),
                        None,
                        telemetry_context,
                    )
                    .await;
                }
                Err(err) => {
                    write_telemetry(
                        start,
                        format!("BulkInsertOrUpdate INTO {}", table_name),
                        None,
                        format!("{:?}", err).into(),
                        telemetry_context,
                    )
                    .await;
                }
            }
        }

        result?;

        Ok(())
    }

    pub async fn insert_or_update_db_entity<TEntity: InsertOrUpdateEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        pk_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), tokio_postgres::Error> {
        let start = DateTimeAsMicroseconds::now();
        let mut sql_builder = crate::code_gens::insert_or_update::InsertOrUpdateBuilder::new();
        entity.populate(&mut sql_builder);

        let sql = sql_builder.build(table_name, pk_name);
        let result = self
            .client
            .execute(&sql, sql_builder.get_values_data())
            .await;

        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(result) => {
                    write_telemetry(
                        start,
                        format!("InsertOrUpdate INTO {}", table_name),
                        format!("Result: {}", result).into(),
                        None,
                        telemetry_context,
                    )
                    .await;
                }
                Err(err) => {
                    write_telemetry(
                        start,
                        format!("InsertOrUpdate INTO {}", table_name),
                        None,
                        format!("{:?}", err).into(),
                        telemetry_context,
                    )
                    .await;
                }
            }
        }

        result?;

        Ok(())
    }

    pub async fn bulk_delete<TEntity: DeleteEntity>(
        &self,
        entities: Vec<TEntity>,
        table_name: &str,
        telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), tokio_postgres::Error> {
        let start = DateTimeAsMicroseconds::now();

        let mut sql_builder = crate::code_gens::delete::BulkDeleteBuilder::new();
        for entity in entities {
            sql_builder.add_new_line();
            entity.populate(&mut sql_builder);
        }
        let sql = sql_builder.build(table_name);
        let result = self
            .client
            .execute(sql.as_str(), sql_builder.get_values_data())
            .await;

        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(result) => {
                    write_telemetry(
                        start,
                        format!("BulkDelete {}", table_name),
                        format!("Result: {}", result).into(),
                        None,
                        telemetry_context,
                    )
                    .await;
                }
                Err(err) => {
                    write_telemetry(
                        start,
                        format!("BulkDelete {}", table_name),
                        None,
                        format!("{:?}", err).into(),
                        telemetry_context,
                    )
                    .await;
                }
            }
        }

        result?;

        Ok(())
    }
}

async fn write_telemetry(
    start: DateTimeAsMicroseconds,
    data: String,
    success: Option<String>,
    fail: Option<String>,
    telemetry_context: &MyTelemetryContext,
) {
    if !my_telemetry::TELEMETRY_INTERFACE.is_telemetry_set_up() {
        println!("Telemetry is not set up");
        return;
    }

    println!("Writing telemetry event");

    my_telemetry::TELEMETRY_INTERFACE
        .write_telemetry_event(TelemetryEvent {
            process_id: telemetry_context.process_id,
            started: start.unix_microseconds,
            finished: DateTimeAsMicroseconds::now().unix_microseconds,
            data: data.to_string(),
            success,
            fail,
            ip: None,
        })
        .await;
}
