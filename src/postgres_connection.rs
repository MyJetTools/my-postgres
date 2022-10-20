#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::{MyTelemetryContext, TelemetryEvent};
use rust_extensions::date_time::DateTimeAsMicroseconds;
#[cfg(feature = "with-logs-and-telemetry")]
use std::collections::HashMap;

#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::date_time::DateTimeAsMicroseconds;
#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use std::sync::{atomic::AtomicBool, Arc};

use crate::{
    DeleteEntity, InsertEntity, InsertOrUpdateEntity, MyPostgressError, SelectEntity, UpdateEntity,
};

pub struct PostgresConnection {
    client: tokio_postgres::Client,
    #[cfg(feature = "with-logs-and-telemetry")]
    logger: Arc<dyn Logger + Send + Sync + 'static>,
    pub connected: Arc<AtomicBool>,
}

impl PostgresConnection {
    pub fn new(
        client: tokio_postgres::Client,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Send + Sync + 'static>,
    ) -> Self {
        Self {
            client: client,
            connected: Arc::new(AtomicBool::new(true)),
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        }
    }

    pub fn disconnect(&self) {
        self.connected
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn get_count(
        &self,
        sql: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Option<i64>, MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();
        let result = self.client.query(&sql, params).await;

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_ok_telemetry(start, sql, telemetry_context).await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry_and_log(
                        start,
                        "get_count".to_string(),
                        Some(sql.as_str()),
                        format!("{:?}", err).into(),
                        telemetry_context,
                        &self.logger,
                    )
                    .await;
                }
            }
        }

        let rows = result?;

        match rows.get(0) {
            Some(row) => {
                let result: i64 = row.get(0);
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    pub async fn query_single_row<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        sql: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();

        let result = self.do_sql_with_single_row_result(&sql, params).await;

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_ok_telemetry(start, sql, telemetry_context).await;
                }
                Err(err) => {
                    write_fail_telemetry_and_log(
                        start,
                        "query_single_row".to_string(),
                        Some(sql.as_str()),
                        format!("{:?}", err),
                        telemetry_context,
                        &self.logger,
                    )
                    .await;
                }
            }
        }

        result
    }

    pub async fn query_rows<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();

        let result = self.client.query(sql, params).await;

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_ok_telemetry(start, sql.to_string(), telemetry_context).await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry_and_log(
                        start,
                        "query_rows".to_string(),
                        Some(sql),
                        format!("{:?}", err),
                        telemetry_context,
                        &self.logger,
                    )
                    .await;
                }
            }
        }
        let result = result?;

        let result = result.iter().map(|itm| TEntity::from_db_row(itm)).collect();
        Ok(result)
    }

    pub async fn insert_db_entity<TEntity: InsertEntity>(
        &self,
        entity: &TEntity,
        table_name: &str,
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();

        let mut sql_builder = crate::code_gens::insert::InsertBuilder::new();
        entity.populate(&mut sql_builder);
        let sql = sql_builder.build(table_name);

        let result = self
            .client
            .execute(&sql, sql_builder.get_values_data())
            .await;

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_ok_telemetry(start, sql, telemetry_context).await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry_and_log(
                        start,
                        process_name.to_string(),
                        Some(sql.as_str()),
                        format!("{:?}", err),
                        telemetry_context,
                        &self.logger,
                    )
                    .await;
                }
            }
        }

        result?;

        Ok(())
    }

    pub async fn execute_sql(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<u64, MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();

        let result = self.client.execute(sql, params).await;

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_ok_telemetry(start, sql.to_string(), telemetry_context).await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry_and_log(
                        start,
                        "execute_sql".to_string(),
                        Some(sql),
                        format!("{:?}", err),
                        telemetry_context,
                        &self.logger,
                    )
                    .await;
                }
            }
        }

        Ok(result?)
    }

    pub async fn insert_db_entity_if_not_exists<TEntity: InsertEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();

        let mut sql_builder = crate::code_gens::insert::InsertBuilder::new();
        entity.populate(&mut sql_builder);
        let sql = format!("{} ON CONFLICT DO NOTHING", sql_builder.build(table_name));

        let result = self
            .client
            .execute(&sql, sql_builder.get_values_data())
            .await;

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_ok_telemetry(start, sql, telemetry_context).await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry_and_log(
                        start,
                        process_name.to_string(),
                        Some(sql.as_str()),
                        format!("{:?}", err),
                        telemetry_context,
                        &self.logger,
                    )
                    .await;
                }
            }
        }

        result?;

        Ok(())
    }

    pub async fn bulk_insert_db_entities<TEntity: InsertEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        if entities.is_empty() {
            return Err(MyPostgressError::Other(
                "bulk_insert_db_entities: Not entities to execute".to_string(),
            ));
        }

        let mut sql_builder = crate::code_gens::insert::BulkInsertBuilder::new();

        for entity in entities {
            sql_builder.start_new_value_line();
            entity.populate(&mut sql_builder);
        }

        let sql = sql_builder.build(table_name);

        self.do_sql(
            &sql,
            sql_builder.get_values_data(),
            #[cfg(feature = "with-logs-and-telemetry")]
            process_name,
            #[cfg(feature = "with-logs-and-telemetry")]
            false,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await?;

        Ok(())
    }

    pub async fn bulk_insert_db_entities_if_not_exists<TEntity: InsertEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        if entities.is_empty() {
            return Err(MyPostgressError::Other(
                "bulk_insert_db_entities_if_not_exists: Not entities to execute".to_string(),
            ));
        }

        let mut sql_builder = crate::code_gens::insert::BulkInsertBuilder::new();

        for entity in entities {
            sql_builder.start_new_value_line();
            entity.populate(&mut sql_builder);
        }

        let sql = format!("{} ON CONFLICT DO NOTHING", sql_builder.build(table_name));

        self.do_sql(
            &sql,
            sql_builder.get_values_data(),
            #[cfg(feature = "with-logs-and-telemetry")]
            process_name,
            #[cfg(feature = "with-logs-and-telemetry")]
            true,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await?;

        Ok(())
    }

    pub async fn update_db_entity<TEntity: UpdateEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();

        let mut sql_builder = crate::code_gens::update::UpdateBuilder::new();
        entity.populate(&mut sql_builder);
        let sql = sql_builder.build(table_name);

        let result = self
            .client
            .execute(&sql, sql_builder.get_values_data())
            .await;

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_ok_telemetry(start, sql, telemetry_context).await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry_and_log(
                        start,
                        process_name.to_string(),
                        Some(sql.as_str()),
                        format!("{:?}", err),
                        telemetry_context,
                        &self.logger,
                    )
                    .await;
                }
            }
        }

        result?;

        Ok(())
    }

    pub async fn bulk_insert_or_update_db_entity<TEntity: InsertOrUpdateEntity>(
        &mut self,
        entities: Vec<TEntity>,
        table_name: &str,
        pk_name: &str,
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
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

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_ok_telemetry(start, process_name.to_string(), telemetry_context).await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry_and_log(
                        start,
                        process_name.to_string(),
                        None,
                        format!("{:?}", err),
                        telemetry_context,
                        &self.logger,
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
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let mut sql_builder = crate::code_gens::insert_or_update::InsertOrUpdateBuilder::new();
        entity.populate(&mut sql_builder);

        let sql = sql_builder.build(table_name, pk_name);

        self.do_sql(
            &sql,
            sql_builder.get_values_data(),
            #[cfg(feature = "with-logs-and-telemetry")]
            process_name,
            #[cfg(feature = "with-logs-and-telemetry")]
            true,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context,
        )
        .await?;

        Ok(())
    }

    pub async fn bulk_delete<TEntity: DeleteEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        if entities.is_empty() {
            return Err(MyPostgressError::Other(
                "bulk_delete: Not entities to execute".to_string(),
            ));
        }

        #[cfg(feature = "with-logs-and-telemetry")]
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
        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(result) => {
                    write_ok_telemetry(
                        start,
                        format!("{}. Result: {}", process_name, result),
                        telemetry_context,
                    )
                    .await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry_and_log(
                        start,
                        process_name.to_string(),
                        Some(sql.as_str()),
                        format!("{:?}", err),
                        telemetry_context,
                        &self.logger,
                    )
                    .await;
                }
            }
        }

        result?;

        Ok(())
    }

    fn handle_error(&self, _err: &tokio_postgres::Error) {
        self.disconnect();
    }

    async fn do_sql(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] process_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] append_sql_to_fail_result: bool,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<u64, MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();

        let result = self.client.execute(sql, params).await;

        if let Err(err) = &result {
            self.handle_error(err);
            #[cfg(feature = "failed-sql-to-console")]
            println!(
                "{}: Failed sql: {}",
                DateTimeAsMicroseconds::now().to_rfc3339(),
                sql
            );
        }

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_ok_telemetry(start, process_name.to_string(), telemetry_context).await;
                }
                Err(err) => {
                    write_fail_telemetry_and_log(
                        start,
                        process_name.to_string(),
                        if append_sql_to_fail_result {
                            Some(sql)
                        } else {
                            None
                        },
                        format!("Err: {:?}", err),
                        telemetry_context,
                        &self.logger,
                    )
                    .await;
                }
            }
        }

        Ok(result?)
    }

    async fn do_sql_with_single_row_result<TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Option<TEntity>, MyPostgressError> {
        let result = self.client.query(sql, params).await;
        match result {
            Ok(result) => {
                if result.len() > 1 {
                    Err(MyPostgressError::SingleRowRequestReturnedMultipleRows(
                        result.len(),
                    ))
                } else if result.len() == 0 {
                    Ok(None)
                } else {
                    let row = result.get(0).unwrap();
                    Ok(Some(TEntity::from_db_row(&row)))
                }
            }
            Err(err) => {
                self.handle_error(&err);
                #[cfg(feature = "failed-sql-to-console")]
                println!(
                    "{}: Failed sql: {}",
                    DateTimeAsMicroseconds::now().to_rfc3339(),
                    sql
                );
                Err(MyPostgressError::PostgresError(err))
            }
        }
    }
}

#[cfg(feature = "with-logs-and-telemetry")]
async fn write_ok_telemetry(
    start: DateTimeAsMicroseconds,
    data: String,
    telemetry_context: &MyTelemetryContext,
) {
    if !my_telemetry::TELEMETRY_INTERFACE.is_telemetry_set_up() {
        return;
    }

    my_telemetry::TELEMETRY_INTERFACE
        .write_telemetry_event(TelemetryEvent {
            process_id: telemetry_context.process_id,
            started: start.unix_microseconds,
            finished: DateTimeAsMicroseconds::now().unix_microseconds,
            data,
            success: Some("Ok".to_string()),
            fail: None,
            ip: None,
        })
        .await;
}

#[cfg(feature = "with-logs-and-telemetry")]
async fn write_fail_telemetry_and_log(
    start: DateTimeAsMicroseconds,
    process: String,
    sql: Option<&str>,
    fail: String,
    telemetry_context: &MyTelemetryContext,
    logger: &Arc<dyn Logger + Send + Sync + 'static>,
) {
    let ctx = if let Some(sql) = sql {
        let mut ctx = HashMap::new();
        ctx.insert("sql".to_string(), sql.to_string());
        Some(ctx)
    } else {
        None
    };

    logger.write_error(process.to_string(), fail.to_string(), ctx);

    if !my_telemetry::TELEMETRY_INTERFACE.is_telemetry_set_up() {
        return;
    }
    my_telemetry::TELEMETRY_INTERFACE
        .write_telemetry_event(TelemetryEvent {
            process_id: telemetry_context.process_id,
            started: start.unix_microseconds,
            finished: DateTimeAsMicroseconds::now().unix_microseconds,
            data: process,
            success: None,
            fail: Some(fail),
            ip: None,
        })
        .await;
}
