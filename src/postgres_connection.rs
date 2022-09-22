#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::{MyTelemetryContext, TelemetryEvent};
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
        select: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Option<i64>, MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();
        let result = self.client.query(&select, params).await;

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_ok_telemetry(start, select.to_string(), telemetry_context).await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry(
                        start,
                        select,
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
        select: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();
        let result = self.client.query(&select, params).await;

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_ok_telemetry(start, select, telemetry_context).await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry(
                        start,
                        select,
                        format!("{:?}", err),
                        telemetry_context,
                        &self.logger,
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
        select: String,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();

        let result = self.client.query(&select, params).await;

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(_) => {
                    write_ok_telemetry(start, select, telemetry_context).await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry(
                        start,
                        select.to_string(),
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
                    write_fail_telemetry(
                        start,
                        sql,
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

    pub async fn insert_db_entity_if_not_exists<TEntity: InsertEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
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
                    write_fail_telemetry(
                        start,
                        sql,
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
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();

        let mut sql_builder = crate::code_gens::insert::BulkInsertBuilder::new();

        for entity in entities {
            sql_builder.start_new_value_line();
            entity.populate(&mut sql_builder);
        }

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
                    write_fail_telemetry(
                        start,
                        sql,
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

    pub async fn bulk_insert_db_entities_if_not_exists<TEntity: InsertEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();

        let mut sql_builder = crate::code_gens::insert::BulkInsertBuilder::new();

        for entity in entities {
            sql_builder.start_new_value_line();
            entity.populate(&mut sql_builder);
        }

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
                    write_fail_telemetry(
                        start,
                        sql,
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

    pub async fn update_db_entity<TEntity: UpdateEntity>(
        &self,
        entity: TEntity,
        table_name: &str,
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
                    write_fail_telemetry(
                        start,
                        sql,
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
                    write_ok_telemetry(
                        start,
                        format!("BulkInsertOrUpdate INTO {}", table_name),
                        telemetry_context,
                    )
                    .await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry(
                        start,
                        format!("BulkInsertOrUpdate INTO {}", table_name),
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
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        #[cfg(feature = "with-logs-and-telemetry")]
        let start = DateTimeAsMicroseconds::now();
        let mut sql_builder = crate::code_gens::insert_or_update::InsertOrUpdateBuilder::new();
        entity.populate(&mut sql_builder);

        let sql = sql_builder.build(table_name, pk_name);
        let result = self
            .client
            .execute(&sql, sql_builder.get_values_data())
            .await;

        #[cfg(feature = "with-logs-and-telemetry")]
        if let Some(telemetry_context) = &telemetry_context {
            match &result {
                Ok(result) => {
                    write_ok_telemetry(
                        start,
                        format!("InsertOrUpdate INTO {}. Result:{}", table_name, result),
                        telemetry_context,
                    )
                    .await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry(
                        start,
                        format!("InsertOrUpdate INTO {}", table_name),
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

    pub async fn bulk_delete<TEntity: DeleteEntity>(
        &self,
        entities: &[TEntity],
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
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
                        format!("BulkDelete {}. Result: {}", table_name, result),
                        telemetry_context,
                    )
                    .await;
                }
                Err(err) => {
                    self.handle_error(err);
                    write_fail_telemetry(
                        start,
                        format!("BulkDelete {}", table_name),
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

    #[cfg(feature = "with-logs-and-telemetry")]
    fn handle_error(&self, _err: &tokio_postgres::Error) {
        self.disconnect();
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
async fn write_fail_telemetry(
    start: DateTimeAsMicroseconds,
    data: String,
    fail: String,
    telemetry_context: &MyTelemetryContext,
    logger: &Arc<dyn Logger + Send + Sync + 'static>,
) {
    let mut ctx = HashMap::new();
    ctx.insert("SQL".to_string(), data.to_string());

    logger.write_error("SQL Request".to_string(), fail.to_string(), Some(ctx));

    #[cfg(feature = "with-logs-and-telemetry")]
    if !my_telemetry::TELEMETRY_INTERFACE.is_telemetry_set_up() {
        return;
    }
    #[cfg(feature = "with-logs-and-telemetry")]
    my_telemetry::TELEMETRY_INTERFACE
        .write_telemetry_event(TelemetryEvent {
            process_id: telemetry_context.process_id,
            started: start.unix_microseconds,
            finished: DateTimeAsMicroseconds::now().unix_microseconds,
            data,
            success: None,
            fail: Some(fail),
            ip: None,
        })
        .await;
}
