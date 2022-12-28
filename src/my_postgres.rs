#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;
#[cfg(feature = "with-tls")]
use openssl::ssl::{SslConnector, SslMethod};
#[cfg(feature = "with-tls")]
use postgres_openssl::MakeTlsConnector;
use rust_extensions::date_time::DateTimeAsMicroseconds;
#[cfg(feature = "with-logs-and-telemetry")]
use rust_extensions::Logger;
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::sync::RwLock;
use tokio_postgres::NoTls;

use crate::{
    count_result::CountResult,
    rented_connection::{RentedConnection, RentedConnectionMut},
    sql_insert::SqlInsertModel,
    sql_select::{BulkSelectBuilder, BulkSelectEntity, SelectEntity, ToSqlString},
    sql_update::SqlUpdateModel,
    sql_where::SqlWhereModel,
    MyPostgressError, PostgresConnection, PostgressSettings,
};

pub struct MyPostgres {
    client: Arc<RwLock<Option<PostgresConnection>>>,
}

impl MyPostgres {
    pub async fn new(
        app_name: String,
        postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
        #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
    ) -> Self {
        let shared_connection = Arc::new(RwLock::new(None));
        tokio::spawn(do_connection(
            app_name,
            postgres_settings,
            shared_connection.clone(),
            #[cfg(feature = "with-logs-and-telemetry")]
            logger,
        ));

        Self {
            client: shared_connection,
        }
    }

    async fn get_connection<'s>(&'s self) -> Result<RentedConnection<'s>, MyPostgressError> {
        RentedConnection::new(self.client.read().await)
    }

    async fn get_connection_mut<'s>(&'s self) -> Result<RentedConnectionMut<'s>, MyPostgressError> {
        RentedConnectionMut::new(self.client.write().await)
    }

    pub async fn get_count<'s, TWhereModel: SqlWhereModel<'s>, TResult: CountResult>(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TResult>, MyPostgressError> {
        let mut sql = String::new();

        let mut params = Vec::new();
        sql.push_str("SELECT COUNT(*)::");
        sql.push_str(TResult::get_postgres_type());

        sql.push_str(" FROM ");
        sql.push_str(table_name);
        sql.push_str(" WHERE ");

        where_model.fill_where(&mut sql, &mut params);

        let connection = self.get_connection().await?;

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in params {
            params_to_invoke.push(param.value);
        }

        let mut result = connection
            .execute_sql_as_vec(
                sql.as_str(),
                &params_to_invoke,
                sql.as_str(),
                |row| TResult::from_db_row(row),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;
        if result.len() > 0 {
            Ok(Some(result.remove(0)))
        } else {
            Ok(None)
        }
    }

    pub async fn query_single_row<
        's,
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel<'s>,
    >(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgressError> {
        let (sql, params) = crate::sql_select::build(
            table_name,
            |sql| TEntity::fill_select_fields(sql),
            where_model,
            TEntity::get_order_by_fields(),
            TEntity::get_group_by_fields(),
        );

        let connection = self.get_connection().await?;

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in params {
            params_to_invoke.push(param.value);
        }

        let mut result = connection
            .execute_sql_as_vec(
                sql.as_str(),
                &params_to_invoke,
                sql.as_str(),
                |row| TEntity::from(row),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        if result.len() > 0 {
            Ok(Some(result.remove(0)))
        } else {
            Ok(None)
        }
    }

    pub async fn query_single_row_with_processing<
        's,
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel<'s>,
        TPostProcessing: Fn(&mut String),
    >(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        post_processing: TPostProcessing,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Option<TEntity>, MyPostgressError> {
        let (mut sql, params) = crate::sql_select::build(
            table_name,
            |sql| TEntity::fill_select_fields(sql),
            where_model,
            TEntity::get_order_by_fields(),
            TEntity::get_group_by_fields(),
        );

        post_processing(&mut sql);

        let connection = self.get_connection().await?;

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in params {
            params_to_invoke.push(param.value);
        }

        let mut result = connection
            .execute_sql_as_vec(
                sql.as_str(),
                &params_to_invoke,
                sql.as_str(),
                |row| TEntity::from(row),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        if result.len() > 0 {
            Ok(Some(result.remove(0)))
        } else {
            Ok(None)
        }
    }

    pub async fn execute_sql<ToSql: ToSqlString>(
        &self,
        sql: &ToSql,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<u64, MyPostgressError> {
        let (sql, params) = sql.as_sql();

        let params = if let Some(params) = params {
            params
        } else {
            &[]
        };

        let connection = self.get_connection().await?;

        connection
            .execute_sql(
                sql.as_str(),
                params,
                sql.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn execute_sql_as_vec<
        ToSql: ToSqlString,
        TEntity: SelectEntity + Send + Sync + 'static,
    >(
        &self,
        sql: &ToSql,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        let (sql, params) = sql.as_sql();

        let params = if let Some(params) = params {
            params
        } else {
            &[]
        };

        let connection = self.get_connection().await?;
        connection
            .execute_sql_as_vec(
                sql.as_str(),
                params,
                sql.as_str(),
                |row| TEntity::from(row),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_rows<
        's,
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel<'s>,
    >(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        let (sql, params) = crate::sql_select::build(
            table_name,
            |sql| TEntity::fill_select_fields(sql),
            where_model,
            TEntity::get_order_by_fields(),
            TEntity::get_group_by_fields(),
        );

        let connection = self.get_connection().await?;

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in params {
            params_to_invoke.push(param.value);
        }

        connection
            .execute_sql_as_vec(
                sql.as_str(),
                params_to_invoke.as_slice(),
                sql.as_str(),
                |row| TEntity::from(row),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn query_rows_with_processing<
        's,
        TEntity: SelectEntity + Send + Sync + 'static,
        TWhereModel: SqlWhereModel<'s>,
        TPostProcessing: Fn(&mut String),
    >(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        post_processing: TPostProcessing,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TEntity>, MyPostgressError> {
        let (mut sql, params) = crate::sql_select::build(
            table_name,
            |sql| TEntity::fill_select_fields(sql),
            where_model,
            TEntity::get_order_by_fields(),
            TEntity::get_group_by_fields(),
        );

        post_processing(&mut sql);

        let connection = self.get_connection().await?;

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in params {
            params_to_invoke.push(param.value);
        }

        connection
            .execute_sql_as_vec(
                sql.as_str(),
                params_to_invoke.as_slice(),
                sql.as_str(),
                |row| TEntity::from(row),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn bulk_query_rows_with_transformation<
        's,
        TIn: SqlWhereModel<'s> + Send + Sync + 'static,
        TOut,
        TEntity: SelectEntity + BulkSelectEntity + Send + Sync + 'static,
        TTransform: Fn(&TIn, Option<TEntity>) -> TOut,
    >(
        &self,
        sql_builder: &'s BulkSelectBuilder<'s, TIn>,
        transform: TTransform,
        #[cfg(feature = "with-logs-and-telemetry")] ctx: Option<&MyTelemetryContext>,
    ) -> Result<Vec<TOut>, MyPostgressError> {
        let process_name = format!("BulkQueryRows: {}", sql_builder.table_name);
        let (sql, params) = sql_builder.build_sql(|sql| TEntity::fill_select_fields(sql));

        let response = {
            let connection = self.get_connection().await?;

            let mut params_to_invoke = Vec::with_capacity(params.len());

            for param in params {
                params_to_invoke.push(param.value);
            }

            connection
                .execute_sql_as_vec(
                    sql.as_str(),
                    &params_to_invoke,
                    process_name.as_str(),
                    |row| TEntity::from(row),
                    #[cfg(feature = "with-logs-and-telemetry")]
                    ctx,
                )
                .await?
        };

        let mut result = Vec::with_capacity(response.len());

        let mut response_as_hashmap = BTreeMap::new();

        for entity in response {
            response_as_hashmap.insert(entity.get_line_no(), entity);
        }

        let mut line_no = 0;
        for where_model in &sql_builder.where_models {
            let out = response_as_hashmap.remove(&line_no);
            let item = transform(where_model, out);
            result.push(item);
            line_no += 1;
        }

        Ok(result)
    }

    pub async fn insert_db_entity<'s, TEntity: SqlInsertModel<'s>>(
        &self,
        entity: &'s TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let mut params = Vec::new();
        let (sql, _) = crate::sql_insert::build_insert(table_name, entity, &mut params, None);

        let process_name: String = format!("insert_db_entity into table {}", table_name);
        let connection = self.get_connection().await?;

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in params {
            params_to_invoke.push(param.value);
        }

        connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn insert_db_entity_if_not_exists<'s, TEntity: SqlInsertModel<'s>>(
        &self,
        entity: &'s TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let mut params = Vec::new();
        let sql = crate::sql_insert::build_insert_if_not_exists(table_name, entity, &mut params);

        let process_name = format!("insert_db_entity_if_not_exists into table {}", table_name);
        let connection = self.get_connection().await?;
        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in params {
            params_to_invoke.push(param.value);
        }

        connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn bulk_insert_db_entities<'s, TEntity: SqlInsertModel<'s>>(
        &self,
        entities: &'s [TEntity],
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let (sql, params) = crate::sql_insert::build_bulk_insert(table_name, entities);

        let process_name = format!("bulk_insert_db_entities into table {}", table_name);

        let connection = self.get_connection().await?;

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in params {
            params_to_invoke.push(param.value);
        }

        connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn bulk_insert_db_entities_if_not_exists<'s, TEntity: SqlInsertModel<'s>>(
        &self,
        table_name: &str,
        entities: &'s [TEntity],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let (mut sql, params) = crate::sql_insert::build_bulk_insert(table_name, entities);

        sql.push_str(" ON CONFLICT DO NOTHING");

        let process_name = format!(
            "bulk_insert_db_entities_if_not_exists into table {}",
            table_name
        );
        let connection = self.get_connection().await?;

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in params {
            params_to_invoke.push(param.value);
        }
        connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn update_db_entity<'s, TEntity: SqlUpdateModel<'s> + SqlWhereModel<'s>>(
        &self,
        entity: &'s TEntity,
        table_name: &str,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let (sql, params) = crate::sql_update::build(table_name, entity, entity);
        let process_name = format!("update_db_entity into table {}", table_name);
        let connection = self.get_connection().await?;

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in params {
            params_to_invoke.push(param.value);
        }

        connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn bulk_insert_or_update_db_entity<
        's,
        TEntity: SqlInsertModel<'s> + SqlUpdateModel<'s>,
    >(
        &self,
        table_name: &str,
        pk_name: &str,
        entities: &'s [TEntity],
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let process_name = format!(
            "bulk_insert_or_update_db_entity into table {} {} entities",
            table_name,
            entities.len()
        );

        let sqls = crate::sql_insert::build_bulk_insert_if_update(table_name, pk_name, entities);

        let mut connection = self.get_connection_mut().await?;

        connection
            .execute_bulk_sql(
                sqls,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await
    }

    pub async fn insert_or_update_db_entity<
        's,
        TEntity: SqlInsertModel<'s> + SqlUpdateModel<'s>,
    >(
        &self,
        table_name: &str,
        pk_name: &str,
        entity: &'s TEntity,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let process_name = format!("insert_or_update_db_entity into table {}", table_name);

        let (sql, params) = crate::sql_insert::build_insert_or_update(table_name, pk_name, entity);

        let connection = self.get_connection().await?;

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in params {
            params_to_invoke.push(param.value);
        }

        connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
                process_name.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn delete_db_entity<'s, TWhereModel: SqlWhereModel<'s>>(
        &self,
        table_name: &str,
        where_model: &'s TWhereModel,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let (sql, params) = crate::sql_delete::build_delete(table_name, where_model);

        let connection = self.get_connection().await?;
        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in params {
            params_to_invoke.push(param.value);
        }

        connection
            .execute_sql(
                sql.as_str(),
                params_to_invoke.as_slice(),
                sql.as_str(),
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }

    pub async fn bulk_delete<'s, TEntity: SqlWhereModel<'s>>(
        &self,
        table_name: &str,
        entities: &'s [TEntity],

        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Result<(), MyPostgressError> {
        let process_name = format!("bulk_delete from table {}", table_name);

        let (sql, params) = crate::sql_delete::build_bulk_delete(table_name, entities);

        let connection = self.get_connection().await?;

        let mut params_to_invoke = Vec::with_capacity(params.len());

        for param in params {
            params_to_invoke.push(param.value);
        }

        connection
            .execute_sql(
                sql.as_str(),
                &params_to_invoke,
                &process_name,
                #[cfg(feature = "with-logs-and-telemetry")]
                telemetry_context,
            )
            .await?;

        Ok(())
    }
}

async fn do_connection(
    app_name: String,
    postgres_settings: Arc<dyn PostgressSettings + Sync + Send + 'static>,
    shared_connection: Arc<RwLock<Option<PostgresConnection>>>,
    #[cfg(feature = "with-logs-and-telemetry")] logger: Arc<dyn Logger + Sync + Send + 'static>,
) {
    loop {
        let conn_string = postgres_settings.get_connection_string().await;

        let conn_string = super::connection_string::format(conn_string.as_str(), app_name.as_str());

        if conn_string.contains("sslmode=require") {
            #[cfg(feature = "with-tls")]
            create_and_start_with_tls(
                conn_string,
                &shared_connection,
                #[cfg(feature = "with-logs-and-telemetry")]
                &logger,
            )
            .await;
            #[cfg(not(feature = "with-tls"))]
            {
                #[cfg(feature = "with-logs-and-telemetry")]
                logger.write_error(
                    "PostgressConnection".to_string(),
                    "Postgres connection with sslmode=require is not supported without tls feature"
                        .to_string(),
                    None,
                );

                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        } else {
            create_and_start_no_tls(
                conn_string,
                &shared_connection,
                #[cfg(feature = "with-logs-and-telemetry")]
                &logger,
            )
            .await
        }
    }
}

async fn create_and_start_no_tls(
    connection_string: String,
    shared_connection: &Arc<RwLock<Option<PostgresConnection>>>,
    #[cfg(feature = "with-logs-and-telemetry")] logger: &Arc<dyn Logger + Sync + Send + 'static>,
) {
    let result = tokio_postgres::connect(connection_string.as_str(), NoTls).await;

    let connected_date_time = DateTimeAsMicroseconds::now();

    match result {
        Ok((client, connection)) => {
            println!(
                "{}: Postgres SQL Connection is estabiled",
                connected_date_time.to_rfc3339()
            );

            let connected = {
                let mut write_access = shared_connection.write().await;
                let postgress_connection = PostgresConnection::new(
                    client,
                    Duration::from_secs(5),
                    #[cfg(feature = "with-logs-and-telemetry")]
                    logger.clone(),
                );
                let result = postgress_connection.connected.clone();
                *write_access = Some(postgress_connection);
                result
            };

            let connected_spawned = connected.clone();

            #[cfg(feature = "with-logs-and-telemetry")]
            let logger_spawned = logger.clone();

            tokio::spawn(async move {
                match connection.await {
                    Ok(_) => {
                        println!(
                            "{}: Connection estabilshed at {} is closed.",
                            DateTimeAsMicroseconds::now().to_rfc3339(),
                            connected_date_time.to_rfc3339(),
                        );
                    }
                    Err(err) => {
                        println!(
                            "{}: Connection estabilshed at {} is closed with error: {}",
                            DateTimeAsMicroseconds::now().to_rfc3339(),
                            connected_date_time.to_rfc3339(),
                            err
                        );

                        #[cfg(feature = "with-logs-and-telemetry")]
                        logger_spawned.write_fatal_error(
                            "Potgress background".to_string(),
                            format!("Exist connection loop"),
                            None,
                        );
                    }
                }

                connected_spawned.store(false, Ordering::SeqCst);
            });

            while connected.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        Err(err) => {
            #[cfg(not(feature = "with-logs-and-telemetry"))]
            println!(
                "{}: Postgres SQL Connection is closed with Err: {:?}",
                DateTimeAsMicroseconds::now().to_rfc3339(),
                err
            );

            #[cfg(feature = "with-logs-and-telemetry")]
            logger.write_fatal_error(
                "CreatingPosrgress".to_string(),
                format!("Invalid connection string. {:?}", err),
                None,
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}

#[cfg(feature = "with-tls")]
async fn create_and_start_with_tls(
    connection_string: String,
    shared_connection: &Arc<RwLock<Option<PostgresConnection>>>,
    #[cfg(feature = "with-logs-and-telemetry")] logger: &Arc<dyn Logger + Sync + Send + 'static>,
) {
    let builder = SslConnector::builder(SslMethod::tls()).unwrap();

    let connector = MakeTlsConnector::new(builder.build());

    let result = tokio_postgres::connect(connection_string.as_str(), connector).await;
    #[cfg(feature = "with-logs-and-telemetry")]
    let logger_spawned = logger.clone();
    match result {
        Ok((client, connection)) => {
            let connected = {
                let mut write_access = shared_connection.write().await;
                let postgress_connection = PostgresConnection::new(
                    client,
                    Duration::from_secs(5),
                    #[cfg(feature = "with-logs-and-telemetry")]
                    logger.clone(),
                );
                let result = postgress_connection.connected.clone();
                *write_access = Some(postgress_connection);
                result
            };

            let connected_copy = connected.clone();

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!(
                        "{}: connection error: {}",
                        DateTimeAsMicroseconds::now().to_rfc3339(),
                        e
                    );
                }
                #[cfg(feature = "with-logs-and-telemetry")]
                logger_spawned.write_fatal_error(
                    "Potgress background".to_string(),
                    format!("Exist connection loop"),
                    None,
                );

                connected_copy.store(false, Ordering::SeqCst);
            });

            while connected.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        Err(_err) => {
            #[cfg(feature = "with-logs-and-telemetry")]
            logger.write_fatal_error(
                "CreatingPosrgress".to_string(),
                format!("Invalid connection string. {:?}", _err),
                None,
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
