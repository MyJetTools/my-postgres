use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use rust_extensions::{date_time::DateTimeAsMicroseconds, StopWatch};
use tokio::{sync::RwLock, time::error::Elapsed};
use tokio_postgres::Row;

use crate::{sql::SqlData, sql_select::SelectEntity, MyPostgresError, PostgresSettings};

use super::{PostgresReadStream, PostgresRowReadStream};

pub struct PostgresConnectionSingleThreaded {
    postgres_client: Option<tokio_postgres::Client>,
    to_start: Option<Arc<PostgresConnectionInner>>,
    db_name: String,
}

impl PostgresConnectionSingleThreaded {
    pub fn new(db_name: String) -> Self {
        Self {
            postgres_client: None,
            to_start: None,
            db_name,
        }
    }

    pub fn new_connection(&mut self, client: tokio_postgres::Client) {
        self.postgres_client = Some(client);
    }

    pub fn disconnect(&mut self) {
        self.postgres_client = None;
    }

    pub fn get_connection(&self) -> Result<Option<&tokio_postgres::Client>, MyPostgresError> {
        if self.to_start.is_some() {
            return Ok(None);
        }

        if let Some(client) = &self.postgres_client {
            Ok(client.into())
        } else {
            Err(MyPostgresError::NoConnection)
        }
    }

    pub fn start_connection(
        &mut self,
        #[cfg(feature = "with-ssh")] ssh_config: Option<crate::ssh::PostgresSshConfig>,
    ) -> bool {
        if let Some(to_start) = self.to_start.take() {
            tokio::spawn(super::connection_loop::start_connection_loop(
                to_start,
                self.db_name.clone(),
                #[cfg(feature = "with-ssh")]
                ssh_config,
            ));
            return true;
        }
        false
    }

    pub fn get_connection_mut(&mut self) -> Result<&mut tokio_postgres::Client, MyPostgresError> {
        if let Some(client) = &mut self.postgres_client {
            Ok(client.into())
        } else {
            Err(MyPostgresError::NoConnection)
        }
    }
}

pub struct PostgresConnectionInner {
    pub inner: Arc<RwLock<PostgresConnectionSingleThreaded>>,
    pub connected: Arc<AtomicBool>,
    pub app_name: String,
    pub postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
    pub to_be_disposable: AtomicBool,
    #[cfg(feature = "with-ssh")]
    ssh_config: Option<crate::ssh::PostgresSshConfig>,
}

impl PostgresConnectionInner {
    pub fn new(
        app_name: String,
        postgres_settings: Arc<dyn PostgresSettings + Sync + Send + 'static>,
        db_name: String,
        #[cfg(feature = "with-ssh")] ssh_config: Option<crate::ssh::PostgresSshConfig>,
    ) -> Self {
        Self {
            app_name,
            postgres_settings,
            inner: Arc::new(RwLock::new(PostgresConnectionSingleThreaded::new(db_name))),
            connected: Arc::new(AtomicBool::new(false)),

            to_be_disposable: AtomicBool::new(false),
            #[cfg(feature = "with-ssh")]
            ssh_config,
        }
    }

    pub async fn engage(&self, to_start: Arc<PostgresConnectionInner>) {
        let mut write_access = self.inner.write().await;
        write_access.to_start = Some(to_start);
    }

    pub fn set_to_be_disposable(&self) {
        self.to_be_disposable.store(true, Ordering::Relaxed);
        self.disconnect();
    }

    pub fn is_to_be_disposable(&self) -> bool {
        self.to_be_disposable.load(Ordering::Relaxed)
    }

    pub fn disconnect(&self) {
        let tokio_postgres_client = self.inner.clone();

        let connected = self.connected.clone();

        tokio::spawn(async move {
            let mut write_access = tokio_postgres_client.write().await;
            write_access.disconnect();
            connected.store(false, std::sync::atomic::Ordering::SeqCst);
        });
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn handle_connection_is_established(
        &self,
        postgres_client: tokio_postgres::Client,
        host: &str,
        tls: bool,
    ) -> DateTimeAsMicroseconds {
        let connected_date_time = DateTimeAsMicroseconds::now();

        if tls {
            println!(
                "{}: TLS Postgres SQL Connection is established to {host}",
                connected_date_time.to_rfc3339()
            );
        } else {
            println!(
                "{}: NoTls Postgres SQL Connection is established to {host}",
                connected_date_time.to_rfc3339()
            );
        }

        {
            let mut write_access: tokio::sync::RwLockWriteGuard<
                '_,
                PostgresConnectionSingleThreaded,
            > = self.inner.write().await;
            write_access.new_connection(postgres_client);
        };
        self.connected.store(true, Ordering::Relaxed);

        connected_date_time
    }

    pub async fn execute_with_timeout<
        TResult,
        TFuture: Future<Output = Result<TResult, tokio_postgres::Error>>,
    >(
        &self,
        sql: Option<&str>,
        execution: TFuture,
        ctx: &crate::RequestContext,
    ) -> Result<TResult, MyPostgresError> {
        let timeout_result: Result<Result<TResult, tokio_postgres::Error>, Elapsed> =
            tokio::time::timeout(ctx.sql_request_time_out, execution).await;

        let result = if timeout_result.is_err() {
            self.connected.store(false, Ordering::Relaxed);
            Err(MyPostgresError::TimeOut(ctx.sql_request_time_out))
        } else {
            match timeout_result.unwrap() {
                Ok(result) => Ok(result),
                Err(err) => Err(MyPostgresError::PostgresError(err)),
            }
        };

        /*
               if let Err(err) = &result {
                   println!(
                       "{}: Execution request {} finished with error {:?}",
                       DateTimeAsMicroseconds::now().to_rfc3339(),
                       process_name,
                       err
                   );

                   if let Some(sql) = sql {
                       let sql = if sql.len() > 255 { &sql[..255] } else { sql };
                       println!("SQL: {}", sql);
                   }
               }
        */

        match &result {
            Ok(_) => {
                #[cfg(feature = "with-logs-and-telemetry")]
                ctx.write_success("Sql execution ok".to_string(), get_sql_telemetry_tags(sql))
                    .await;
            }
            Err(err) => {
                ctx.write_fail(
                    format!("{:?}", err),
                    sql,
                    #[cfg(feature = "with-logs-and-telemetry")]
                    get_sql_telemetry_tags(sql),
                )
                .await;
            }
        }

        result
    }

    async fn start_connection(&self) {
        let started = {
            let mut write_access = self.inner.write().await;
            write_access.start_connection(
                #[cfg(feature = "with-ssh")]
                self.ssh_config.clone(),
            )
        };

        if started {
            loop {
                if self.is_connected() {
                    break;
                }
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        }
    }

    pub async fn execute_sql(
        &self,
        sql: &SqlData,
        ctx: &crate::RequestContext,
    ) -> Result<u64, MyPostgresError> {
        let mut start_connection = false;
        let sw = StopWatch::new();

        loop {
            if start_connection {
                self.start_connection().await;
            }
            let connection_access = self.inner.read().await;

            let connection_access = connection_access.get_connection()?;

            match connection_access {
                Some(connection_access) => {
                    let params = sql.values.get_values_to_invoke();

                    let execution = connection_access.execute(&sql.sql, params.as_slice());

                    if ctx.is_debug {
                        println!("Executing SQL: {}", &sql.sql);
                    }

                    let result = self
                        .execute_with_timeout(Some(&sql.sql), execution, &ctx)
                        .await;

                    match result {
                        Ok(result) => {
                            if ctx.is_debug {
                                println!(
                                    "SQL: {} executed in {}",
                                    &sql.sql,
                                    sw.duration_as_string()
                                );
                            }
                            return Ok(result);
                        }
                        Err(err) => {
                            self.handle_error(err, &ctx)?;
                        }
                    }
                }
                None => {
                    start_connection = true;
                }
            }
        }
    }

    pub async fn execute_sql_as_vec<'s>(
        &self,
        sql: &SqlData,
        ctx: &crate::RequestContext,
    ) -> Result<Vec<Row>, MyPostgresError> {
        let mut start_connection = false;

        let is_debug = std::env::var("DEBUG_SQL").is_ok();

        let sw = StopWatch::new();

        loop {
            if start_connection {
                self.start_connection().await;
            }

            let connection_access = self.inner.read().await;

            let connection_access = connection_access.get_connection()?;

            match connection_access {
                Some(connection_access) => {
                    if is_debug {
                        println!("Executing SQL: {}", &sql.sql);
                    }

                    let params = sql.values.get_values_to_invoke();
                    let execution = connection_access.query(&sql.sql, params.as_slice());

                    let result = self
                        .execute_with_timeout(Some(&sql.sql), execution, &ctx)
                        .await;

                    match result {
                        Ok(result) => {
                            if is_debug {
                                println!(
                                    "SQL: {} is executed in {}",
                                    &sql.sql,
                                    sw.duration_as_string()
                                );
                            }
                            return Ok(result);
                        }
                        Err(err) => {
                            self.handle_error(err, &ctx)?;
                        }
                    };
                }
                None => {
                    start_connection = true;
                }
            }
        }
    }

    pub async fn execute_sql_as_stream<'s, TEntity: SelectEntity + Send + Sync + 'static>(
        &self,
        sql: &SqlData,
        ctx: crate::RequestContext,
    ) -> Result<PostgresReadStream<TEntity>, MyPostgresError> {
        let mut start_connection = false;

        loop {
            if start_connection {
                self.start_connection().await;
            }

            let connection_access = self.inner.read().await;

            let connection_access = connection_access.get_connection()?;

            match connection_access {
                Some(connection_access) => {
                    if std::env::var("DEBUG_SQL").is_ok() {
                        println!("SQL: {}", &sql.sql);
                    }

                    let params = sql.values.get_values_to_invoke();

                    let execution =
                        connection_access.query_raw(sql.sql.as_str(), params.into_iter());

                    let stream = self
                        .execute_with_timeout(Some(&sql.sql), execution, &ctx)
                        .await?;

                    return Ok(PostgresReadStream::new(
                        sql.sql.to_string(),
                        stream,
                        self.connected.clone(),
                        ctx,
                    ));
                }
                None => {
                    start_connection = true;
                }
            }
        }
    }

    pub async fn execute_sql_as_row_stream(
        &self,
        sql: &SqlData,
        ctx: &crate::RequestContext,
    ) -> Result<PostgresRowReadStream, MyPostgresError> {
        let mut start_connection = false;
        loop {
            if start_connection {
                self.start_connection().await;
            }

            let connection_access = self.inner.read().await;

            let connection_access = connection_access.get_connection()?;

            match connection_access {
                Some(connection_access) => {
                    if std::env::var("DEBUG_SQL").is_ok() {
                        println!("SQL: {}", &sql.sql);
                    }

                    let params = sql.values.get_values_to_invoke();

                    let execution =
                        connection_access.query_raw(sql.sql.as_str(), params.into_iter());

                    let stream = self
                        .execute_with_timeout(Some(&sql.sql), execution, &ctx)
                        .await?;

                    return Ok(PostgresRowReadStream::new(
                        sql.sql.to_string(),
                        stream,
                        self.connected.clone(),
                        ctx.to_owned(),
                    ));
                }
                None => {
                    start_connection = true;
                }
            }
        }
    }

    pub async fn execute_bulk_sql<'s>(
        &self,
        sql_with_params: Vec<SqlData>,
        ctx: crate::RequestContext,
    ) -> Result<(), MyPostgresError> {
        if std::env::var("DEBUG_SQL").is_ok() {
            if let Some(first_value) = sql_with_params.get(0) {
                println!("SQL: {:?}", first_value.sql);
            }
        }

        let mut connection_access = self.inner.write().await;

        let connection_access = connection_access.get_connection_mut()?;

        let execution = {
            let builder = connection_access.build_transaction();
            let transaction = builder.start().await?;

            for sql_data in &sql_with_params {
                transaction
                    .execute(&sql_data.sql, &sql_data.values.get_values_to_invoke())
                    .await?;
            }
            transaction.commit()
        };

        let result = self.execute_with_timeout(None, execution, &ctx).await;

        if let Err(err) = result {
            self.handle_error(err, &ctx)?;
        }

        Ok(())
    }

    fn handle_error(
        &self,
        err: MyPostgresError,
        ctx: &crate::RequestContext,
    ) -> Result<(), MyPostgresError> {
        match &err {
            MyPostgresError::NoConnection => {}
            MyPostgresError::SingleRowRequestReturnedMultipleRows(_) => {}
            MyPostgresError::PostgresError(_) => {}
            MyPostgresError::Other(_) => {
                self.disconnect();
            }
            MyPostgresError::TimeOut(_) => {
                self.disconnect();
            }
            MyPostgresError::ConnectionNotStartedYet => {}
        }

        let now = DateTimeAsMicroseconds::now();

        if now.duration_since(ctx.started).as_positive_or_zero() > ctx.sql_request_time_out {
            return Err(err);
        }

        Ok(())
    }
}

#[cfg(feature = "with-logs-and-telemetry")]
pub fn get_sql_telemetry_tags(sql: Option<&str>) -> Option<Vec<my_telemetry::TelemetryEventTag>> {
    if let Some(sql) = sql {
        Some(vec![my_telemetry::TelemetryEventTag {
            key: "SQL".to_string(),
            value: if sql.len() > 2048 {
                sql[..2048].to_string()
            } else {
                sql.to_string()
            },
        }])
    } else {
        None
    }
}
