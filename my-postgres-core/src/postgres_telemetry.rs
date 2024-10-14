use std::{sync::Arc, time::Duration};

use my_logger::LogEventCtx;
#[cfg(feature = "with-logs-and-telemetry")]
use my_telemetry::MyTelemetryContext;
use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(Clone)]
pub struct RequestContext {
    #[cfg(feature = "with-logs-and-telemetry")]
    pub telemetry_context: Option<MyTelemetryContext>,
    pub started: DateTimeAsMicroseconds,
    pub process_name: Arc<String>,
    pub sql_request_time_out: Duration,
}

impl RequestContext {
    pub fn new(
        sql_request_time_out: Duration,
        process_name: String,
        #[cfg(feature = "with-logs-and-telemetry")] telemetry_context: Option<&MyTelemetryContext>,
    ) -> Self {
        Self {
            sql_request_time_out,
            #[cfg(feature = "with-logs-and-telemetry")]
            telemetry_context: telemetry_context.cloned(),
            started: DateTimeAsMicroseconds::now(),
            process_name: Arc::new(process_name),
        }
    }

    #[cfg(feature = "with-logs-and-telemetry")]
    pub async fn write_success(
        &self,
        ok_message: String,
        tags: Option<Vec<my_telemetry::TelemetryEventTag>>,
    ) {
        if let Some(ctx) = self.telemetry_context.as_ref() {
            my_telemetry::TELEMETRY_INTERFACE
                .write_success(
                    ctx,
                    self.started,
                    self.process_name.to_string(),
                    ok_message,
                    tags,
                )
                .await;
        }
    }

    pub async fn write_fail(
        &self,
        fail_message: String,
        sql: Option<&str>,
        #[cfg(feature = "with-logs-and-telemetry")] tags: Option<
            Vec<my_telemetry::TelemetryEventTag>,
        >,
    ) {
        #[cfg(feature = "with-logs-and-telemetry")]
        if my_telemetry::TELEMETRY_INTERFACE.is_telemetry_set_up() {
            if let Some(ctx) = self.telemetry_context.as_ref() {
                my_telemetry::TELEMETRY_INTERFACE
                    .write_fail(
                        ctx,
                        self.started,
                        self.process_name.to_string(),
                        fail_message.to_string(),
                        tags,
                    )
                    .await;
            }
        }

        let ctx = if let Some(sql) = sql {
            LogEventCtx::new().add("sql", sql)
        } else {
            LogEventCtx::new()
        };
        my_logger::LOGGER.write_error(self.process_name.to_string(), fail_message, ctx);
    }
}
