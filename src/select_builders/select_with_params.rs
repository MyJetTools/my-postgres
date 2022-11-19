use rust_extensions::StrOrString;

use crate::{SelectEntity, ToSqlString};

pub struct SqlWithParams<'s> {
    pub sql: &'s str,
    pub params: &'s [&'s (dyn tokio_postgres::types::ToSql + Sync)],
    #[cfg(feature = "with-logs-and-telemetry")]
    pub my_telemetry: Option<Vec<my_telemetry::MyTelemetryContext>>,
}

pub trait WithSqlParams<'s> {
    fn inject_sql_params_data(
        &'s self,
        params: &'s [&'s (dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: Option<
            Vec<my_telemetry::MyTelemetryContext>,
        >,
    ) -> SqlWithParams;
}

impl<'s> WithSqlParams<'s> for String {
    fn inject_sql_params_data(
        &'s self,
        params: &'s [&'s (dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: Option<
            Vec<my_telemetry::MyTelemetryContext>,
        >,
    ) -> SqlWithParams {
        SqlWithParams {
            sql: self,
            params,
            #[cfg(feature = "with-logs-and-telemetry")]
            my_telemetry,
        }
    }
}

impl<'s> WithSqlParams<'s> for &'s str {
    fn inject_sql_params_data(
        &'s self,
        params: &'s [&'s (dyn tokio_postgres::types::ToSql + Sync)],
        #[cfg(feature = "with-logs-and-telemetry")] my_telemetry: Option<
            Vec<my_telemetry::MyTelemetryContext>,
        >,
    ) -> SqlWithParams {
        SqlWithParams {
            sql: self,
            params,
            #[cfg(feature = "with-logs-and-telemetry")]
            my_telemetry,
        }
    }
}

impl<'s, TEntity: SelectEntity> ToSqlString<TEntity> for SqlWithParams<'s> {
    fn as_sql(&self) -> StrOrString {
        crate::sql_formatter::format_sql(StrOrString::crate_as_str(self.sql), || {
            TEntity::get_select_fields()
        })
    }

    fn get_params_data(&self) -> Option<&[&(dyn tokio_postgres::types::ToSql + Sync)]> {
        Some(self.params)
    }

    #[cfg(feature = "with-logs-and-telemetry")]
    fn get_telemetry(&self) -> Option<&[my_telemetry::MyTelemetryContext]> {
        if let Some(result) = &self.my_telemetry {
            Some(result.as_slice())
        } else {
            None
        }
    }
}
