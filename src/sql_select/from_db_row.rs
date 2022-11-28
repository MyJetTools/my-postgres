use rust_extensions::date_time::DateTimeAsMicroseconds;

pub trait FromDbRow<TResult> {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, sql_type: Option<&str>) -> TResult;
}

impl FromDbRow<String> for String {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> String {
        row.get(name)
    }
}

impl FromDbRow<Option<String>> for Option<String> {
    fn from_db_row(
        row: &tokio_postgres::Row,
        name: &str,
        _sql_type: Option<&str>,
    ) -> Option<String> {
        row.get(name)
    }
}

impl FromDbRow<i64> for i64 {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> i64 {
        row.get(name)
    }
}
impl FromDbRow<Option<i64>> for Option<i64> {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> Option<i64> {
        row.get(name)
    }
}

impl FromDbRow<u64> for u64 {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> u64 {
        let result: i64 = row.get(name);
        result as u64
    }
}

impl FromDbRow<Option<u64>> for Option<u64> {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> Option<u64> {
        let result: Option<i64> = row.get(name);
        let result = result?;
        Some(result as u64)
    }
}

impl FromDbRow<i32> for i32 {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> i32 {
        row.get(name)
    }
}

impl FromDbRow<Option<i32>> for Option<i32> {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> Option<i32> {
        row.get(name)
    }
}

impl FromDbRow<u32> for u32 {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> u32 {
        let result: i64 = row.get(name);
        result as u32
    }
}

impl FromDbRow<Option<u32>> for Option<u32> {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> Option<u32> {
        let result: Option<i64> = row.get(name);
        let result = result?;
        Some(result as u32)
    }
}

impl FromDbRow<bool> for bool {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> bool {
        row.get(name)
    }
}

impl FromDbRow<Option<bool>> for Option<bool> {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> Option<bool> {
        row.get(name)
    }
}

impl FromDbRow<f64> for f64 {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> f64 {
        row.get(name)
    }
}

impl FromDbRow<Option<f64>> for Option<f64> {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> Option<f64> {
        row.get(name)
    }
}

impl FromDbRow<f32> for f32 {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> f32 {
        row.get(name)
    }
}

impl FromDbRow<Option<f32>> for Option<f32> {
    fn from_db_row(row: &tokio_postgres::Row, name: &str, _sql_type: Option<&str>) -> Option<f32> {
        row.get(name)
    }
}

impl FromDbRow<DateTimeAsMicroseconds> for DateTimeAsMicroseconds {
    fn from_db_row(
        row: &tokio_postgres::Row,
        name: &str,
        _sql_type: Option<&str>,
    ) -> DateTimeAsMicroseconds {
        let unix_microseconds: i64 = row.get(name);
        DateTimeAsMicroseconds::new(unix_microseconds)
    }
}

impl FromDbRow<Option<DateTimeAsMicroseconds>> for Option<DateTimeAsMicroseconds> {
    fn from_db_row(
        row: &tokio_postgres::Row,
        name: &str,
        _sql_type: Option<&str>,
    ) -> Option<DateTimeAsMicroseconds> {
        let unix_microseconds: Option<i64> = row.get(name);
        let unix_microseconds = unix_microseconds?;
        Some(DateTimeAsMicroseconds::new(unix_microseconds))
    }
}
