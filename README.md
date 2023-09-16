### My Postgres
Highly recommended using this library with Macros library: https://github.com/MyJetTools/my-postgres-macros

### Connection String

Please use the connection string with the template

```
host=xxx port=5432 dbname=xxx user=xxx password=xxx sslmode=require
```

or if we do not want to use tls
```
host=xxx port=5432 dbname=xxx user=xxx password=xxx
```

### TLS Feature

For the sake of optimization - the TLS feature is not included by default.

If you are planning to use connections with TLS required, please add a feature

```toml
[dependencies]
my-postgres = { tag = "xxx", git = "https://github.com/MyJetTools/my-postgres.git", features = [
    "with-tls",
] }
```
whether to use TLS or not is detected by **sslmode=require** in the connection string



### Application name

Please do not include application name in the connection sting, since it's injected as an in parameter of the MyPostgres structure

```rust
pub struct MySettings;

#[async_trait::async_trait]
impl PostgresSettings for MySettings {
    async fn get_connection_string(&self) -> String {
        "host=xxx port=5432 dbname=xxx user=xxx password=xxx sslmode=require".to_string()
    }
}


#[tokio::main]
async fn main() {
  let postgres_settings = Arc::new(MySettings);

  let application_name = "TestApp";

    let my_postgres = my_postgres::MyPostgres::from_settings(application_name, postgres_settings)
        .build()
        .await;
}

```


If there is a table schema to be applied
```rust

#[derive(TableSchema)]
pub struct TestETagDto {
    #[primary_key(0)]
    pub id: i32,

    #[sql_type("timestamp")]
    pub date: DateTimeAsMicroseconds,

    #[default_value("test")]
    pub value: String,

    #[db_field_name("etag")]
    #[e_tag]
    pub e_tag: i64,
}


#[tokio::main]
async fn main() {
  let postgres_settings = Arc::new(MySettings);

  let application_name = "TestApp";

    let partition_key_name = "test_pk";

    let my_postgres = my_postgres::MyPostgres::from_settings(application_name, postgres_settings)
        .with_table_schema_verification::<TestDto>("test", Some(partition_key_name.to_string()))
        .build()
        .await;
}
```


### Sql request timeout

Default SqlRequest timeout is 5 seconds. To specify the other one please use

```rust

#[tokio::main]
async fn main() {
  let postgres_settings = Arc::new(MySettings);

  let application_name = "TestApp";

    let my_postgres = my_postgres::MyPostgres::from_settings(application_name, postgres_settings)
        .set_sql_request_timeout(Duration::from_secs(1))
        .build()
        .await;
}
```


### Shared Sql connection

The Connection can be created and then injected into several MyPostgres structures


```rust

#[tokio::main]
async fn main() {
    let postgres_connection =
        PostgresConnection::new_as_single_connection(application_name, postgres_settings).await;

    let postgres_connection = Arc::new(postgres_connection);

    let my_postgres1 = my_postgres::MyPostgres::from_connection_string(postgres_connection.clone())
        .build()
        .await;

    let my_postgres2 = my_postgres::MyPostgres::from_connection_string(postgres_connection)
        .build()
        .await;
}

```


### Sql connection pool

```rust

#[tokio::main]
async fn main() {
    let postgres_connection =
        PostgresConnection::new_as_multiple_connections(application_name, postgres_settings, 3);

    let postgres_connection = Arc::new(postgres_connection);

    let my_postgres1 = my_postgres::MyPostgres::from_connection_string(postgres_connection.clone())
        .build()
        .await;

    let my_postgres2 = my_postgres::MyPostgres::from_connection_string(postgres_connection)
        .build()
        .await;
}

```

### My Postgres Macros
https://github.com/MyJetTools/my-postgres-macros/wiki
