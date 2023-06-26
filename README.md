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

Please do not include application name in the connection sting, since it's injected during the construction of the MyPostgress structure as a parameter

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

  let my_postgres =
    my_postgres::MyPostgres::new(conn_string, application_name)
        .await;
}

```