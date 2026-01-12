# My Postgres

Rust library for ergonomic PostgreSQL access with async Tokio, connection pooling, optional TLS/SSH, retries, telemetry hooks, and proc-macros that generate SQL models and table schemas. Documentation from the project wiki is consolidated here for convenience ([wiki link](https://github.com/MyJetTools/my-postgres/wiki)).

## Workspace crates
- `my-postgres-core`: async connection management (TLS, SSH), SQL builders, retries, telemetry/logging hooks, schema sync.
- `my-postgres`: thin facade that re-exports core and (optionally) proc-macros.
- `my-postgres-macros`: proc-macros for schema and CRUD model generation.
- `my-postgres-tests`: example/tests crate using the library with macros.

## AI navigation cheat sheet
- Wiki pages (mirrored below): [Home/Overview](https://github.com/MyJetTools/my-postgres/wiki), [GroupBy fields](https://github.com/MyJetTools/my-postgres/wiki/GroupBy-fields), [Macros](https://github.com/MyJetTools/my-postgres/wiki/Macros), [Other attributes](https://github.com/MyJetTools/my-postgres/wiki/Other-attributes), [Other types](https://github.com/MyJetTools/my-postgres/wiki/Other-Types), [Select attributes](https://github.com/MyJetTools/my-postgres/wiki/Select-attributes), [TableSchema macros](https://github.com/MyJetTools/my-postgres/wiki/TableSchema-macros), [Where Model](https://github.com/MyJetTools/my-postgres/wiki/Where-Model).
- Attribute sources: `my-postgres-macros/src/attributes/`.
- Table schema sync code paths: `my-postgres-core/src/sync_table_schema/`.
- SQL builders: `my-postgres-core/src/sql/` (`select`, `insert`, `update`, `union`, `where`, etc.).
- GroupBy helpers: `my-postgres-core/src/group_by_fields/`.
- Connection/pool logic: `my-postgres-core/src/connection/`.
- Tests: `my-postgres-macros/tests/src/dto/` (table_schema_tests, etc.) and `my-postgres-tests/src/`.

## Install & features
```toml
[dependencies]
my-postgres = { tag = "xxx", git = "https://github.com/MyJetTools/my-postgres.git", features = [
    # opt-in flags; pick what you need
    "with-tls",          # enables TLS (openssl + postgres-openssl in core)
    "with-ssh",          # enables SSH tunneling support
    "with-telemetry",    # enables my-telemetry + logging hooks
    "macros",            # pulls in proc-macros for derives
] }
```
- TLS is auto-detected via `sslmode=require` in the connection string.
- SSH tunneling is enabled when the connection string contains `ssh=user@host:port`.
- Keep TLS/SSH features disabled unless needed for lean builds.

## Connection strings
- TLS example: `host=xxx port=5432 dbname=xxx user=xxx password=xxx sslmode=require`
- Non-TLS: `host=xxx port=5432 dbname=xxx user=xxx password=xxx`
- SSH tunnel: `host=xxx port=5432 dbname=xxx user=xxx password=xxx ssh=user@sshhost:22`
- Application name is passed separately when constructing `MyPostgres`; do **not** embed it in the connection string.

## Quickstart
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

## Table schema verification
Use `TableSchema` derive to describe a table and have it verified/applied at startup:
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
        .with_table_schema_verification::<TestETagDto>("test", Some(partition_key_name.to_string()))
        .build()
        .await;
}
```

## Timeouts and retries
- Default SQL request timeout: 5s. Override with `.set_sql_request_timeout(Duration::from_secs(n))` on the builder.
- Core has retry helpers for transient errors (see `with_retries` in `my-postgres-core`).

## Connection reuse & pooling
Create a single connection and share it, or build a small pool:
```rust
// single shared connection
let conn = PostgresConnection::new_as_single_connection(application_name, postgres_settings).await;
let conn = Arc::new(conn);
let db1 = my_postgres::MyPostgres::from_connection_string(conn.clone()).build().await;
let db2 = my_postgres::MyPostgres::from_connection_string(conn).build().await;

// pool of N connections
let pool = PostgresConnection::new_as_multiple_connections(application_name, postgres_settings, 3);
let pool = Arc::new(pool);
let db = my_postgres::MyPostgres::from_connection_string(pool).build().await;
```

## Proc-macros (schema & models)
Common derives and attributes (full list in code under `my-postgres-macros/src/attributes`):
- `SelectDbEntity` / `BulkSelectDbEntity`: map query rows to structs. Attributes: `db_column_name`, `sql`, `sql_type`, `order_by`, `order_by_desc`, `group_by`, `primary_key`, `default_if_null`, `wrap_column_name`, `force_cast_db_type`, `line_no`.
- `InsertDbEntity`: insert models. Attributes: `db_column_name`, `ignore`, `bigint`, `json`, `sql_type`, `primary_key`, `e_tag`, `default_if_null`, `ignore_if_none`, `wrap_column_name`.
- `UpdateDbEntity`: update models. Attributes: `db_column_name`, `primary_key`, `ignore`, `sql_type`, `e_tag`, `default_if_null`, `ignore_if_none`, `wrap_column_name`, `json`.
- `WhereDbModel`: where/limit/offset support. Attributes: `db_column_name`, `bigint`, `operator`, `ignore_if_none`, `ignore`, `limit`, `offset`, `sql_type`, `default_if_null`, `wrap_column_name`, `inside_json`, `inline_where_model`.
- `TableSchema`: table definition/DDL generation. Attributes: `bigint`, `sql_type`, `ignore_table_column`, `primary_key`, `db_index`, `default_if_null`, `default_value`, `wrap_column_name`, `db_column_name`, `generate_select_model`, `generate_update_model`, `generate_where_model`.
- Enum helpers: `DbEnumAsU8/I8/U16/I16/U32/I32/U64/I64/String` (+ `WithModel` variants) to map enums to DB values with optional defaults.
- JSON helpers: `MyPostgresJsonModel`, `MyPostgresJsonWhereModel` for JSON column shapes and filters.
- Raw where macro: `#[where_raw_model]` to plug custom where logic.

## SQL helpers (core)
- Builders for select/insert/update/where, group-by fields, unions, and SQL value helpers live in `my-postgres-core::sql` and related modules.
- GroupBy helpers cover avg/count/max/min/sum and field type helpers.
- Utilities exist for null handling, column naming, and conflict strategies (`update_conflict_type`).

### GroupBy fields (wiki)
- Types live in `my_postgres::group_by_fields` to aggregate alongside grouped columns.
- Mark grouping keys with `#[group_by]`; use aggregate wrappers like `GroupByMin<T>` / `GroupByMax<T>` on other fields.
- Example derived model and generated SQL (from wiki):
```rust
#[derive(SelectDbEntity, Debug)]
pub struct MinMaxKeySelectDto {
    #[group_by]
    pub candle_type: i64,
    #[group_by]
    pub instrument_id: String,
    pub is_bid: bool,

    #[db_column_name("date")]
    pub min: GroupByMin<i64>,
    #[db_column_name("date")]
    pub max: GroupByMax<i64>,
}
// Generates: SELECT candle_type,instrument_id,is_bid,MIN(date)::bigint as "min",MAX(date)::bigint as "max"
```

### Macros use-cases (wiki)
- Uses `tokio_postgres` under the hood; `DateTimeAsMicroseconds` for datetime fields. Attribute reference: `my-postgres-macros/src/attributes`.
- Insert: `#[derive(InsertDbEntity)]` on a DTO; call `insert_db_entity(&dto, TABLE)` (optionally with telemetry). Generates `INSERT INTO ... VALUES ($1, $2, ...)`.
- Update: mark keys with `#[primary_key]`; call `update_db_entity(&dto, TABLE)` to generate `UPDATE ... SET ... WHERE ...`.
- Insert or update: derive both `InsertDbEntity` and `UpdateDbEntity`; call `insert_or_update_db_entity(&dto, TABLE, PK_NAME)` to emit `ON CONFLICT ON CONSTRAINT {PK_NAME} DO UPDATE`.
- Insert if not exists: `insert_db_entity_if_not_exists(&dto, TABLE, PK_NAME)` -> `ON CONFLICT DO NOTHING`.
- Delete: derive `WhereDbModel` for filters; call `delete_db_entity(&where_dto, TABLE)`.
- Select: derive `SelectDbEntity` + `WhereDbModel`; use `query_rows` (Vec) or `query_single_row` (Option). Group-by also supported via `#[group_by]` / `#[sql]`.
- Bulk select: combine `BulkSelectDbEntity` (+ optional `SelectDbEntity`); build `BulkSelectBuilder` and call `bulk_query_rows` or `bulk_query_rows_with_transformation` to batch requests by `line_no`.
- Concurrent insert or update with `e_tag`: add `#[e_tag]` i64 field and use `concurrent_insert_or_update_single_entity` to gate updates on matching etag.

#### Macros examples
```rust
// Insert
#[derive(InsertDbEntity)]
pub struct KeyValueDto {
    pub client_id: String,
    pub key: String,
    pub value: String,
}
postgres_client.insert_db_entity(&KeyValueDto { client_id, key, value }, TABLE).await?;

// Update (mark keys)
#[derive(UpdateDbEntity)]
pub struct KeyValueDto {
    #[primary_key]
    pub client_id: String,
    #[primary_key]
    pub key: String,
    pub value: String,
}
postgres_client.update_db_entity(&KeyValueDto { client_id, key, value }, TABLE).await?;

// Insert or update
#[derive(InsertDbEntity, UpdateDbEntity)]
pub struct KeyValueDto {
    #[primary_key]
    pub client_id: String,
    #[primary_key]
    pub key: String,
    pub value: String,
}
postgres_client
    .insert_or_update_db_entity(&KeyValueDto { client_id, key, value }, TABLE, PK_NAME)
    .await?;

// Select (vector)
#[derive(WhereDbModel)]
pub struct GetInputParam {
    pub client_id: String,
    pub key: String,
}
#[derive(SelectDbEntity)]
pub struct KeyValueDto {
    pub client_id: String,
    pub key: String,
    pub value: String,
}
let rows: Vec<KeyValueDto> = postgres_client.query_rows(TABLE, &GetInputParam { client_id, key }).await?;

// Bulk select
#[derive(BulkSelectDbEntity, SelectDbEntity)]
pub struct BulkSelectKeyValueDto {
    #[line_no]
    pub line_no: i32,
    pub client_id: String,
    pub key: String,
    pub value: String,
}
let builder = BulkSelectBuilder::new(TABLE, keys);
let rows: Vec<KeyValueDto> = postgres_client.bulk_query_rows(&builder).await?;

// Group-by with aggregations
#[derive(SelectDbEntity, Debug)]
pub struct MinMaxKeySelectDto {
    #[group_by]
    pub candle_type: i64,
    #[group_by]
    pub instrument_id: String,
    pub is_bid: bool,
    #[db_column_name("date")]
    pub min: GroupByMin<i64>,
    #[db_column_name("date")]
    pub max: GroupByMax<i64>,
}
let rows: Vec<MinMaxKeySelectDto> = postgres_client.query_rows(TABLE, &WhereDto { /* filters */ }).await?;

// Concurrent insert/update with e_tag
#[derive(SelectDbEntity, InsertDbEntity, UpdateDbEntity)]
pub struct TestETagDto {
    #[primary_key]
    pub id: i32,
    #[sql_type("timestamp")]
    pub date: DateTimeAsMicroseconds,
    #[db_field_name("etag")]
    #[e_tag]
    pub e_tag: i64,
}
#[derive(WhereDbModel)]
pub struct ETagWhere {
    pub id: i32,
}
my_postgres.concurrent_insert_or_update_single_entity(
    "test-table",
    &ETagWhere { id: 1 },
    || Some(TestETagDto { id: 1, date: DateTimeAsMicroseconds::now(), e_tag: 0 }),
    |itm| {
        itm.date = DateTimeAsMicroseconds::now();
        true
    },
);
```

### Other attributes (wiki)
- `#[ignore]`: skip a field in SQL rendering.
- `#[ignore_if_none]`: skip if the Option is `None`.
- `#[wrap_column_name]`: wrap the column in quotes to avoid reserved-word clashes.
- `#[json]`: deserialize text cell as JSON into the field.
- `#[sql("expr")]`: substitute a column with a custom SQL expression.
- `#[db_field_name("db_col")]`: map struct field to a differently named DB column.
- `#[inside_json("path.to.leaf")]`: target JSON subfields in where models (`->` / `->>` accessors).

```rust
// ignore / ignore_if_none
#[derive(SelectDbEntity)]
pub struct WhereDto {
    #[ignore]
    pub dt_from: Option<DateTimeAsMicroseconds>,
}

#[derive(WhereDbModel)]
pub struct WhereDtoOpt {
    #[ignore_if_none]
    pub dt_from: Option<DateTimeAsMicroseconds>,
}

// wrap_column_name
#[derive(SelectDbEntity)]
pub struct Wrapped {
    #[wrap_column_name]
    pub column_name: i32,
}

// json
#[derive(SelectDbEntity)]
pub struct MyDto {
    #[json]
    pub dt_from: String,
}

// sql expression + group_by
#[derive(SelectDbEntity)]
pub struct StatisticsDto {
    #[sql("count(*)::int")]
    pub count: i32,
    #[group_by]
    pub asset_id: String,
}

// db_field_name
#[derive(WhereDbModel)]
pub struct WhereDto2<'s> {
    #[db_field_name("dt")]
    pub dt_to: Option<DateTimeAsMicroseconds>,
}

// inside_json
#[derive(WhereDbModel)]
pub struct JsonWhere {
    #[inside_json("field_name.next_level")]
    pub my_json_field: String,
}
// Generates: "my_json_field"->'field_name'->>'next_level' = $1
```

### Other types (wiki)
- `DateTimeAsMicroseconds` can map to `bigint` or `timestamp` columns via `#[sql_type("bigint")]` or `#[sql_type("timestamp")]`.
- JSON columns: you can either deserialize with `#[json]` on the field or declare the DB type with `#[sql_type("json")]`/`#[sql_type("jsonb")]`; both can be combined if you want typed JSON and explicit SQL type.

```rust
#[derive(SelectDbEntity)]
pub struct HistoryDtoBigint {
    #[sql_type("bigint")]
    pub date_time: DateTimeAsMicroseconds,
}

#[derive(SelectDbEntity)]
pub struct HistoryDtoTs {
    #[sql_type("timestamp")]
    pub date_time: DateTimeAsMicroseconds,
}

#[derive(SelectDbEntity)]
pub struct JsonTypedDto {
    #[sql_type("jsonb")]
    #[json]
    pub payload: String, // or a typed struct if you prefer
}
```

### Select attributes (wiki)
- `#[order_by]` / `#[order_by_desc]`: include the field in ORDER BY (asc/desc).
- `#[wrap_column_name("expr with ${}")]`: render a custom expression, substituting `${}` with the field/column name.

```rust
#[derive(SelectDbEntity)]
pub struct OrderedDto {
    #[order_by]
    pub dt_from: String,
}

#[derive(Debug, Clone, PartialEq, SelectDbEntity)]
pub struct DbInfoEntity {
    pub table_schema: Option<String>,
    pub table_name: Option<String>,
    #[wrap_column_name("pg_total_relation_size(table_schema || '.' || table_name) AS ${}")]
    pub total_size: Option<i64>,
}
```

### TableSchema macros (wiki)
- Auto-creates/updates tables: creates missing tables/columns/PKs/indexes; recreates PKs/indexes if definitions differ; can relax NOT NULL to NULL. Limitations: cannot change column types or tighten NULL→NOT NULL automatically.
- `#[primary_key(order)]`: order defines PK sequence (0,1,2… or 10,20,30…).
- `#[db_index(id, index_name, is_unique, order)]`: mark fields to be combined into one index when `index_name`/`is_unique` match; `id` must be unique per index field; `order` is ASC/DESC.
- Other field attributes: `#[db_field_name]`, `#[sql_type]`, `#[ignore_table_column]`, `#[default_value("literal")]`; enum defaults via `#[default_value]` on the enum case.
- Generated models: `#[generate_update_model(name, param_type="where"|"update")]`, `#[generate_where_model(name, operator, operator_from, operator_to, as_str, as_option, as_vec, ignore_if_none, limit="field")]`, `#[generate_select_model("Name")]`.
- Schema verification: `.with_table_schema_verification::<MyDto>("table_name", Some("pk_name".to_string()))` during builder setup.

```rust
// PK ordering + index
#[derive(InsertDbEntity, TableSchema)]
pub struct ClientCredentialsDto {
    #[primary_key(0)]
    #[db_index(id:0, index_name: "email_idx", is_unique: true, order: "ASC")]
    pub id: String,
    #[db_index(id:1, index_name: "email_idx", is_unique: true, order: "ASC")]
    pub email: String,
    pub hash: String,
    #[sql_type("timestamp")]
    pub created: DateTimeAsMicroseconds,
}

// Generate update/where/select models
#[derive(TableSchema)]
pub struct MyTableModel {
    #[generate_update_model(name:"UpdateAcceptedDto", param_type:"where")]
    pub id: String,
    #[generate_update_model(name:"UpdateAcceptedDto", param_type:"update")]
    pub accepted: Option<DateTimeAsMicroseconds>,

    #[generate_where_model(name:"ByTraderAndDate", as_str=true, as_vec=true, limit:"limit_field")]
    #[db_column_name("my_trader_id")]
    pub trader_id: String,

    #[sql_type("timestamp")]
    #[generate_where_model(name:"ByTraderAndDate", operator_from: ">", operator_to: "<")]
    pub date: DateTimeAsMicroseconds,

    #[generate_select_model("MySelectDto")]
    #[db_column_name("my_trader_id")]
    pub trader_id_for_select: String,
}

// Schema verification on startup
let my_postgres = my_postgres::MyPostgres::from_settings(app_name, settings)
    .with_table_schema_verification::<ClientCredentialsDto>("client_credentials", Some("client_credentials_pk".to_string()))
    .build()
    .await;
```

Tests for TableSchema macros live at `my-postgres-macros/tests/src/dto/table_schema_tests`.

### Where Model (wiki)
- `#[derive(WhereDbModel)]` renders AND-composed predicates; Option fields are skipped when `None`.
- Operators via `#[operator(">")]`, `#[operator("<")]`, etc.; vectors render `IN (...)` unless empty (then skipped).
- `#[limit]` / `#[offset]` add LIMIT/OFFSET (Option types skip rendering).
- `IsNull`/`IsNotNull` helpers for null checks.
- `NoneWhereModel` to pass `None` where-parameters.
- `#[where_raw_model("field = ${val} ...")]` for custom where strings with placeholders.
- JSON support: derive `MyPostgresJsonModel` on a struct and use it in where models, or use `#[inside_json("path.to.leaf")]`/`#[db_column_name]` for targeted JSON paths; dynamic JSON supported via maps.
- `StaticLineWhereModel` for fixed where clauses.

```rust
// Basic
#[derive(WhereDbModel)]
pub struct GetInputParam {
    pub client_id: String,
    pub key: String,
}
// WHERE client_id = $1 AND key = $2

// Operators + Option skip
#[derive(WhereDbModel)]
pub struct WithOps {
    #[operator(">")]
    pub from_amount: i64,
    #[operator("<")]
    pub to_amount: Option<i64>,
}

// IN with Vec
#[derive(WhereDbModel)]
pub struct WithIn {
    #[operator(">")]
    pub from_amount: i64,
    #[operator("<")]
    pub to_amount: i64,
    pub status: Vec<i32>, // empty vec => omitted
}

// Limit/offset
#[derive(WhereDbModel)]
pub struct WithPaging {
    pub asset_id: String,
    #[limit]
    pub limit: Option<usize>,
    #[offset]
    pub offset: Option<usize>,
}

// Raw where
#[where_raw_model("Content=${field_1} AND Content2=${field_2} AND Content3 in ${field_3}")]
pub struct WhereRawModel {
    pub field_1: String,
    pub field_2: bool,
    pub field_3: Vec<i32>,
}

// JSON model matching
#[derive(Serialize, Deserialize, MyPostgresJsonModel)]
pub struct JsonTestField {
    pub key: String,
    pub value: i32,
}
#[derive(WhereDbModel)]
pub struct WhereWithJsonField {
    pub field_before: String,
    #[db_column_name("my_json_field")]
    pub json_field: JsonTestField,
    pub field_after: bool,
}
// field_before=$1 AND ("my_json_field"->>'key'=$2 AND "my_json_field"->>'value'=5) AND field_after=true

// Targeted JSON path
#[derive(Debug, WhereDbModel)]
pub struct WhereJsonField {
    #[inside_json("db_column_name.sub_field")]
    pub json_prop: String,
}
// "db_column_name"->>'sub_field'->>'json_prop'=$1

// Static line
let where_model = StaticLineWhereModel::new("NOT starts_with(table_name, '_')");
let rows = postgres.query_rows("information_schema.tables", Some(&where_model)).await?;
```

## Telemetry and logging
- Enable `with-telemetry` to integrate `my-telemetry` and `my-logger` for request spans/metrics.
- Debug SQL prints can be toggled via `DEBUG_SQL=true|1|<table>|<operation>`.

## Links
- Project wiki (source of this doc): https://github.com/MyJetTools/my-postgres/wiki
- Proc-macro wiki: https://github.com/MyJetTools/my-postgres-macros/wiki
- Attribute source: `my-postgres-macros/src/attributes`
