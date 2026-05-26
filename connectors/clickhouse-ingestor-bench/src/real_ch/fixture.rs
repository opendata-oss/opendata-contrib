//! Real-ClickHouse fixture for the bench harness.
//!
//! Provides `RealClickHouseFixture::setup_testcontainers()` which
//! spins up a fresh `clickhouse-server` container, creates the OTLP
//! logs schema via the production `logs_table_ddl(...)`, and exposes
//! the helper queries the bench uses to compute correctness
//! (count_visible / count_pre_dedupe_duplicates /
//! count_post_dedupe_duplicates). `Drop` shuts the container down.
//!
//! An `ExternalCompose` mode + a docker-compose YAML for
//! operator-managed runs is future work; this ships only the
//! `Testcontainers` mode so the `cargo test` smoke works end-to-end.
//! Operator soak runs are a follow-up alongside the matrix runner
//! once the perf-test environment shape is settled.

use std::collections::BTreeMap;
use std::time::Duration;

use clickhouse_ingestor::writer::{ClickHouseWriter, WriterConfig};
use opendata_ingest_clickhouse::adapter::logs::{LogsAdapterConfig, logs_table_ddl};
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::clickhouse::ClickHouse as ClickHouseImage;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FixtureError {
    #[error("testcontainers: {0}")]
    Testcontainers(String),
    #[error("ClickHouse writer: {0}")]
    Writer(String),
    #[error("ClickHouse query: {0}")]
    Query(String),
}

/// A live ClickHouse server brought up via `testcontainers-rs`.
/// `Drop` shuts the container down. The fixture owns the
/// `ClickHouseWriter` used by both `ensure_table()` /
/// `drop_table()` and any post-run correctness queries.
pub struct RealClickHouseFixture {
    _container: ContainerAsync<ClickHouseImage>,
    pub endpoint: String,
    pub database: String,
    pub table: String,
    pub adapter_config: LogsAdapterConfig,
    pub writer: ClickHouseWriter,
}

impl RealClickHouseFixture {
    /// Spin up a fresh ClickHouse container, create the database +
    /// table for the OTLP-logs schema, and return a fixture handle
    /// the bench drives the runtime against.
    pub async fn setup_testcontainers(
        database: impl Into<String>,
        table: impl Into<String>,
        adapter_config: LogsAdapterConfig,
    ) -> Result<Self, FixtureError> {
        let container = ClickHouseImage::default()
            .start()
            .await
            .map_err(|e| FixtureError::Testcontainers(e.to_string()))?;
        let port = container
            .get_host_port_ipv4(8123)
            .await
            .map_err(|e| FixtureError::Testcontainers(e.to_string()))?;
        let endpoint = format!("http://127.0.0.1:{port}");

        let writer = ClickHouseWriter::new(WriterConfig {
            endpoint: endpoint.clone(),
            user: "default".into(),
            password: String::new(),
            request_timeout: Duration::from_secs(30),
            max_attempts: 4,
            initial_backoff: Duration::from_millis(100),
            ..Default::default()
        });

        let database = database.into();
        let table = table.into();
        let adapter_config = LogsAdapterConfig {
            database: database.clone(),
            table: table.clone(),
            ..adapter_config
        };

        writer
            .execute_statement(&format!("CREATE DATABASE IF NOT EXISTS {database}"))
            .await
            .map_err(|e| FixtureError::Writer(e.to_string()))?;
        writer
            .execute_statement(&logs_table_ddl(&adapter_config))
            .await
            .map_err(|e| FixtureError::Writer(e.to_string()))?;

        Ok(Self {
            _container: container,
            endpoint,
            database,
            table,
            adapter_config,
            writer,
        })
    }

    /// `SELECT count() FROM <db>.<table> FINAL` — visible row count
    /// after `ReplacingMergeTree` dedupe.
    pub async fn count_visible(&self) -> Result<u64, FixtureError> {
        self.count_scalar(&format!(
            "SELECT count() FROM {}.{} FINAL",
            self.database, self.table
        ))
        .await
    }

    /// `SELECT count() FROM <db>.<table>` — raw row count pre-FINAL.
    pub async fn count_raw(&self) -> Result<u64, FixtureError> {
        self.count_scalar(&format!(
            "SELECT count() FROM {}.{}",
            self.database, self.table
        ))
        .await
    }

    /// Returns `count() - count(DISTINCT _odb_*)` (pre-FINAL). A
    /// non-zero value means the runtime + sink produced duplicate
    /// source-coordinate rows that `ReplacingMergeTree` must
    /// reconcile; correctness still holds if `count_visible == 0`
    /// extra rows are observable after `FINAL`.
    pub async fn count_pre_dedupe_duplicates(&self) -> Result<u64, FixtureError> {
        self.count_scalar(&format!(
            "SELECT count() - count(DISTINCT (_odb_sequence, _odb_entry_index, _odb_record_index)) \
             FROM {}.{}",
            self.database, self.table
        ))
        .await
    }

    /// Same as `count_pre_dedupe_duplicates`, applied to the
    /// post-FINAL deduped view. Must be `0` for the correctness
    /// gate to pass.
    pub async fn count_post_dedupe_duplicates(&self) -> Result<u64, FixtureError> {
        self.count_scalar(&format!(
            "SELECT count() - count(DISTINCT (_odb_sequence, _odb_entry_index, _odb_record_index)) \
             FROM {}.{} FINAL",
            self.database, self.table
        ))
        .await
    }

    /// Order-independent fingerprint of all post-FINAL rows in
    /// `<self.database>.<table>`. Wraps each row as
    /// `toString(tuple(* EXCEPT _odb_clickhouse_inserted_at))` then
    /// sums `cityHash64` across rows. Sum is associative +
    /// commutative, so the result doesn't depend on storage
    /// iteration order — two tables receiving the same logical rows
    /// via different wire formats produce the same fingerprint.
    ///
    /// `_odb_clickhouse_inserted_at` is excluded because it carries a
    /// server-side `DEFAULT now64(9)` (set per row at INSERT
    /// materialization). Two separate INSERTs against sibling tables
    /// — the format-equivalence test does exactly this — fire
    /// `now64(9)` at slightly different wall-clock instants and
    /// therefore can never produce identical values in that column.
    /// The writer's projection (the [`COLUMNS`] const in
    /// `opendata_ingest_clickhouse::adapter::logs`) already omits the
    /// column for the same reason; this query stays consistent with
    /// what the writer actually controls.
    ///
    /// [`COLUMNS`]: opendata_ingest_clickhouse::adapter::logs::COLUMNS
    pub async fn hash_rows_in(&self, table: &str) -> Result<u64, FixtureError> {
        self.count_scalar(&format!(
            "SELECT sum(cityHash64(toString(tuple(* EXCEPT _odb_clickhouse_inserted_at)))) \
             FROM {}.{} FINAL",
            self.database, table
        ))
        .await
    }

    /// Per-column equivalent of `hash_rows_in`. Returns a map from
    /// column name to its order-independent fingerprint
    /// (`sum(cityHash64(toString(col)))` post-FINAL). Lets the
    /// `format_swap_produces_identical_column_hashes` scenario
    /// localize a mismatch to a single column when full-row hashes
    /// disagree.
    pub async fn hash_columns_in(
        &self,
        table: &str,
        columns: &[&str],
    ) -> Result<BTreeMap<String, u64>, FixtureError> {
        let mut out = BTreeMap::new();
        for column in columns {
            let h = self
                .count_scalar(&format!(
                    "SELECT sum(cityHash64(toString({col}))) FROM {db}.{table} FINAL",
                    col = column,
                    db = self.database,
                    table = table,
                ))
                .await?;
            out.insert((*column).to_string(), h);
        }
        Ok(out)
    }

    async fn count_scalar(&self, sql: &str) -> Result<u64, FixtureError> {
        let raw = self
            .writer
            .execute_statement(sql)
            .await
            .map_err(|e| FixtureError::Query(e.to_string()))?;
        raw.trim()
            .parse::<u64>()
            .map_err(|e| FixtureError::Query(format!("parse {sql:?} -> {raw:?}: {e}")))
    }
}
