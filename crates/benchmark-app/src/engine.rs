use anyhow::{Context, Result};
use benchmark_core::{
    BenchmarkConfig, EngineKind, HematiteJournalMode, HematiteStorageConfig, OperationKind,
    SqliteStorageConfig,
};
use hematite::Hematite;
use hematite::query::JournalMode;
use rand::Rng;
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BenchRow {
    pub id: u64,
    pub category: String,
    pub score: i64,
    pub payload: String,
    pub updated_at: i64,
}

pub trait EngineAdapter: Send {
    fn engine_kind(&self) -> EngineKind;
    fn warnings(&self) -> &[String];
    fn prepare_dataset(&mut self, config: &BenchmarkConfig) -> Result<()>;
    fn point_read(&mut self, id: u64) -> Result<usize>;
    fn range_scan(&mut self, start_id: u64, limit: usize) -> Result<usize>;
    fn insert_row(&mut self, row: &BenchRow) -> Result<()>;
    fn update_row(&mut self, row: &BenchRow) -> Result<usize>;
    fn delete_row(&mut self, id: u64) -> Result<usize>;
    fn aggregate(&mut self) -> Result<usize>;
    fn flush(&mut self) -> Result<()>;
}

pub fn open_engine(config: &BenchmarkConfig, data_dir: &Path) -> Result<Box<dyn EngineAdapter>> {
    std::fs::create_dir_all(data_dir)?;
    let storage = config.resolved_storage();
    match config.engine {
        EngineKind::Sqlite => Ok(Box::new(SqliteAdapter::open(data_dir, storage.sqlite)?)),
        EngineKind::Hematite => Ok(Box::new(HematiteAdapter::open(data_dir, storage.hematite)?)),
    }
}

pub fn make_row(config: &BenchmarkConfig, id: u64) -> BenchRow {
    let payload = payload_for(id, config.scenario.payload_size_bytes);
    let category = format!("category-{}", id % config.scenario.category_count as u64);
    let score = ((id * 37) % 10_000) as i64;
    BenchRow {
        id,
        category,
        score,
        payload,
        updated_at: now_ms() as i64,
    }
}

pub fn choose_existing_id(max_id: u64) -> u64 {
    let mut rng = rand::rng();
    if max_id <= 1 {
        1
    } else {
        rng.random_range(1..=max_id)
    }
}

pub fn logical_bytes_for_operation(
    config: &BenchmarkConfig,
    op: OperationKind,
    rows: usize,
) -> (u64, u64) {
    let payload_bytes = config.scenario.payload_size_bytes as u64;
    match op {
        OperationKind::PointRead => (payload_bytes, 0),
        OperationKind::RangeScan | OperationKind::Aggregate => (payload_bytes * rows as u64, 0),
        OperationKind::Insert | OperationKind::Update => (0, payload_bytes),
        OperationKind::Delete => (0, 0),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn payload_for(id: u64, len: usize) -> String {
    let seed = format!("payload-{id:020}");
    seed.chars().cycle().take(len).collect()
}

struct SqliteAdapter {
    conn: Connection,
    path: PathBuf,
    warnings: Vec<String>,
}

impl SqliteAdapter {
    fn open(data_dir: &Path, settings: SqliteStorageConfig) -> Result<Self> {
        let path = data_dir.join("sqlite.db");
        let conn = Connection::open(&path)?;
        let warnings = apply_sqlite_settings(&conn, settings)
            .map(|warning| warning.into_iter().collect())
            .context("failed to configure sqlite settings")?;
        Ok(Self {
            conn,
            path,
            warnings,
        })
    }
}

impl EngineAdapter for SqliteAdapter {
    fn engine_kind(&self) -> EngineKind {
        EngineKind::Sqlite
    }

    fn warnings(&self) -> &[String] {
        &self.warnings
    }

    fn prepare_dataset(&mut self, config: &BenchmarkConfig) -> Result<()> {
        let schema = r#"
            PRAGMA temp_store = MEMORY;
            CREATE TABLE IF NOT EXISTS bench_records (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                score INTEGER NOT NULL,
                payload TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_bench_records_category ON bench_records(category);
        "#;
        self.conn.execute_batch(schema)?;

        // Seed in batches to keep memory usage bounded. A single transaction
        // over tens of thousands of rows would hold all dirty pages in memory
        // until commit, which can crash the machine on large initial_rows.
        const SEED_BATCH_SIZE: u64 = 500;
        let mut id = 1u64;
        while id <= config.scenario.initial_rows {
            let batch_end = (id + SEED_BATCH_SIZE - 1).min(config.scenario.initial_rows);
            let tx = self.conn.transaction()?;
            {
                let mut stmt = tx.prepare(
                    "INSERT INTO bench_records (id, category, score, payload, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                )?;
                for row_id in id..=batch_end {
                    let row = make_row(config, row_id);
                    stmt.execute(params![
                        row.id as i64,
                        row.category,
                        row.score,
                        row.payload,
                        row.updated_at
                    ])?;
                }
            }
            tx.commit()?;
            id = batch_end + 1;
        }
        Ok(())
    }

    fn point_read(&mut self, id: u64) -> Result<usize> {
        let mut stmt = self.conn.prepare(
            "SELECT id, category, score, payload, updated_at FROM bench_records WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id as i64])?;
        Ok(usize::from(rows.next()?.is_some()))
    }

    fn range_scan(&mut self, start_id: u64, limit: usize) -> Result<usize> {
        let mut stmt = self.conn.prepare(
            "SELECT id, category, score, payload, updated_at FROM bench_records WHERE id >= ?1 ORDER BY id LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![start_id as i64, limit as i64], |_| Ok(()))?;
        Ok(rows.count())
    }

    fn insert_row(&mut self, row: &BenchRow) -> Result<()> {
        self.conn.execute(
            "INSERT INTO bench_records (id, category, score, payload, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![row.id as i64, row.category, row.score, row.payload, row.updated_at],
        )?;
        Ok(())
    }

    fn update_row(&mut self, row: &BenchRow) -> Result<usize> {
        let affected = self.conn.execute(
            "UPDATE bench_records SET score = ?2, payload = ?3, updated_at = ?4 WHERE id = ?1",
            params![row.id as i64, row.score, row.payload, row.updated_at],
        )?;
        Ok(affected)
    }

    fn delete_row(&mut self, id: u64) -> Result<usize> {
        let affected = self.conn.execute(
            "DELETE FROM bench_records WHERE id = ?1",
            params![id as i64],
        )?;
        Ok(affected)
    }

    fn aggregate(&mut self) -> Result<usize> {
        let mut stmt = self.conn.prepare(
            "SELECT category, COUNT(*), SUM(score) FROM bench_records GROUP BY category",
        )?;
        let rows = stmt.query_map([], |_| Ok(()))?;
        Ok(rows.count())
    }

    fn flush(&mut self) -> Result<()> {
        self.conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        let _ = &self.path;
        Ok(())
    }
}

fn apply_sqlite_settings(
    conn: &Connection,
    settings: SqliteStorageConfig,
) -> Result<Option<String>> {
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.pragma_update(
        None,
        "journal_mode",
        settings.journal_mode.as_pragma_value(),
    )?;
    conn.pragma_update(None, "synchronous", settings.synchronous.as_pragma_value())?;
    Ok(None)
}

struct HematiteAdapter {
    db: Hematite,
    warnings: Vec<String>,
}

impl HematiteAdapter {
    fn open(data_dir: &Path, settings: HematiteStorageConfig) -> Result<Self> {
        let path = data_dir.join("hematite.db");
        let path_string = path.to_string_lossy().to_string();
        let mut db = Hematite::new(&path_string)?;
        let warnings = apply_hematite_settings(&mut db, settings)?;
        Ok(Self { db, warnings })
    }
}

impl EngineAdapter for HematiteAdapter {
    fn engine_kind(&self) -> EngineKind {
        EngineKind::Hematite
    }

    fn warnings(&self) -> &[String] {
        &self.warnings
    }

    fn prepare_dataset(&mut self, config: &BenchmarkConfig) -> Result<()> {
        self.db.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS bench_records (
                id INT PRIMARY KEY,
                category TEXT NOT NULL,
                score INT NOT NULL,
                payload TEXT NOT NULL,
                updated_at INT64 NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_bench_records_category ON bench_records(category);
        "#,
        )?;

        // Seed in batches to keep memory usage bounded. A single transaction
        // over tens of thousands of rows forces Hematite to hold all dirty WAL
        // frames in memory until commit, which can OOM the machine.
        const SEED_BATCH_SIZE: u64 = 500;
        let mut id = 1u64;
        while id <= config.scenario.initial_rows {
            let batch_end = (id + SEED_BATCH_SIZE - 1).min(config.scenario.initial_rows);
            let mut tx = self.db.transaction()?;
            for row_id in id..=batch_end {
                let row = make_row(config, row_id);
                tx.execute(&format!(
                    "INSERT INTO bench_records (id, category, score, payload, updated_at) VALUES ({}, {}, {}, {}, {});",
                    row.id,
                    sql_string_literal(&row.category),
                    row.score,
                    sql_string_literal(&row.payload),
                    row.updated_at
                ))?;
            }
            tx.commit()?;
            id = batch_end + 1;
        }
        Ok(())
    }

    fn point_read(&mut self, id: u64) -> Result<usize> {
        let rows = self.db.query(&format!(
            "SELECT id, category, score, payload, updated_at FROM bench_records WHERE id = {};",
            id
        ))?;
        Ok(rows.len())
    }

    fn range_scan(&mut self, start_id: u64, limit: usize) -> Result<usize> {
        let rows = self.db.query(&format!(
            "SELECT id, category, score, payload, updated_at FROM bench_records WHERE id >= {} ORDER BY id LIMIT {};",
            start_id, limit
        ))?;
        Ok(rows.len())
    }

    fn insert_row(&mut self, row: &BenchRow) -> Result<()> {
        self.db.execute(&format!(
            "INSERT INTO bench_records (id, category, score, payload, updated_at) VALUES ({}, {}, {}, {}, {});",
            row.id,
            sql_string_literal(&row.category),
            row.score,
            sql_string_literal(&row.payload),
            row.updated_at
        ))?;
        Ok(())
    }

    fn update_row(&mut self, row: &BenchRow) -> Result<usize> {
        let result = self.db.execute(&format!(
            "UPDATE bench_records SET score = {}, payload = {}, updated_at = {} WHERE id = {};",
            row.score,
            sql_string_literal(&row.payload),
            row.updated_at,
            row.id
        ))?;
        Ok(result.affected_rows)
    }

    fn delete_row(&mut self, id: u64) -> Result<usize> {
        let result = self
            .db
            .execute(&format!("DELETE FROM bench_records WHERE id = {};", id))?;
        Ok(result.affected_rows)
    }

    fn aggregate(&mut self) -> Result<usize> {
        let rows = self
            .db
            .query("SELECT category, COUNT(*), SUM(score) FROM bench_records GROUP BY category;")?;
        Ok(rows.len())
    }

    fn flush(&mut self) -> Result<()> {
        self.db.checkpoint_wal().ok();
        Ok(())
    }
}

fn apply_hematite_settings(
    db: &mut Hematite,
    settings: HematiteStorageConfig,
) -> Result<Vec<String>> {
    let mode = match settings.journal_mode {
        HematiteJournalMode::Rollback => JournalMode::Rollback,
        HematiteJournalMode::Wal => JournalMode::Wal,
    };
    db.set_journal_mode(mode)?;
    Ok(Vec::new())
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub fn execute_operation(
    adapter: &mut dyn EngineAdapter,
    config: &BenchmarkConfig,
    op: OperationKind,
    next_id: u64,
) -> Result<usize> {
    match op {
        OperationKind::PointRead => {
            adapter.point_read(choose_existing_id(next_id.saturating_sub(1)))
        }
        OperationKind::RangeScan => adapter.range_scan(
            choose_existing_id(next_id.saturating_sub(1)),
            config.scenario.range_scan_size,
        ),
        OperationKind::Insert => {
            let row = make_row(config, next_id);
            adapter.insert_row(&row)?;
            Ok(1)
        }
        OperationKind::Update => {
            let id = choose_existing_id(next_id.saturating_sub(1));
            let mut row = make_row(config, id);
            row.score += 1;
            adapter.update_row(&row)
        }
        OperationKind::Delete => adapter.delete_row(choose_existing_id(next_id.saturating_sub(1))),
        OperationKind::Aggregate => adapter.aggregate(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use benchmark_core::{
        BenchmarkConfig, EngineKind, LoadConfig, OperationMix, ScenarioConfig, StorageConfig,
    };
    use tempfile::tempdir;

    fn sqlite_config() -> BenchmarkConfig {
        BenchmarkConfig {
            run_name: "sqlite".to_string(),
            engine: EngineKind::Sqlite,
            scenario: ScenarioConfig {
                initial_rows: 50,
                payload_size_bytes: 64,
                category_count: 4,
                range_scan_size: 8,
            },
            load: LoadConfig {
                concurrency: 2,
                batch_size: 1,
                duration_secs: 2,
                sample_interval_ms: 200,
                mix: OperationMix::default(),
            },
            ramp_schedule: vec![],
            storage: StorageConfig::default(),
            durability: None,
            profiling: None,
        }
    }

    /// The operations the benchmark issues, with the answers both engines owe.
    ///
    /// Every adapter is driven through this, so a SQL feature one engine has
    /// dropped shows up as a failing test rather than as a silently cheaper
    /// number in a comparison. Counts are exact: an engine that quietly returns
    /// nothing for a scan or an aggregate would otherwise look fast.
    fn assert_workload_contract(
        adapter: &mut dyn EngineAdapter,
        config: &BenchmarkConfig,
    ) -> Result<()> {
        // prepare_dataset seeded ids 1..=initial_rows.
        let seeded = config.scenario.initial_rows;
        let categories = config.scenario.category_count as usize;

        assert_eq!(adapter.point_read(1)?, 1, "first seeded row must read back");
        assert_eq!(
            adapter.point_read(seeded)?,
            1,
            "last seeded row must read back"
        );
        assert_eq!(
            adapter.point_read(seeded + 10_000)?,
            0,
            "a missing id must read back empty"
        );

        assert_eq!(adapter.range_scan(1, 5)?, 5, "a scan must honour its LIMIT");
        assert_eq!(
            adapter.range_scan(seeded - 2, 5)?,
            3,
            "a scan must stop at the last row instead of wrapping"
        );

        assert_eq!(
            adapter.aggregate()?,
            categories,
            "GROUP BY must return one row per category"
        );

        adapter.insert_row(&make_row(config, seeded + 1))?;
        assert_eq!(
            adapter.point_read(seeded + 1)?,
            1,
            "an inserted row must be visible"
        );

        assert_eq!(
            adapter.update_row(&make_row(config, 10))?,
            1,
            "updating an existing row must affect exactly it"
        );
        assert_eq!(
            adapter.update_row(&make_row(config, seeded + 10_000))?,
            0,
            "updating a missing row must affect nothing"
        );

        assert_eq!(adapter.delete_row(5)?, 1, "deleting a row must affect it");
        assert_eq!(
            adapter.point_read(5)?,
            0,
            "a deleted row must no longer read back"
        );
        assert_eq!(
            adapter.delete_row(5)?,
            0,
            "deleting an already deleted row must affect nothing"
        );

        assert_eq!(
            adapter.aggregate()?,
            categories,
            "GROUP BY must still cover every category after the writes"
        );

        adapter.flush()?;
        Ok(())
    }

    #[test]
    fn sqlite_adapter_supports_full_workload() -> Result<()> {
        let dir = tempdir()?;
        let mut adapter = SqliteAdapter::open(dir.path(), Default::default())?;
        let config = sqlite_config();
        adapter.prepare_dataset(&config)?;
        assert_workload_contract(&mut adapter, &config)
    }

    #[test]
    fn hematite_adapter_supports_full_workload() -> Result<()> {
        let dir = tempdir()?;
        let mut adapter = HematiteAdapter::open(dir.path(), Default::default())?;
        let config = BenchmarkConfig {
            engine: EngineKind::Hematite,
            ..sqlite_config()
        };
        adapter.prepare_dataset(&config)?;
        assert_workload_contract(&mut adapter, &config)
    }

    /// The schema statements must be re-runnable, since a worker reopens a data
    /// directory a previous run already created.
    #[test]
    fn hematite_schema_is_idempotent() -> Result<()> {
        let dir = tempdir()?;
        let mut adapter = HematiteAdapter::open(dir.path(), Default::default())?;
        let config = BenchmarkConfig {
            engine: EngineKind::Hematite,
            scenario: ScenarioConfig {
                initial_rows: 0,
                ..sqlite_config().scenario
            },
            ..sqlite_config()
        };
        adapter.prepare_dataset(&config)?;
        adapter.prepare_dataset(&config)?;
        Ok(())
    }
}
