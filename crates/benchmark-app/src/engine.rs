use anyhow::{Context, Result};
use benchmark_core::{BenchmarkConfig, DurabilityPreset, EngineKind, OperationKind};
use hematite::Hematite;
use hematite::query::JournalMode;
use rand::Rng;
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};

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
    fn flush(&mut self) -> Result<()>;
}

pub fn open_engine(
    engine: EngineKind,
    data_dir: &Path,
    durability: DurabilityPreset,
) -> Result<Box<dyn EngineAdapter>> {
    std::fs::create_dir_all(data_dir)?;
    match engine {
        EngineKind::Sqlite => Ok(Box::new(SqliteAdapter::open(data_dir, durability)?)),
        EngineKind::Hematite => Ok(Box::new(HematiteAdapter::open(data_dir, durability)?)),
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
        OperationKind::RangeScan => (payload_bytes * rows as u64, 0),
        OperationKind::Insert | OperationKind::Update => (0, payload_bytes),
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
    fn open(data_dir: &Path, durability: DurabilityPreset) -> Result<Self> {
        let path = data_dir.join("sqlite.db");
        let conn = Connection::open(&path)?;
        let warnings = apply_sqlite_durability(&conn, durability)
            .map(|warning| warning.into_iter().collect())
            .context("failed to configure sqlite durability")?;
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

        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO bench_records (id, category, score, payload, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for id in 1..=config.scenario.initial_rows {
                let row = make_row(config, id);
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

    fn flush(&mut self) -> Result<()> {
        self.conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        let _ = &self.path;
        Ok(())
    }
}

fn apply_sqlite_durability(
    conn: &Connection,
    durability: DurabilityPreset,
) -> Result<Option<String>> {
    let (journal_mode, synchronous) = match durability {
        DurabilityPreset::Safe => ("WAL", "FULL"),
        DurabilityPreset::Balanced => ("WAL", "NORMAL"),
        DurabilityPreset::Fast => ("MEMORY", "OFF"),
    };
    conn.pragma_update(None, "journal_mode", journal_mode)?;
    conn.pragma_update(None, "synchronous", synchronous)?;
    Ok(None)
}

struct HematiteAdapter {
    db: Hematite,
    warnings: Vec<String>,
}

impl HematiteAdapter {
    fn open(data_dir: &Path, durability: DurabilityPreset) -> Result<Self> {
        let path = data_dir.join("hematite.db");
        let path_string = path.to_string_lossy().to_string();
        let mut db = Hematite::new(&path_string)?;
        let warnings = apply_hematite_durability(&mut db, durability)?;
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

        for id in 1..=config.scenario.initial_rows {
            let row = make_row(config, id);
            self.db.execute(&format!(
                "INSERT INTO bench_records (id, category, score, payload, updated_at) VALUES ({}, {}, {}, {}, {});",
                row.id,
                sql_string_literal(&row.category),
                row.score,
                sql_string_literal(&row.payload),
                row.updated_at
            ))?;
        }
        Ok(())
    }

    fn point_read(&mut self, id: u64) -> Result<usize> {
        let rows = self.db.query(&format!(
            "SELECT id, category, score, payload, updated_at FROM bench_records WHERE id = {};",
            id
        ))?;
        Ok(rows.rows.len())
    }

    fn range_scan(&mut self, start_id: u64, limit: usize) -> Result<usize> {
        let rows = self.db.query(&format!(
            "SELECT id, category, score, payload, updated_at FROM bench_records WHERE id >= {} ORDER BY id LIMIT {};",
            start_id, limit
        ))?;
        Ok(rows.rows.len())
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

    fn flush(&mut self) -> Result<()> {
        self.db.checkpoint_wal().ok();
        Ok(())
    }
}

fn apply_hematite_durability(
    db: &mut Hematite,
    durability: DurabilityPreset,
) -> Result<Vec<String>> {
    let mut warnings = Vec::new();
    let mode = match durability {
        DurabilityPreset::Safe => {
            warnings.push(
                "Hematite only exposes WAL vs rollback journal mode, so the safe preset uses WAL without a separate synchronous knob.".to_string(),
            );
            JournalMode::Wal
        }
        DurabilityPreset::Balanced => JournalMode::Wal,
        DurabilityPreset::Fast => {
            warnings.push(
                "Hematite does not expose SQLite-style synchronous=OFF; the fast preset falls back to rollback journaling.".to_string(),
            );
            JournalMode::Rollback
        }
    };
    db.set_journal_mode(mode)?;
    Ok(warnings)
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use benchmark_core::{BenchmarkConfig, EngineKind, LoadConfig, OperationMix, ScenarioConfig};
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
            durability: DurabilityPreset::Balanced,
        }
    }

    #[test]
    fn sqlite_adapter_supports_full_workload() -> Result<()> {
        let dir = tempdir()?;
        let mut adapter = SqliteAdapter::open(dir.path(), DurabilityPreset::Balanced)?;
        let config = sqlite_config();
        adapter.prepare_dataset(&config)?;

        assert_eq!(adapter.point_read(1)?, 1);
        assert!(adapter.range_scan(1, 5)? >= 1);
        adapter.insert_row(&make_row(&config, 51))?;
        assert_eq!(adapter.update_row(&make_row(&config, 10))?, 1);
        Ok(())
    }

    #[test]
    fn hematite_adapter_supports_documented_sql_surface() -> Result<()> {
        let dir = tempdir()?;
        let mut adapter = HematiteAdapter::open(dir.path(), DurabilityPreset::Balanced)?;
        let config = BenchmarkConfig {
            engine: EngineKind::Hematite,
            ..sqlite_config()
        };
        adapter.prepare_dataset(&config)?;

        assert_eq!(adapter.point_read(1)?, 1);
        assert!(adapter.range_scan(1, 5)? >= 1);
        adapter.insert_row(&make_row(&config, 51))?;
        assert_eq!(adapter.update_row(&make_row(&config, 10))?, 1);
        Ok(())
    }
}
