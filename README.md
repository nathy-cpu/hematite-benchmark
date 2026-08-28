# Hematite Benchmark Dashboard

Local benchmarking dashboard for comparing `hematite-db` and SQLite under the same workload.

## What It Does

- Runs each engine in its own worker process
- Streams live benchmark samples to a browser UI
- Supports pre-run setup, scheduled ramps, and mid-run load updates
- Persists runs to `./runs` for later overlays

## Start The Dashboard

```bash
cargo run -p benchmark-app --bin benchmark-server
```

Open `http://127.0.0.1:3000`.

## Notes

- Workers are launched as subprocesses and report JSON events back to the server.
- Exact live disk-I/O numbers come from `/proc/self/io` on Linux.
- Saved runs include `config.json`, `metrics.jsonl`, and `summary.json`.

## The Workload

Both engines run the same schema and the same six operations, so the comparison
turns on storage and execution rather than on one engine being handed an easier
query.

```sql
CREATE TABLE bench_records (
    id          INTEGER PRIMARY KEY,   -- INT PRIMARY KEY on Hematite
    category    TEXT    NOT NULL,
    score       INTEGER NOT NULL,      -- INT
    payload     TEXT    NOT NULL,
    updated_at  INTEGER NOT NULL       -- INT64
);
CREATE INDEX idx_bench_records_category ON bench_records(category);
```

| Operation    | SQL                                                        |
| ------------ | ---------------------------------------------------------- |
| Point read   | `SELECT ... WHERE id = ?`                                   |
| Range scan   | `SELECT ... WHERE id >= ? ORDER BY id LIMIT ?`              |
| Insert       | `INSERT INTO bench_records (...) VALUES (...)`              |
| Update       | `UPDATE bench_records SET ... WHERE id = ?`                 |
| Delete       | `DELETE FROM bench_records WHERE id = ?`                    |
| Aggregate    | `SELECT category, COUNT(*), SUM(score) ... GROUP BY category` |

A single integer primary key is the row id in both engines, so neither pays for
a second B-tree to resolve `WHERE id = ?`.

### Keeping It Fair Against A Moving SQL Dialect

Hematite's dialect is deliberately narrow, and it gets narrower — see
`docs/sql-dialect.md` in the Hematite repo. The risk for a benchmark is not that
a dropped feature breaks the build; it is that one engine quietly returns fewer
rows, or nothing at all, and so looks faster.

`assert_workload_contract` in [engine.rs](crates/benchmark-app/src/engine.rs)
guards against that. It drives *every* adapter through the full operation set and
asserts exact counts — rows scanned, groups aggregated, rows affected by an
update or a delete, and that a missing row reads back empty. An engine that loses
a feature fails the test instead of posting a better number.

Run it after any Hematite upgrade:

```bash
cargo test -p benchmark-app engine::
```
