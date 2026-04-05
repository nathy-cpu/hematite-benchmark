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
