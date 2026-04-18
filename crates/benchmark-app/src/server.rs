use anyhow::{Context, Result};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use benchmark_core::{
    AppliedControlEvent, ArtifactPaths, BenchmarkConfig, ControlMessage, MetricSample,
    RunAggregate, RunDetail, RunListItem, RunLogEntry, RunLogLevel, RunLogSource, RunStatus,
    RunSummary, WorkerEvent,
};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path as FsPath, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerOptions {
    pub worker_perf: bool,
    pub worker_perf_generate_flamegraph: bool,
    pub worker_perf_freq_hz: Option<u32>,
    pub worker_perf_output: Option<String>,
    pub worker_strace: bool,
    pub worker_strace_output: Option<String>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            worker_perf: false,
            worker_perf_generate_flamegraph: true,
            worker_perf_freq_hz: None,
            worker_perf_output: None,
            worker_strace: false,
            worker_strace_output: None,
        }
    }
}
const INDEX_HTML: &str = include_str!("../static/index.html");
const APP_JS: &str = include_str!("../static/app.js");
const STYLES_CSS: &str = include_str!("../static/styles.css");
const MAX_RECENT_LOGS: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerVerbosity {
    Quiet,
    Normal,
    Verbose,
    Trace,
}

impl ServerVerbosity {
    pub fn default_filter(self) -> &'static str {
        match self {
            Self::Quiet => "warn",
            Self::Normal => "info,benchmark_app=info",
            Self::Verbose => "info,benchmark_app=debug",
            Self::Trace => "info,benchmark_app=trace",
        }
    }
}

impl Default for ServerVerbosity {
    fn default() -> Self {
        Self::Normal
    }
}

#[derive(Clone)]
struct AppState {
    runs_dir: PathBuf,
    runs: Arc<RwLock<HashMap<String, StoredRun>>>,
    options: Arc<RwLock<ServerOptions>>,
}

struct StoredRun {
    run_id: String,
    config: BenchmarkConfig,
    effective_config: BenchmarkConfig,
    status: RunStatus,
    started_at_ms: u64,
    ended_at_ms: Option<u64>,
    warnings: Vec<String>,
    error_messages: Vec<String>,
    control_events: Vec<AppliedControlEvent>,
    latest_sample: Option<MetricSample>,
    recent_logs: Vec<RunLogEntry>,
    log_count: usize,
    summary: Option<RunSummary>,
    run_dir: PathBuf,
    active: Option<ActiveRun>,
}

struct ActiveRun {
    stdin: Arc<Mutex<ChildStdin>>,
    tx: broadcast::Sender<WorkerEvent>,
}

struct SpawnedWorker {
    child: Child,
    launcher: &'static str,
}

pub async fn run_server() -> Result<()> {
    run_server_with_verbosity(ServerVerbosity::default(), ServerOptions::default()).await
}

pub async fn run_server_with_verbosity(
    verbosity: ServerVerbosity,
    options: ServerOptions,
) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| verbosity.default_filter().into()),
        )
        .init();

    let runs_dir = std::env::current_dir()?.join("runs");
    fs::create_dir_all(&runs_dir).await?;
    let state = AppState {
        runs_dir: runs_dir.clone(),
        runs: Arc::new(RwLock::new(load_existing_runs(&runs_dir).await?)),
        options: Arc::new(RwLock::new(options)),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/setup", get(index))
        .route("/dashboard", get(index))
        .route("/history", get(index))
        .route("/app.js", get(app_js))
        .route("/styles.css", get(styles_css))
        .route("/api/runs", get(list_runs).post(start_run))
        .route("/api/options", get(get_options).post(set_options))
        .route("/api/runs/{run_id}", get(get_run))
        .route("/api/runs/{run_id}/control", post(control_run))
        .route("/api/runs/{run_id}/stream", get(stream_run))
        .route("/api/runs/{run_id}/artifact", get(get_artifact))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 3000)).await?;
    info!("benchmark dashboard listening on http://127.0.0.1:3000");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn app_js() -> Response {
    (
        [(axum::http::header::CONTENT_TYPE, "application/javascript")],
        APP_JS,
    )
        .into_response()
}

async fn styles_css() -> Response {
    ([(axum::http::header::CONTENT_TYPE, "text/css")], STYLES_CSS).into_response()
}

async fn list_runs(State(state): State<AppState>) -> Json<Vec<RunListItem>> {
    let runs = state.runs.read().await;
    let mut items = runs
        .values()
        .map(|run| RunListItem {
            run_id: run.run_id.clone(),
            engine: run.config.engine,
            run_name: run.config.run_name.clone(),
            status: run.status.clone(),
            started_at_ms: run.started_at_ms,
            ended_at_ms: run.ended_at_ms,
            latest_sample: run.latest_sample.clone(),
            summary: run.summary.clone(),
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| right.started_at_ms.cmp(&left.started_at_ms));
    Json(items)
}

async fn get_options(State(state): State<AppState>) -> Json<ServerOptions> {
    let opts = state.options.read().await.clone();
    Json(opts)
}

async fn set_options(
    State(state): State<AppState>,
    Json(new_opts): Json<ServerOptions>,
) -> Result<StatusCode, ApiError> {
    *state.options.write().await = new_opts;
    Ok(StatusCode::ACCEPTED)
}

async fn get_artifact(
    Path(run_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<Response, ApiError> {
    let name = params
        .get("name")
        .ok_or_else(|| ApiError::bad_request("missing name parameter"))?;
    if name.contains("..") || name.contains('/') || name.contains('\\') {
        return Err(ApiError::bad_request("invalid artifact name"));
    }
    let run_dir = {
        let runs = state.runs.read().await;
        let run = runs
            .get(&run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        run.run_dir.clone()
    };
    let path = run_dir.join(name);
    if !path.exists() {
        return Err(ApiError::not_found("artifact not found"));
    }
    let bytes = fs::read(&path).await?;
    let content_type = match path.extension().and_then(|s| s.to_str()) {
        Some("svg") => "image/svg+xml",
        Some("json") => "application/json",
        Some("txt") | Some("log") | Some("out") => "text/plain",
        Some("perf") | Some("data") => "application/octet-stream",
        Some("gz") => "application/gzip",
        _ => "application/octet-stream",
    };
    Ok(([(axum::http::header::CONTENT_TYPE, content_type)], bytes).into_response())
}

async fn get_run(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<RunDetail>, ApiError> {
    let runs = state.runs.read().await;
    let run = runs
        .get(&run_id)
        .ok_or_else(|| ApiError::not_found("run not found"))?;
    let samples = load_samples(&run.run_dir.join("metrics.jsonl")).await?;
    let logs = load_logs(&run.run_dir.join("logs.jsonl")).await?;
    Ok(Json(RunDetail {
        run_id: run.run_id.clone(),
        status: run.status.clone(),
        config: run.config.clone(),
        effective_config: run.effective_config.clone(),
        warnings: run.warnings.clone(),
        error_messages: run.error_messages.clone(),
        control_events: run.control_events.clone(),
        samples,
        logs,
        summary: run.summary.clone(),
    }))
}

async fn start_run(
    State(state): State<AppState>,
    Json(config): Json<BenchmarkConfig>,
) -> Result<Json<RunListItem>, ApiError> {
    config.validate().map_err(ApiError::bad_request)?;

    let run_id = Uuid::new_v4().to_string();
    let run_dir = state.runs_dir.join(&run_id);
    let config_path = run_dir.join("config.json");
    let metrics_path = run_dir.join("metrics.jsonl");
    let control_events_path = run_dir.join("control-events.jsonl");
    let logs_path = run_dir.join("logs.jsonl");
    fs::create_dir_all(run_dir.join("data")).await?;
    fs::write(&config_path, serde_json::to_vec_pretty(&config)?).await?;
    fs::write(&metrics_path, &[]).await?;
    fs::write(&control_events_path, &[]).await?;
    fs::write(&logs_path, &[]).await?;

    let started_at_ms = now_ms();
    let (tx, _) = broadcast::channel(200);
    {
        let mut runs = state.runs.write().await;
        runs.insert(
            run_id.clone(),
            StoredRun {
                run_id: run_id.clone(),
                config: config.clone(),
                effective_config: config.clone(),
                status: RunStatus::Pending,
                started_at_ms,
                ended_at_ms: None,
                warnings: Vec::new(),
                error_messages: Vec::new(),
                control_events: Vec::new(),
                latest_sample: None,
                recent_logs: Vec::new(),
                log_count: 0,
                summary: None,
                run_dir: run_dir.clone(),
                active: None,
            },
        );
    }

    record_run_log(
        &state,
        &run_id,
        RunLogEntry {
            timestamp_ms: now_ms(),
            level: RunLogLevel::Info,
            source: RunLogSource::Server,
            message: format!(
                "Run queued: engine={}, rows={}, concurrency={}, duration={}s",
                config.engine.as_str(),
                config.scenario.initial_rows,
                config.load.concurrency,
                config.load.duration_secs
            ),
        },
    )
    .await?;

    let opts = state.options.read().await.clone();
    let spawned = match spawn_worker_process(&run_id, &run_dir, &config_path, opts, &config).await {
        Ok(child) => child,
        Err(error) => {
            state.runs.write().await.remove(&run_id);
            return Err(error);
        }
    };
    record_run_log(
        &state,
        &run_id,
        RunLogEntry {
            timestamp_ms: now_ms(),
            level: RunLogLevel::Info,
            source: RunLogSource::Server,
            message: format!(
                "Launching worker via {}{}",
                spawned.launcher,
                spawned
                    .child
                    .id()
                    .map(|pid| format!(" (pid={pid})"))
                    .unwrap_or_default()
            ),
        },
    )
    .await?;
    let active = match register_worker(
        state.clone(),
        run_id.clone(),
        run_dir.clone(),
        spawned.child,
        tx.clone(),
    )
    .await
    {
        Ok(active) => active,
        Err(error) => {
            state.runs.write().await.remove(&run_id);
            return Err(error);
        }
    };

    {
        let mut runs = state.runs.write().await;
        if let Some(run) = runs.get_mut(&run_id) {
            run.active = Some(active);
        }
    }

    Ok(Json(RunListItem {
        run_id,
        engine: config.engine,
        run_name: config.run_name,
        status: RunStatus::Pending,
        started_at_ms,
        ended_at_ms: None,
        latest_sample: None,
        summary: None,
    }))
}

async fn control_run(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
    Json(message): Json<ControlMessage>,
) -> Result<StatusCode, ApiError> {
    let stdin = {
        let runs = state.runs.read().await;
        let run = runs
            .get(&run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        let active = run
            .active
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("run is not active"))?;
        active.stdin.clone()
    };

    let mut stdin = stdin.lock().await;
    let payload = serde_json::to_vec(&message)?;
    stdin.write_all(&payload).await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await?;
    Ok(StatusCode::ACCEPTED)
}

async fn stream_run(
    Path(run_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>>, ApiError> {
    let rx = {
        let runs = state.runs.read().await;
        let run = runs
            .get(&run_id)
            .ok_or_else(|| ApiError::not_found("run not found"))?;
        let active = run
            .active
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("run is not active"))?;
        active.tx.subscribe()
    };

    let stream = BroadcastStream::new(rx).filter_map(|event| async move {
        match event {
            Ok(event) => Some(Ok(Event::default().json_data(event).expect("serializable"))),
            Err(_) => None,
        }
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn register_worker(
    state: AppState,
    run_id: String,
    run_dir: PathBuf,
    mut child: Child,
    tx: broadcast::Sender<WorkerEvent>,
) -> Result<ActiveRun, ApiError> {
    let stdout = child.stdout.take().context("worker stdout missing")?;
    let stderr = child.stderr.take().context("worker stderr missing")?;
    let stdin = child.stdin.take().context("worker stdin missing")?;
    let stdin = Arc::new(Mutex::new(stdin));
    let metrics_path = run_dir.join("metrics.jsonl");
    let control_events_path = run_dir.join("control-events.jsonl");
    let summary_path = run_dir.join("summary.json");

    let stdout_state = state.clone();
    let stdout_run_id = run_id.clone();
    let _stdout_tx = tx.clone();
    let _stdout_task = tokio::spawn(async move {
        let mut file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&metrics_path)
            .await
        {
            Ok(file) => file,
            Err(error) => {
                error!(?error, "failed to open metrics file");
                return;
            }
        };
        let mut control_events_file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&control_events_path)
            .await
        {
            Ok(file) => file,
            Err(error) => {
                error!(?error, "failed to open control events file");
                return;
            }
        };
        // Handle stdout
        let mut reader = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            if let Ok(event) = serde_json::from_str::<WorkerEvent>(&line) {
                let _ = handle_worker_event(&stdout_state, &stdout_run_id, &summary_path, &mut file, &mut control_events_file, event).await;
            }
        }
    });

    // Spawn stderr handler
    let stderr_state = state.clone();
    let stderr_run_id = run_id.clone();
    let _stderr_tx = tx.clone();
    let _stderr_task = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            let _ = record_run_log(&stderr_state, &stderr_run_id, RunLogEntry {
                timestamp_ms: now_ms(),
                level: RunLogLevel::Info,
                source: RunLogSource::WorkerStderr,
                message: line,
            }).await;
        }
    });

    // Spawn post-run processing
    let post_state = state.clone();
    let post_run_id = run_id.clone();
    let post_run_dir = run_dir.clone();
    let _post_task = tokio::spawn(async move {
        let _ = child.wait().await;

        // Post-run: scan the run directory for perf data files (and process each).
        // Prefer the spawn-options.json file (written at spawn time) for per-run decisions;
        // fall back to server options where applicable.
        let spawn_opts_path = post_run_dir.join("spawn-options.json");
        let spawn_opts: Option<ServerOptions> = match fs::read(&spawn_opts_path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).ok(),
            Err(_) => None,
        };

        let server_opts = post_state.options.read().await.clone();

        // Collect candidate perf files in the run dir.
        let mut perf_files: Vec<PathBuf> = Vec::new();
        if let Ok(mut dir) = fs::read_dir(&post_run_dir).await {
            while let Ok(Some(entry)) = dir.next_entry().await {
                if let Some(name) = entry.file_name().to_str() {
                    if name == "perf.data" || (name.ends_with(".data") && name.contains("perf")) {
                        perf_files.push(entry.path());
                    }
                }
            }
        }

        // Also consider configured output paths from spawn or server options.
        if let Some(ref so) = spawn_opts {
            if so.worker_perf {
                if let Some(ref p) = so.worker_perf_output {
                    let ppath = if std::path::Path::new(p).is_absolute() {
                        std::path::PathBuf::from(p)
                    } else {
                        post_run_dir.join(p)
                    };
                    if ppath.exists() {
                        perf_files.push(ppath);
                    }
                }
            }
        } else if server_opts.worker_perf {
            if let Some(ref p) = server_opts.worker_perf_output {
                let ppath = if std::path::Path::new(p).is_absolute() {
                    std::path::PathBuf::from(p)
                } else {
                    post_run_dir.join(p)
                };
                if ppath.exists() {
                    perf_files.push(ppath);
                }
            }
        }

        // Deduplicate
        perf_files.sort();
        perf_files.dedup();

        if !perf_files.is_empty() {
            for perf_path in perf_files {
                let _ = record_run_log(
                    &post_state,
                    &post_run_id,
                    RunLogEntry {
                        timestamp_ms: now_ms(),
                        level: RunLogLevel::Info,
                        source: RunLogSource::Server,
                        message: format!("found perf data at {}", perf_path.display()),
                    },
                )
                .await;

                // Decide whether to generate flamegraph: spawn_opts has priority, then server opts.
                let mut generate = server_opts.worker_perf_generate_flamegraph;
                if let Some(ref so) = spawn_opts {
                    generate = so.worker_perf_generate_flamegraph;
                }

                if generate {
                    // Run `perf script -i <perf.data>` and attempt to run stackcollapse+flamegraph.
                    match Command::new("perf")
                        .args(["script", "-i", perf_path.to_str().unwrap()])
                        .output()
                        .await
                    {
                        Ok(perf_script_out) if perf_script_out.status.success() => {
                            let stem = perf_path
                                .file_stem()
                                .and_then(|s| s.to_str())
                                .unwrap_or("perf");
                            let folded_path = post_run_dir.join(format!("{}.folded", stem));
                            let flame_path = post_run_dir.join(format!("{}-flamegraph.svg", stem));

                            match Command::new("stackcollapse-perf.pl")
                                .stdin(Stdio::piped())
                                .stdout(Stdio::piped())
                                .spawn()
                            {
                                Ok(mut proc) => {
                                    if let Some(mut stdin) = proc.stdin.take() {
                                        let _ = stdin.write_all(&perf_script_out.stdout).await;
                                    }
                                    match proc.wait_with_output().await {
                                        Ok(collapse_out) if collapse_out.status.success() => {
                                            match Command::new("flamegraph.pl")
                                                .stdin(Stdio::piped())
                                                .stdout(Stdio::piped())
                                                .spawn()
                                            {
                                                Ok(mut flame_proc) => {
                                                    if let Some(mut flame_stdin) =
                                                        flame_proc.stdin.take()
                                                    {
                                                        let _ = flame_stdin
                                                            .write_all(&collapse_out.stdout)
                                                            .await;
                                                    }
                                                    match flame_proc.wait_with_output().await {
                                                        Ok(flame_out)
                                                            if flame_out.status.success() =>
                                                        {
                                                            let _ = fs::write(
                                                                &flame_path,
                                                                &flame_out.stdout,
                                                            )
                                                            .await;
                                                            let _ = record_run_log(
                                                                &post_state,
                                                                &post_run_id,
                                                                RunLogEntry {
                                                                    timestamp_ms: now_ms(),
                                                                    level: RunLogLevel::Info,
                                                                    source: RunLogSource::Server,
                                                                    message: format!(
                                                                        "flamegraph generated: {}",
                                                                        flame_path.display()
                                                                    ),
                                                                },
                                                            )
                                                            .await;
                                                        }
                                                        _ => {
                                                            let _ = fs::write(&folded_path, &collapse_out.stdout).await;
                                                            let perf_script_path = post_run_dir.join(format!("{}.script", stem));
                                                            let _ = fs::write(&perf_script_path, &perf_script_out.stdout).await;
                                                            let _ = record_run_log(
                                                                &post_state,
                                                                &post_run_id,
                                                                RunLogEntry {
                                                                    timestamp_ms: now_ms(),
                                                                    level: RunLogLevel::Warn,
                                                                    source: RunLogSource::Server,
                                                                    message: format!("flamegraph.pl not available; saved folded perf data at {}", folded_path.display()),
                                                                },
                                                            )
                                                            .await;
                                                        }
                                                    }
                                                }
                                                Err(_) => {
                                                    let perf_script_path = post_run_dir.join(format!("{}.script", stem));
                                                    let _ = fs::write(&perf_script_path, &perf_script_out.stdout).await;
                                                    let _ = record_run_log(
                                                        &post_state,
                                                        &post_run_id,
                                                        RunLogEntry {
                                                            timestamp_ms: now_ms(),
                                                            level: RunLogLevel::Warn,
                                                            source: RunLogSource::Server,
                                                            message: format!("flamegraph.pl not available; wrote perf.script to {}", perf_script_path.display()),
                                                        },
                                                    )
                                                    .await;
                                                }
                                            }
                                        }
                                        _ => {
                                            let perf_script_path = post_run_dir.join(format!("{}.script", stem));
                                            let _ = fs::write(&perf_script_path, &perf_script_out.stdout).await;
                                            let _ = record_run_log(
                                                &post_state,
                                                &post_run_id,
                                                RunLogEntry {
                                                    timestamp_ms: now_ms(),
                                                    level: RunLogLevel::Warn,
                                                    source: RunLogSource::Server,
                                                    message: format!("stackcollapse-perf.pl failed; wrote perf.script to {}", perf_script_path.display()),
                                                },
                                            )
                                            .await;
                                        }
                                    }
                                }
                                Err(_) => {
                                    let perf_script_path = post_run_dir.join(format!("{}.script", stem));
                                    let _ = fs::write(&perf_script_path, &perf_script_out.stdout).await;
                                    let _ = record_run_log(
                                        &post_state,
                                        &post_run_id,
                                        RunLogEntry {
                                            timestamp_ms: now_ms(),
                                            level: RunLogLevel::Warn,
                                            source: RunLogSource::Server,
                                            message: format!("stackcollapse-perf.pl not available; wrote perf.script to {}", perf_script_path.display()),
                                        },
                                    )
                                    .await;
                                }
                            }
                        }
                        _ => {
                            let _ = record_run_log(
                                &post_state,
                                &post_run_id,
                                RunLogEntry {
                                    timestamp_ms: now_ms(),
                                    level: RunLogLevel::Warn,
                                    source: RunLogSource::Server,
                                    message: format!("failed to run `perf script` on {}; leaving perf data in run dir", perf_path.display()),
                                },
                            )
                            .await;
                        }
                    }
                } else {
                    let _ = record_run_log(
                        &post_state,
                        &post_run_id,
                        RunLogEntry {
                            timestamp_ms: now_ms(),
                            level: RunLogLevel::Info,
                            source: RunLogSource::Server,
                            message: format!("perf data present at {} but flamegraph generation is disabled", perf_path.display()),
                        },
                    )
                    .await;
                }
            }
        }

        // Strace: list any strace.* files written into the run directory and report them
        match fs::read_dir(&post_run_dir).await {
            Ok(mut dir) => {
                let mut found = Vec::new();
                while let Ok(Some(entry)) = dir.next_entry().await {
                    if let Some(name) = entry.file_name().to_str() {
                        if name.starts_with("strace") {
                            found.push(entry.path());
                        }
                    }
                }
                if !found.is_empty() {
                    let paths = found
                        .iter()
                        .map(|p| p.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    let _ = record_run_log(
                        &post_state,
                        &post_run_id,
                        RunLogEntry {
                            timestamp_ms: now_ms(),
                            level: RunLogLevel::Info,
                            source: RunLogSource::Server,
                            message: format!("strace outputs: {}", paths),
                        },
                    )
                    .await;
                }
            }
            Err(e) => {
                error!(?e, "failed to scan run dir for strace outputs");
            }
        }

        // Update saved summary to include any artifacts produced after the run (perf data, flamegraph, strace)
        let summary_path = post_run_dir.join("summary.json");
        if summary_path.exists() {
            match fs::read(&summary_path).await {
                Ok(bytes) => {
                    if let Ok(mut existing_summary) = serde_json::from_slice::<RunSummary>(&bytes) {
                        existing_summary.artifact_paths = artifact_paths(&post_run_dir);
                        match serde_json::to_vec_pretty(&existing_summary) {
                            Ok(serialized) => {
                                if let Err(e) = fs::write(&summary_path, &serialized).await {
                                    error!(
                                        ?e,
                                        "failed to write updated summary.json with artifacts"
                                    );
                                } else {
                                    let _ = record_run_log(
                                        &post_state,
                                        &post_run_id,
                                        RunLogEntry {
                                            timestamp_ms: now_ms(),
                                            level: RunLogLevel::Info,
                                            source: RunLogSource::Server,
                                            message: format!("updated summary artifact paths"),
                                        },
                                    )
                                    .await;
                                    let mut runs = post_state.runs.write().await;
                                    if let Some(run) = runs.get_mut(&post_run_id) {
                                        run.summary = Some(existing_summary.clone());
                                    }
                                }
                            }
                            Err(e) => {
                                error!(?e, "failed to serialize updated summary.json");
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(?e, "failed to read summary.json for artifact update");
                }
            }
        }
    });

    Ok(ActiveRun { stdin, tx })
}

async fn handle_worker_event(
    state: &AppState,
    run_id: &str,
    summary_path: &FsPath,
    metrics_file: &mut tokio::fs::File,
    control_events_file: &mut tokio::fs::File,
    event: WorkerEvent,
) -> Result<()> {
    match event {
        WorkerEvent::Ready { pid, warnings, .. } => {
            let mut runs = state.runs.write().await;
            if let Some(run) = runs.get_mut(run_id) {
                run.status = RunStatus::Running;
                run.warnings = warnings;
            }
            drop(runs);
            record_run_log(
                state,
                run_id,
                RunLogEntry {
                    timestamp_ms: now_ms(),
                    level: RunLogLevel::Info,
                    source: RunLogSource::WorkerEvent,
                    message: format!("Worker ready (pid={pid}) and dataset prepared"),
                },
            )
            .await?;
        }
        WorkerEvent::ControlApplied {
            event,
            effective_config,
            ..
        } => {
            let message = format!(
                "Control applied: {}",
                describe_control_event(&effective_config)
            );
            control_events_file
                .write_all(format!("{}\n", serde_json::to_string(&event)?).as_bytes())
                .await?;
            let mut runs = state.runs.write().await;
            if let Some(run) = runs.get_mut(run_id) {
                run.control_events.push(event);
                run.effective_config = effective_config;
            }
            drop(runs);
            record_run_log(
                state,
                run_id,
                RunLogEntry {
                    timestamp_ms: now_ms(),
                    level: RunLogLevel::Info,
                    source: RunLogSource::WorkerEvent,
                    message,
                },
            )
            .await?;
        }
        WorkerEvent::Sample { sample } => {
            metrics_file
                .write_all(format!("{}\n", serde_json::to_string(&sample)?).as_bytes())
                .await?;
            let mut runs = state.runs.write().await;
            if let Some(run) = runs.get_mut(run_id) {
                run.latest_sample = Some(sample.clone());
                run.status = RunStatus::Running;
            }
            drop(runs);
            let message = format!(
                "Sample: writes/s={:.1}, reads/s={:.1}, p95={:.2} ms, rss={}, errors={}",
                sample.writes_per_sec,
                sample.reads_per_sec,
                sample.p95_latency_ms,
                sample.rss_bytes,
                sample.error_count
            );
            record_run_log(
                state,
                run_id,
                RunLogEntry {
                    timestamp_ms: sample.timestamp_ms,
                    level: RunLogLevel::Debug,
                    source: RunLogSource::WorkerEvent,
                    message,
                },
            )
            .await?;
        }
        WorkerEvent::Finished { mut summary } => {
            let status = summary.status.clone();
            let avg_writes = summary.avg_writes_per_sec;
            let avg_reads = summary.avg_reads_per_sec;
            let missing_error_logs = {
                let runs = state.runs.read().await;
                runs.get(run_id)
                    .map(|run| {
                        summary
                            .error_messages
                            .iter()
                            .filter(|message| {
                                !run.recent_logs
                                    .iter()
                                    .any(|entry| entry.message == **message)
                            })
                            .cloned()
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            };
            for message in missing_error_logs {
                record_run_log(
                    state,
                    run_id,
                    RunLogEntry {
                        timestamp_ms: now_ms(),
                        level: RunLogLevel::Error,
                        source: RunLogSource::WorkerEvent,
                        message,
                    },
                )
                .await?;
            }
            record_run_log(
                state,
                run_id,
                RunLogEntry {
                    timestamp_ms: now_ms(),
                    level: RunLogLevel::Info,
                    source: RunLogSource::Server,
                    message: format!(
                        "Run finished: status={status:?}, avg writes/s={avg_writes:.1}, avg reads/s={avg_reads:.1}"
                    ),
                },
            )
            .await?;
            {
                let runs = state.runs.read().await;
                if let Some(run) = runs.get(run_id) {
                    summary.log_count = run.log_count;
                    summary.recent_logs = run.recent_logs.clone();
                }
            }
            fs::write(summary_path, serde_json::to_vec_pretty(&summary)?).await?;
            let mut runs = state.runs.write().await;
            if let Some(run) = runs.get_mut(run_id) {
                run.ended_at_ms = Some(summary.ended_at_ms);
                run.status = summary.status.clone();
                run.warnings = summary.warnings.clone();
                run.error_messages = summary.error_messages.clone();
                run.control_events = summary.control_events.clone();
                run.effective_config = summary.final_config.clone();
                run.summary = Some(summary);
                run.active = None;
            }
        }
        WorkerEvent::Log { entry, .. } => {
            record_run_log(state, run_id, entry).await?;
        }
        WorkerEvent::Failed { message, .. } => {
            warn!("worker failed: {message}");
            record_run_log(
                state,
                run_id,
                RunLogEntry {
                    timestamp_ms: now_ms(),
                    level: RunLogLevel::Error,
                    source: RunLogSource::Server,
                    message: format!("Worker failed: {message}"),
                },
            )
            .await?;
            finalize_failed_run(state, run_id, message).await?;
        }
    }
    Ok(())
}

async fn spawn_worker_process(
    run_id: &str,
    run_dir: &FsPath,
    config_path: &FsPath,
    options: ServerOptions,
    config: &BenchmarkConfig,
) -> Result<SpawnedWorker, ApiError> {
    let current_exe = std::env::current_exe()?;
    let worker_binary = current_exe.with_file_name(worker_binary_name());

    // Build the invocation vector (program + args).
    let mut invocation: Vec<String> = Vec::new();
    let reuse_binary = should_reuse_worker_binary(&current_exe, &worker_binary);
    if reuse_binary {
        invocation.push(worker_binary.display().to_string());
    } else {
        invocation.push("cargo".to_string());
        invocation.push("run".to_string());
        invocation.push("--quiet".to_string());
        invocation.push("-p".to_string());
        invocation.push("benchmark-app".to_string());
        invocation.push("--bin".to_string());
        invocation.push("benchmark-worker".to_string());
        invocation.push("--".to_string());
    }

    invocation.push("--run-id".to_string());
    invocation.push(run_id.to_string());
    invocation.push("--run-dir".to_string());
    invocation.push(run_dir.display().to_string());
    invocation.push("--config".to_string());
    invocation.push(config_path.display().to_string());

    // Merge server-level options with per-run profiling fields (if any)
    let mut effective = options.clone();
    if let Some(prof) = &config.profiling {
        if let Some(val) = prof.worker_perf {
            effective.worker_perf = val;
        }
        if let Some(val) = prof.worker_perf_generate_flamegraph {
            effective.worker_perf_generate_flamegraph = val;
        }
        if let Some(val) = prof.worker_perf_freq_hz {
            effective.worker_perf_freq_hz = Some(val);
        }
        if let Some(val) = prof.worker_perf_output.clone() {
            effective.worker_perf_output = Some(val);
        }
        if let Some(val) = prof.worker_strace {
            effective.worker_strace = val;
        }
        if let Some(val) = prof.worker_strace_output.clone() {
            effective.worker_strace_output = Some(val);
        }
    }

    // Persist the effective spawn options into the run directory so post-run tasks can
    // discover what runner options were used (especially when per-run profiling is set).
    let spawn_opts_path = run_dir.join("spawn-options.json");
    if let Ok(serialized) = serde_json::to_vec_pretty(&effective) {
        let _ = fs::write(&spawn_opts_path, &serialized).await;
    }

    // Decide whether to wrap invocation with perf or strace
    let (launcher, mut command) = if effective.worker_perf {
        // normalize perf output path: relative -> inside run_dir
        let perf_out = effective
            .worker_perf_output
            .clone()
            .unwrap_or_else(|| "perf.data".to_string());
        let perf_out_path = if std::path::Path::new(&perf_out).is_absolute() {
            std::path::PathBuf::from(perf_out)
        } else {
            run_dir.join(perf_out)
        };
        let perf_out_str = perf_out_path.display().to_string();
        let freq = effective
            .worker_perf_freq_hz
            .map(|hz| hz.to_string())
            .unwrap_or_else(|| "99".to_string());
        let mut cmd = Command::new("perf");
        cmd.args(["record", "-F", &freq, "-g", "-o", &perf_out_str, "--"]);
        cmd.args(invocation.iter());
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        ("perf record", cmd)
    } else if effective.worker_strace {
        let strace_prefix = effective
            .worker_strace_output
            .clone()
            .unwrap_or_else(|| "strace".to_string());
        let strace_path = if std::path::Path::new(&strace_prefix).is_absolute() {
            std::path::PathBuf::from(strace_prefix)
        } else {
            run_dir.join(strace_prefix)
        };
        let strace_prefix_str = strace_path.display().to_string();
        let mut cmd = Command::new("strace");
        cmd.args(["-ff", "-tt", "-o", &strace_prefix_str, "-s", "200", "--"]);
        cmd.args(invocation.iter());
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        ("strace", cmd)
    } else {
        // direct invocation
        let program = &invocation[0];
        let mut cmd = Command::new(program);
        if invocation.len() > 1 {
            cmd.args(&invocation[1..]);
        }
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if reuse_binary {
            ("worker binary", cmd)
        } else {
            ("cargo run", cmd)
        }
    };

    let child = command.spawn().map_err(ApiError::from)?;
    Ok(SpawnedWorker { child, launcher })
}

fn should_reuse_worker_binary(current_exe: &FsPath, worker_binary: &FsPath) -> bool {
    if !worker_binary.exists() {
        return false;
    }

    let Ok(worker_meta) = std::fs::metadata(worker_binary) else {
        return true;
    };
    let Ok(server_meta) = std::fs::metadata(current_exe) else {
        return true;
    };
    let Ok(worker_modified) = worker_meta.modified() else {
        return true;
    };
    let Ok(server_modified) = server_meta.modified() else {
        return true;
    };

    worker_modified >= server_modified
}

fn worker_binary_name() -> &'static OsStr {
    #[cfg(target_os = "windows")]
    {
        OsStr::new("benchmark-worker.exe")
    }
    #[cfg(not(target_os = "windows"))]
    {
        OsStr::new("benchmark-worker")
    }
}

async fn load_existing_runs(runs_dir: &FsPath) -> Result<HashMap<String, StoredRun>> {
    let mut runs = HashMap::new();
    let mut entries = fs::read_dir(runs_dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_dir() {
            continue;
        }
        let run_dir = entry.path();
        let run_id = entry.file_name().to_string_lossy().to_string();
        let config_path = run_dir.join("config.json");
        if !config_path.exists() {
            continue;
        }
        let config: BenchmarkConfig = serde_json::from_slice(&fs::read(&config_path).await?)?;
        let control_events = load_control_events(&run_dir.join("control-events.jsonl"))
            .await
            .unwrap_or_default();
        let summary_path = run_dir.join("summary.json");
        let summary = if summary_path.exists() {
            Some(serde_json::from_slice::<RunSummary>(
                &fs::read(&summary_path).await?,
            )?)
        } else {
            None
        };
        let latest_sample = latest_sample(&run_dir.join("metrics.jsonl")).await?;
        let status = summary
            .as_ref()
            .map(|summary| summary.status.clone())
            .unwrap_or(RunStatus::Interrupted);
        let started_at_ms = summary
            .as_ref()
            .map(|summary| summary.started_at_ms)
            .unwrap_or_else(now_ms);
        let ended_at_ms = summary.as_ref().map(|summary| summary.ended_at_ms);
        let warnings = summary
            .as_ref()
            .map(|summary| summary.warnings.clone())
            .unwrap_or_default();
        let error_messages = summary
            .as_ref()
            .map(|summary| summary.error_messages.clone())
            .unwrap_or_default();
        let recent_logs = summary
            .as_ref()
            .map(|summary| summary.recent_logs.clone())
            .unwrap_or_default();
        let log_count = summary
            .as_ref()
            .map(|summary| summary.log_count)
            .unwrap_or_else(|| {
                load_logs_sync(&run_dir.join("logs.jsonl"))
                    .map(|logs| logs.len())
                    .unwrap_or(0)
            });
        let effective_config = summary
            .as_ref()
            .map(|summary| summary.final_config.clone())
            .unwrap_or_else(|| apply_control_events(&config, &control_events));

        runs.insert(
            run_id.clone(),
            StoredRun {
                run_id,
                config,
                effective_config,
                status,
                started_at_ms,
                ended_at_ms,
                warnings,
                error_messages,
                control_events,
                latest_sample,
                recent_logs,
                log_count,
                summary,
                run_dir,
                active: None,
            },
        );
    }
    Ok(runs)
}

async fn load_samples(path: &FsPath) -> Result<Vec<MetricSample>, ApiError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = fs::read(path).await?;
    let mut samples = Vec::new();
    for line in String::from_utf8_lossy(&bytes).lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(sample) = serde_json::from_str::<MetricSample>(line) {
            samples.push(sample);
        }
    }
    Ok(samples)
}

async fn latest_sample(path: &FsPath) -> Result<Option<MetricSample>> {
    let samples = load_samples(path)
        .await
        .map_err(|error| anyhow::anyhow!(error.message))?;
    Ok(samples.last().cloned())
}

async fn load_logs(path: &FsPath) -> Result<Vec<RunLogEntry>, ApiError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = fs::read(path).await?;
    Ok(parse_logs(&bytes))
}

fn load_logs_sync(path: &FsPath) -> Result<Vec<RunLogEntry>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    Ok(parse_logs(&std::fs::read(path)?))
}

fn parse_logs(bytes: &[u8]) -> Vec<RunLogEntry> {
    let mut logs = Vec::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(entry) = serde_json::from_str::<RunLogEntry>(line) {
            logs.push(entry);
        }
    }
    logs
}

async fn load_control_events(path: &FsPath) -> Result<Vec<AppliedControlEvent>, ApiError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = fs::read(path).await?;
    let mut events = Vec::new();
    for line in String::from_utf8_lossy(&bytes).lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(event) = serde_json::from_str::<AppliedControlEvent>(line) {
            events.push(event);
        }
    }
    Ok(events)
}

fn apply_control_events(
    initial_config: &BenchmarkConfig,
    events: &[AppliedControlEvent],
) -> BenchmarkConfig {
    let mut config = initial_config.clone();
    for event in events {
        match &event.control {
            ControlMessage::UpdateConcurrency { concurrency } => {
                config.load.concurrency = *concurrency;
            }
            ControlMessage::UpdateMix {
                point_reads,
                range_scans,
                inserts,
                updates,
            } => {
                config.load.mix = benchmark_core::OperationMix {
                    point_reads: *point_reads,
                    range_scans: *range_scans,
                    inserts: *inserts,
                    updates: *updates,
                };
            }
            ControlMessage::ApplyPhase { phase } => config.apply_phase(phase),
            ControlMessage::Pause | ControlMessage::Resume | ControlMessage::Stop => {}
        }
    }
    config
}

fn summarize_samples(samples: &[MetricSample]) -> RunAggregate {
    let mut aggregate = RunAggregate::default();
    for sample in samples {
        aggregate.update(sample);
    }
    aggregate
}

fn artifact_paths(run_dir: &FsPath) -> ArtifactPaths {
    let perf_data = run_dir.join("perf.data");
    let flame = run_dir.join("perf-flamegraph.svg");
    // collect any strace files
    let mut strace_paths = Vec::new();
    if let Ok(iter) = std::fs::read_dir(run_dir) {
        for entry in iter.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with("strace") {
                    strace_paths.push(entry.path().display().to_string());
                }
            }
        }
    }

    ArtifactPaths {
        config_path: run_dir.join("config.json").display().to_string(),
        metrics_path: run_dir.join("metrics.jsonl").display().to_string(),
        summary_path: run_dir.join("summary.json").display().to_string(),
        control_events_path: run_dir.join("control-events.jsonl").display().to_string(),
        data_dir: run_dir.join("data").display().to_string(),
        logs_path: run_dir.join("logs.jsonl").display().to_string(),
        perf_data_path: if perf_data.exists() {
            Some(perf_data.display().to_string())
        } else {
            None
        },
        flamegraph_path: if flame.exists() {
            Some(flame.display().to_string())
        } else {
            None
        },
        strace_paths,
    }
}

async fn record_run_log(state: &AppState, run_id: &str, entry: RunLogEntry) -> Result<()> {
    match entry.level {
        RunLogLevel::Debug => debug!(run_id, source = ?entry.source, "{}", entry.message),
        RunLogLevel::Info => info!(run_id, source = ?entry.source, "{}", entry.message),
        RunLogLevel::Warn => warn!(run_id, source = ?entry.source, "{}", entry.message),
        RunLogLevel::Error => error!(run_id, source = ?entry.source, "{}", entry.message),
    }

    let (run_dir, tx) = {
        let runs = state.runs.read().await;
        let Some(run) = runs.get(run_id) else {
            return Ok(());
        };
        (
            run.run_dir.clone(),
            run.active.as_ref().map(|active| active.tx.clone()),
        )
    };

    let log_path = run_dir.join("logs.jsonl");
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await?;
    file.write_all(format!("{}\n", serde_json::to_string(&entry)?).as_bytes())
        .await?;

    {
        let mut runs = state.runs.write().await;
        if let Some(run) = runs.get_mut(run_id) {
            run.log_count += 1;
            run.recent_logs.push(entry.clone());
            trim_recent_logs(&mut run.recent_logs);
        }
    }

    if let Some(tx) = tx {
        let _ = tx.send(WorkerEvent::Log {
            run_id: run_id.to_string(),
            entry,
        });
    }
    Ok(())
}

fn trim_recent_logs(logs: &mut Vec<RunLogEntry>) {
    if logs.len() > MAX_RECENT_LOGS {
        let extra = logs.len() - MAX_RECENT_LOGS;
        logs.drain(0..extra);
    }
}

fn describe_control_event(config: &BenchmarkConfig) -> String {
    format!(
        "effective concurrency={} mix={}/{}/{}/{}",
        config.load.concurrency,
        config.load.mix.point_reads,
        config.load.mix.range_scans,
        config.load.mix.inserts,
        config.load.mix.updates
    )
}

async fn finalize_failed_run(state: &AppState, run_id: &str, message: String) -> Result<()> {
    let (
        run_dir,
        config,
        effective_config,
        started_at_ms,
        ended_at_ms,
        warnings,
        error_messages,
        control_events,
        recent_logs,
        log_count,
        has_summary,
    ) = {
        let mut runs = state.runs.write().await;
        let Some(run) = runs.get_mut(run_id) else {
            return Ok(());
        };
        if !run
            .error_messages
            .iter()
            .any(|existing| existing == &message)
            && run.error_messages.len() < 8
        {
            run.error_messages.push(message.clone());
        }
        run.status = RunStatus::Failed;
        run.active = None;
        let ended_at_ms = *run.ended_at_ms.get_or_insert_with(now_ms);
        (
            run.run_dir.clone(),
            run.config.clone(),
            run.effective_config.clone(),
            run.started_at_ms,
            ended_at_ms,
            run.warnings.clone(),
            run.error_messages.clone(),
            run.control_events.clone(),
            run.recent_logs.clone(),
            run.log_count,
            run.summary.is_some(),
        )
    };

    if has_summary {
        return Ok(());
    }

    let samples = load_samples(&run_dir.join("metrics.jsonl"))
        .await
        .map_err(|error| anyhow::anyhow!(error.message))?;
    let aggregate = summarize_samples(&samples);
    let summary = RunSummary {
        run_id: run_id.to_string(),
        engine: config.engine,
        config,
        final_config: effective_config,
        started_at_ms,
        ended_at_ms,
        status: RunStatus::Failed,
        warnings,
        error_messages,
        control_events,
        artifact_paths: artifact_paths(&run_dir),
        avg_writes_per_sec: aggregate.avg_writes_per_sec(),
        avg_reads_per_sec: aggregate.avg_reads_per_sec(),
        peak_rss_bytes: aggregate.peak_rss_bytes(),
        peak_disk_usage_bytes: aggregate.peak_disk_usage_bytes(),
        log_count,
        recent_logs,
    };
    let summary_path = run_dir.join("summary.json");
    fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?).await?;

    let mut runs = state.runs.write().await;
    if let Some(run) = runs.get_mut(run_id) {
        run.summary = Some(summary);
        run.ended_at_ms = Some(ended_at_ms);
        run.status = RunStatus::Failed;
        run.active = None;
    }
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(error: std::io::Error) -> Self {
        anyhow::Error::from(error).into()
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(error: serde_json::Error) -> Self {
        anyhow::Error::from(error).into()
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        (
            self.status,
            headers,
            serde_json::json!({ "error": self.message }).to_string(),
        )
            .into_response()
    }
}