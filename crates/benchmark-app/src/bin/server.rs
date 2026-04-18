use anyhow::Result;
use benchmark_app::server::{ServerOptions, ServerVerbosity};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let (verbosity, options) = parse_args(env::args().skip(1))?;
    benchmark_app::server::run_server_with_verbosity(verbosity, options).await
}
fn parse_args(args: impl IntoIterator<Item = String>) -> Result<(ServerVerbosity, ServerOptions)> {
    let mut verbosity = ServerVerbosity::default();
    let mut options = ServerOptions::default();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-v" | "--verbose" => {
                verbosity = match verbosity {
                    ServerVerbosity::Quiet | ServerVerbosity::Normal => ServerVerbosity::Verbose,
                    ServerVerbosity::Verbose => ServerVerbosity::Trace,
                    ServerVerbosity::Trace => ServerVerbosity::Trace,
                };
            }
            "--verbosity" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--verbosity requires a value"))?;
                verbosity = match value.as_str() {
                    "quiet" => ServerVerbosity::Quiet,
                    "normal" => ServerVerbosity::Normal,
                    "verbose" => ServerVerbosity::Verbose,
                    "trace" => ServerVerbosity::Trace,
                    other => {
                        return Err(anyhow::anyhow!(
                            "unsupported verbosity '{other}', expected quiet|normal|verbose|trace"
                        ));
                    }
                };
            }
            "--perf" => {
                options.worker_perf = true;
            }
            "--perf-output" => {
                if let Some(value) = args.next() {
                    options.worker_perf_output = Some(value);
                }
            }
            "--perf-freq" => {
                if let Some(value) = args.next() {
                    if let Ok(hz) = value.parse::<u32>() {
                        options.worker_perf_freq_hz = Some(hz);
                    }
                }
            }
            "--no-flamegraph" => {
                options.worker_perf_generate_flamegraph = false;
            }
            "--strace" => {
                options.worker_strace = true;
            }
            "--strace-output" => {
                if let Some(value) = args.next() {
                    options.worker_strace_output = Some(value);
                }
            }
            _ => {}
        }
    }
    Ok((verbosity, options))
}
