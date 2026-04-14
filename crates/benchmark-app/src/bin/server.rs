use anyhow::Result;
use benchmark_app::server::ServerVerbosity;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    benchmark_app::server::run_server_with_verbosity(parse_verbosity(env::args().skip(1))?).await
}

fn parse_verbosity(args: impl IntoIterator<Item = String>) -> Result<ServerVerbosity> {
    let mut verbosity = ServerVerbosity::default();
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
            _ => {}
        }
    }
    Ok(verbosity)
}
