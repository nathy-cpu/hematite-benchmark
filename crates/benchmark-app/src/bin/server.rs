use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    benchmark_app::server::run_server().await
}
