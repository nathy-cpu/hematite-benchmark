use anyhow::Result;

fn main() -> Result<()> {
    benchmark_app::runtime::run_worker_from_args()
}
