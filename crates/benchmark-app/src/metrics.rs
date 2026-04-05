use benchmark_core::IoPrecision;
use std::fs;
use std::path::Path;
use sysinfo::{Pid, ProcessesToUpdate, System};
use walkdir::WalkDir;

#[derive(Debug, Clone, Copy)]
pub struct IoCounters {
    pub read_bytes: u64,
    pub write_bytes: u64,
}

pub fn current_rss_bytes() -> u64 {
    let mut system = System::new();
    let pid = Pid::from_u32(std::process::id());
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    system
        .process(pid)
        .map(|process| process.memory())
        .unwrap_or(0)
}

pub fn current_io_counters() -> (Option<IoCounters>, IoPrecision) {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = fs::read_to_string("/proc/self/io") {
            let mut read_bytes = None;
            let mut write_bytes = None;
            for line in contents.lines() {
                if let Some(value) = line.strip_prefix("read_bytes:") {
                    read_bytes = value.trim().parse::<u64>().ok();
                }
                if let Some(value) = line.strip_prefix("write_bytes:") {
                    write_bytes = value.trim().parse::<u64>().ok();
                }
            }
            if let (Some(read_bytes), Some(write_bytes)) = (read_bytes, write_bytes) {
                return (
                    Some(IoCounters {
                        read_bytes,
                        write_bytes,
                    }),
                    IoPrecision::Exact,
                );
            }
        }
    }

    (None, IoPrecision::Approximate)
}

pub fn dir_size_bytes(path: &Path) -> u64 {
    WalkDir::new(path)
        .into_iter()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.metadata().ok())
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len())
        .sum()
}
