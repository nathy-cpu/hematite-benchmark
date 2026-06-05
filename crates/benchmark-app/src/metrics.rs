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

/// RSS breakdown: anonymous (heap+stack) and total (including file-backed mmap).
#[derive(Debug, Clone, Copy)]
pub struct RssInfo {
    /// Anonymous RSS — heap, stack, and anonymous mmap only.
    /// This is the meaningful "memory usage" for benchmarks since it excludes
    /// file-backed pages (e.g. mmap'd database files).
    pub anon_bytes: u64,
    /// Total RSS including file-backed mmap pages.
    pub total_bytes: u64,
}

pub fn current_rss_info() -> RssInfo {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = fs::read_to_string("/proc/self/status") {
            let mut anon_kb = None;
            let mut total_kb = None;
            for line in contents.lines() {
                if let Some(value) = line.strip_prefix("RssAnon:") {
                    anon_kb = parse_status_kb(value);
                }
                if let Some(value) = line.strip_prefix("VmRSS:") {
                    total_kb = parse_status_kb(value);
                }
            }
            if let Some(total) = total_kb {
                return RssInfo {
                    anon_bytes: anon_kb.unwrap_or(total) * 1024,
                    total_bytes: total * 1024,
                };
            }
        }
        // Fallback: /proc/self/statm (no anon/total distinction available)
        if let Ok(contents) = fs::read_to_string("/proc/self/statm") {
            if let Some(rss_pages) = contents.split_whitespace().nth(1) {
                if let Ok(pages) = rss_pages.parse::<u64>() {
                    let total = pages * 4096;
                    return RssInfo {
                        anon_bytes: total,
                        total_bytes: total,
                    };
                }
            }
        }
    }

    // Non-Linux fallback via sysinfo
    let mut system = System::new();
    let pid = Pid::from_u32(std::process::id());
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    let total = system
        .process(pid)
        .map(|process| process.memory())
        .unwrap_or(0);
    RssInfo {
        anon_bytes: total,
        total_bytes: total,
    }
}

#[cfg(target_os = "linux")]
fn parse_status_kb(value: &str) -> Option<u64> {
    value
        .trim()
        .trim_end_matches(" kB")
        .trim()
        .parse::<u64>()
        .ok()
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
                // Physical bytes sent to storage — used for the write I/O metric.
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

    // Non-Linux fallback via sysinfo
    let mut system = System::new();
    let pid = Pid::from_u32(std::process::id());
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    if let Some(process) = system.process(pid) {
        let disk = process.disk_usage();
        (
            Some(IoCounters {
                read_bytes: disk.total_read_bytes,
                write_bytes: disk.total_written_bytes,
            }),
            IoPrecision::Exact,
        )
    } else {
        (None, IoPrecision::Approximate)
    }
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
