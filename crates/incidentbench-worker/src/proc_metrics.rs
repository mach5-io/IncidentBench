// Copyright 2025 Mach5 Software, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Tracks process CPU and memory usage by reading /proc/self.
pub struct ProcMetrics {
    /// Previous total CPU ticks (utime + stime).
    prev_cpu_ticks: AtomicU64,
    /// Timestamp of the previous reading.
    prev_time: std::sync::Mutex<Instant>,
    /// Clock ticks per second (sysconf(_SC_CLK_TCK), typically 100).
    clk_tck: f64,
}

impl ProcMetrics {
    pub fn new() -> Self {
        // CLK_TCK is 100 on all standard Linux kernels.
        let clk_tck = 100.0;
        let initial_ticks = read_cpu_ticks().unwrap_or(0);
        Self {
            prev_cpu_ticks: AtomicU64::new(initial_ticks),
            prev_time: std::sync::Mutex::new(Instant::now()),
            clk_tck,
        }
    }

    /// Sample current CPU utilization (0.0-N.0 where N = number of cores)
    /// and resident memory in bytes.
    pub fn sample(&self) -> (f64, u64) {
        let cpu = self.cpu_utilization();
        let mem = read_rss_bytes().unwrap_or(0);
        (cpu, mem)
    }

    fn cpu_utilization(&self) -> f64 {
        let current_ticks = match read_cpu_ticks() {
            Some(t) => t,
            None => return 0.0,
        };

        let prev_ticks = self.prev_cpu_ticks.swap(current_ticks, Ordering::Relaxed);
        let delta_ticks = current_ticks.saturating_sub(prev_ticks);

        let mut prev_time = self.prev_time.lock().unwrap();
        let elapsed = prev_time.elapsed().as_secs_f64();
        *prev_time = Instant::now();

        if elapsed > 0.0 && self.clk_tck > 0.0 {
            (delta_ticks as f64 / self.clk_tck) / elapsed
        } else {
            0.0
        }
    }
}

/// Read utime + stime from /proc/self/stat.
/// Fields 14 and 15 (0-indexed 13 and 14) are utime and stime in clock ticks.
fn read_cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // Skip past the comm field (enclosed in parens) since it may contain spaces.
    let after_comm = stat.rfind(')')? + 2;
    let fields: Vec<&str> = stat[after_comm..].split_whitespace().collect();
    // After comm and state, fields[11] = utime, fields[12] = stime (0-indexed from after state).
    // Actually: field 0 = state, 1 = ppid, ..., 11 = utime, 12 = stime
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

/// Read VmRSS from /proc/self/status (in bytes).
fn read_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb_str = rest.trim().strip_suffix("kB")?.trim();
            let kb: u64 = kb_str.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}
