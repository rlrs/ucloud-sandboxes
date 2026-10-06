//! One sandbox's relay traffic and the 10 ms policy's fixed bounds
//! (`local_wait.py` `Flow`, `cpu_usage_usec`).

use std::collections::VecDeque;
use std::path::Path;

/// A call's request went out at least this long ago,
pub const SETTLE_SECONDS: f64 = 0.05;
/// and the sandbox used no more than `IDLE_USEC` of CPU in this window.
pub const IDLE_SECONDS: f64 = 0.05;
pub const IDLE_USEC: u64 = 2000;
/// Clock arithmetic: a window of exactly 50 ms is a full window.
pub const EPSILON: f64 = 1e-6;
/// An answered call is watched this long, until the next request: any pause
/// that lands on it is undone. A status read in progress re-pauses after it
/// (keep_paused), and a thaw queued behind that read can lose to it.
pub const ANSWERED_WATCH_SECONDS: f64 = 30.0;

/// The newest payload in each direction, on the monotonic clock in seconds.
#[derive(Clone, Debug, Default)]
pub struct Flow {
    pub last_out: f64,
    pub last_in: f64,
    /// (monotonic time, usage_usec).
    pub cpu: VecDeque<(f64, u64)>,
    /// Thaw any pause that lands on this answered call until then.
    pub answered_until: f64,
}

impl Flow {
    /// A request went out after the last answer: its call is open.
    pub fn open(&self) -> bool {
        self.last_out > self.last_in
    }

    pub fn outstanding(&self, now: f64) -> bool {
        self.open() && now - self.last_out >= SETTLE_SECONDS - EPSILON
    }

    /// No more than `IDLE_USEC` CPU over at least the last `IDLE_SECONDS`.
    pub fn idle(&self, now: f64) -> bool {
        match (self.cpu.front(), self.cpu.back()) {
            (Some(first), Some(last)) if self.cpu.len() >= 2 => {
                now - first.0 >= IDLE_SECONDS - EPSILON && last.1.saturating_sub(first.1) <= IDLE_USEC
            }
            _ => false,
        }
    }

    /// Keep the newest sample at least `IDLE_SECONDS` old, and every newer one.
    pub fn sample(&mut self, now: f64, usage: u64) {
        self.cpu.push_back((now, usage));
        while self.cpu.len() > 2 && self.cpu[1].0 <= now - IDLE_SECONDS + EPSILON {
            self.cpu.pop_front();
        }
    }
}

/// `usage_usec` of a cgroup's `cpu.stat`.
pub fn cpu_usage_usec(path: &Path) -> std::io::Result<u64> {
    let text = std::fs::read_to_string(path)?;
    text.lines()
        .find_map(|line| line.strip_prefix("usage_usec "))
        .and_then(|value| value.split_whitespace().next()?.parse().ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "cgroup cpu.stat has no usage_usec"))
}
