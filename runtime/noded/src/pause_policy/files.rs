//! The two small files that cross the process boundary in phase 3a (spec
//! §6.3), both under `<state_root>/noded/` (0700), replaced by atomic rename:
//!
//! - `status.json`, written here every tick (250 ms) for the agent's heartbeat:
//!   ```json
//!   {"seq": 41,
//!    "pause_stats": {"pauses": 0, "thaws": 0, ... the 17 PauseStats names},
//!    "paused_sandboxes": 3,
//!    "resident_wait_extra": {"reason": "resident_headroom", "reclaim_target_bytes": 0}}
//!   ```
//!   Every number is a non-negative integer under today's heartbeat name and
//!   `reason` is one of the seven `resident_wait.reason` values, because the
//!   gateway voids a heartbeat with any other. `seq` and the counters continue
//!   from the previous file across a daemon restart, so they stay monotone.
//! - `agent-demand.json`, written by the agent (from `_admission_changed`, at
//!   least every 250 ms) and read here as the reclaim tick's demand:
//!   `{"seq": 7, "admission_open": true, "physical_bytes": 0, "ram_backing_bytes": 0}`.
//!   A missing, malformed or stale one (older than `DEMAND_MAX_AGE_SECONDS` by
//!   its mtime, or the same `seq` for that long) is unknown: zero demand, so
//!   only measured pressure acts, as with closed admission.

use std::io::Write;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value, json};

use super::Clock;
use super::decision::{MemoryDemand, REASONS};
use crate::fsutil::{ensure_private_dir, read_owned_file};
use crate::pause::Counter;

pub const DIRECTORY: &str = "noded";
pub const STATUS_FILE: &str = "status.json";
pub const DEMAND_FILE: &str = "agent-demand.json";
/// The agent writes at least every 250 ms; four missed writes are stale.
pub const DEMAND_MAX_AGE_SECONDS: f64 = 1.0;
const MAX_FILE_BYTES: u64 = 64 * 1024;

/// What one status write reports.
#[derive(Clone, Debug, PartialEq)]
pub struct Status {
    /// `PauseStats::snapshot()` of this process.
    pub pause_stats: Map<String, Value>,
    /// The markers the last tick found.
    pub paused_sandboxes: u64,
    /// The last tick's `decide_resident_wait` (no demand-free hysteresis).
    pub reason: &'static str,
    pub reclaim_target_bytes: u64,
}

/// The 17 counters, exactly, each a non-negative integer.
fn valid_stats(stats: &Value) -> Option<Vec<u64>> {
    let stats = stats.as_object()?;
    if stats.len() != Counter::ALL.len() {
        return None;
    }
    Counter::ALL.iter().map(|counter| stats.get(counter.name())?.as_u64()).collect()
}

/// `<state_root>/noded/status.json`, owned by this daemon.
pub struct StatusFile {
    directory: PathBuf,
    path: PathBuf,
    seq: u64,
    /// The counters the previous daemon reported; this one's add to them.
    baseline: Vec<u64>,
}

impl StatusFile {
    /// Continue `seq` and the counters from a previous valid file.
    pub fn open(state_root: &Path) -> StatusFile {
        let directory = state_root.join(DIRECTORY);
        let path = directory.join(STATUS_FILE);
        let previous = read_owned_file(&path, MAX_FILE_BYTES).ok().flatten().and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
        let seq = previous.as_ref().and_then(|value| value.get("seq")?.as_u64()).unwrap_or(0);
        let baseline = previous.as_ref().and_then(|value| valid_stats(value.get("pause_stats")?)).unwrap_or_else(|| vec![0; Counter::ALL.len()]);
        StatusFile { directory, path, seq, baseline }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The document for `status` (advancing `seq`).
    pub fn render(&mut self, status: &Status) -> Value {
        self.seq += 1;
        let stats: Map<String, Value> = Counter::ALL
            .iter()
            .zip(&self.baseline)
            .map(|(counter, base)| {
                let live = status.pause_stats.get(counter.name()).and_then(Value::as_u64).unwrap_or(0);
                let value = if counter.name().ends_with("_max") { live.max(*base) } else { live.saturating_add(*base) };
                (counter.name().to_string(), Value::from(value))
            })
            .collect();
        let reason = if REASONS.contains(&status.reason) { status.reason } else { "resident_headroom" };
        json!({
            "seq": self.seq,
            "pause_stats": stats,
            "paused_sandboxes": status.paused_sandboxes,
            "resident_wait_extra": {"reason": reason, "reclaim_target_bytes": status.reclaim_target_bytes},
        })
    }

    /// Render and replace the file atomically (no fsync: a crash leaves the
    /// previous or no file, which a reader treats as stale or unknown).
    pub fn write(&mut self, status: &Status) -> std::io::Result<Value> {
        let document = self.render(status);
        ensure_private_dir(&self.directory)?;
        let temporary = self.directory.join(format!(".{STATUS_FILE}.{}.tmp", std::process::id()));
        let result = (|| {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
                .open(&temporary)?;
            file.write_all(&serde_json::to_vec(&document).map_err(std::io::Error::other)?)?;
            drop(file);
            std::fs::rename(&temporary, &self.path)
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        result.map(|()| document)
    }
}

/// One valid `agent-demand.json`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AgentDemand {
    pub seq: u64,
    pub admission_open: bool,
    pub physical_bytes: u64,
    pub ram_backing_bytes: u64,
}

impl AgentDemand {
    pub fn parse(bytes: &[u8]) -> Option<AgentDemand> {
        let value: Value = serde_json::from_slice(bytes).ok()?;
        let object = value.as_object()?;
        Some(AgentDemand {
            seq: object.get("seq")?.as_u64()?,
            admission_open: object.get("admission_open")?.as_bool()?,
            physical_bytes: object.get("physical_bytes")?.as_u64()?,
            ram_backing_bytes: object.get("ram_backing_bytes")?.as_u64()?,
        })
    }
}

/// The reader of `<state_root>/noded/agent-demand.json`.
pub struct DemandFile {
    path: PathBuf,
    clock: Arc<dyn Clock>,
    /// The last seq seen, and when (monotonic) it was first seen.
    seen: Mutex<Option<(u64, f64)>>,
}

impl DemandFile {
    pub fn new(state_root: &Path, clock: Arc<dyn Clock>) -> DemandFile {
        DemandFile { path: state_root.join(DIRECTORY).join(DEMAND_FILE), clock, seen: Mutex::new(None) }
    }

    /// The current demand, or `None` when unknown.
    pub fn read(&self) -> Option<AgentDemand> {
        let meta = std::fs::symlink_metadata(&self.path).ok()?;
        let written = meta.mtime() as f64 + meta.mtime_nsec() as f64 / 1e9;
        let fresh = |age: f64| age <= DEMAND_MAX_AGE_SECONDS;
        if !fresh(self.clock.wall() - written) {
            return None;
        }
        let demand = AgentDemand::parse(&read_owned_file(&self.path, MAX_FILE_BYTES).ok()??)?;
        let now = self.clock.monotonic();
        let mut seen = self.seen.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let since = match *seen {
            Some((seq, since)) if seq == demand.seq => since,
            _ => now,
        };
        *seen = Some((demand.seq, since));
        fresh(now - since).then_some(demand)
    }
}

/// The reclaim tick's demand input.
pub trait DemandSource: Send + Sync {
    fn demand(&self) -> MemoryDemand;
}

/// Phase 3a's demand: the agent's ledger through `agent-demand.json`, billed
/// only while both the drain row and the agent say admission is open. Closed
/// admission (drain) bills nothing, so paused waits are never swapped out or
/// hibernated for a drain.
pub struct AgentDemandSource {
    pub file: DemandFile,
    pub admission_open: Box<dyn Fn() -> bool + Send + Sync>,
}

impl DemandSource for AgentDemandSource {
    fn demand(&self) -> MemoryDemand {
        if !(self.admission_open)() {
            return MemoryDemand::default();
        }
        match self.file.read() {
            Some(demand) if demand.admission_open => MemoryDemand { physical_bytes: demand.physical_bytes, ram_backing_bytes: demand.ram_backing_bytes },
            _ => MemoryDemand::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pause::PauseStats;
    use crate::pause::tests::TempDir;
    use crate::pause_policy::tests::FakeClock;
    use std::os::unix::fs::PermissionsExt;

    fn status(stats: &PauseStats, paused: u64) -> Status {
        Status { pause_stats: stats.snapshot(), paused_sandboxes: paused, reason: "memory_headroom", reclaim_target_bytes: 7 }
    }

    #[test]
    fn status_has_the_exact_schema_and_continues_across_restarts() {
        let dir = TempDir::new("status");
        let stats = PauseStats::new();
        stats.add(&[(Counter::Pauses, 2.0), (Counter::ThawMsMax, 30.0), (Counter::PauseReclaimMsTotal, 2.5)]);
        let mut file = StatusFile::open(&dir.0);
        let written = file.write(&status(&stats, 3)).unwrap();
        let read: Value = serde_json::from_slice(&std::fs::read(file.path()).unwrap()).unwrap();
        assert_eq!(read, written);
        let keys: Vec<&String> = read.as_object().unwrap().keys().collect();
        assert_eq!(keys, ["seq", "pause_stats", "paused_sandboxes", "resident_wait_extra"]);
        assert_eq!(read["seq"], 1);
        assert_eq!(read["paused_sandboxes"], 3);
        assert_eq!(read["resident_wait_extra"], json!({"reason": "memory_headroom", "reclaim_target_bytes": 7}));
        let names: Vec<&str> = read["pause_stats"].as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(names, Counter::ALL.iter().map(|counter| counter.name()).collect::<Vec<_>>());
        assert_eq!((&read["pause_stats"]["pauses"], &read["pause_stats"]["pause_reclaim_ms_total"]), (&json!(2), &json!(2)));
        assert_eq!(std::fs::metadata(dir.0.join("noded")).unwrap().permissions().mode() & 0o777, 0o700);
        assert_eq!(std::fs::metadata(file.path()).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(file.write(&status(&stats, 3)).unwrap()["seq"], 2);

        // A restarted daemon starts its counters at zero; the file does not.
        let restarted = PauseStats::new();
        restarted.add(&[(Counter::Pauses, 1.0), (Counter::ThawMsMax, 10.0)]);
        let mut file = StatusFile::open(&dir.0);
        let read = file.write(&status(&restarted, 0)).unwrap();
        assert_eq!((&read["seq"], &read["pause_stats"]["pauses"], &read["pause_stats"]["thaw_ms_max"]), (&json!(3), &json!(3), &json!(30)));
        // Only the gateway's reason enum is ever written.
        let odd = Status { reason: "unknown", ..status(&restarted, 0) };
        assert_eq!(file.render(&odd)["resident_wait_extra"]["reason"], "resident_headroom");
        // An invalid previous file is no baseline.
        std::fs::write(file.path(), br#"{"seq": 9, "pause_stats": {"pauses": -1}}"#).unwrap();
        let read = StatusFile::open(&dir.0).render(&status(&restarted, 0));
        assert_eq!((&read["seq"], &read["pause_stats"]["pauses"]), (&json!(10), &json!(1)));
        let leftovers: Vec<_> = std::fs::read_dir(dir.0.join("noded")).unwrap().filter_map(|entry| entry.ok()).filter(|entry| entry.file_name().to_string_lossy().starts_with('.')).collect();
        assert!(leftovers.is_empty());
    }

    fn write_demand(dir: &TempDir, text: &str) {
        let directory = dir.0.join(DIRECTORY);
        ensure_private_dir(&directory).unwrap();
        std::fs::write(directory.join(DEMAND_FILE), text).unwrap();
        std::fs::set_permissions(directory.join(DEMAND_FILE), std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    fn wall_now() -> f64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64()
    }

    #[test]
    fn agent_demand_is_read_only_while_fresh_valid_and_open() {
        let dir = TempDir::new("demand");
        let clock = FakeClock::new();
        clock.set_wall(wall_now());
        let open = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let flag = open.clone();
        let source = AgentDemandSource { file: DemandFile::new(&dir.0, clock.clone()), admission_open: Box::new(move || flag.load(std::sync::atomic::Ordering::SeqCst)) };
        let none = MemoryDemand::default();
        assert_eq!(source.demand(), none); // No file: unknown.
        write_demand(&dir, r#"{"seq": 1, "admission_open": true, "physical_bytes": 4096, "ram_backing_bytes": 1024}"#);
        assert_eq!(source.demand(), MemoryDemand { physical_bytes: 4096, ram_backing_bytes: 1024 });
        clock.advance(0.9);
        assert_eq!(source.demand().physical_bytes, 4096);
        // The same seq for longer than the bound is a stalled writer, even
        // with a fresh mtime.
        clock.advance(0.2);
        clock.set_wall(wall_now());
        write_demand(&dir, r#"{"seq": 1, "admission_open": true, "physical_bytes": 4096, "ram_backing_bytes": 1024}"#);
        assert_eq!(source.demand(), none);
        write_demand(&dir, r#"{"seq": 2, "admission_open": true, "physical_bytes": 1, "ram_backing_bytes": 0}"#);
        assert_eq!(source.demand().physical_bytes, 1);
        // An old mtime is stale too.
        clock.set_wall(wall_now() + 5.0);
        assert_eq!(source.demand(), none);
        clock.set_wall(wall_now());
        // Closed admission bills nothing, from either side.
        open.store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(source.demand(), none);
        open.store(true, std::sync::atomic::Ordering::SeqCst);
        write_demand(&dir, r#"{"seq": 3, "admission_open": false, "physical_bytes": 9223372036854775808, "ram_backing_bytes": 9223372036854775808}"#);
        assert_eq!(source.demand(), none);
        for invalid in [
            r#"{"seq": 4, "admission_open": true, "physical_bytes": -1, "ram_backing_bytes": 0}"#,
            r#"{"seq": 4, "admission_open": 1, "physical_bytes": 1, "ram_backing_bytes": 0}"#,
            r#"{"seq": 4, "admission_open": true, "physical_bytes": 1.5, "ram_backing_bytes": 0}"#,
            r#"{"seq": 4, "admission_open": true, "physical_bytes": 1}"#,
            "[]",
            "not json",
        ] {
            write_demand(&dir, invalid);
            assert_eq!(source.demand(), none, "{invalid}");
        }
        // A file others may write is never trusted.
        write_demand(&dir, r#"{"seq": 5, "admission_open": true, "physical_bytes": 1, "ram_backing_bytes": 0}"#);
        std::fs::set_permissions(dir.0.join(DIRECTORY).join(DEMAND_FILE), std::fs::Permissions::from_mode(0o666)).unwrap();
        assert_eq!(source.demand(), none);
    }
}
