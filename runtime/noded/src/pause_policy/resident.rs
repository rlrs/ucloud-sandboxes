//! `resident_memory`: cached, incarnation-fenced cgroup memory observations
//! (`ResidentMemorySampler`) and the windowed `memory.reclaim` writer
//! (`ResidentMemoryReclaimer`). In phase 3a the daemon samples paused
//! sandboxes only; the agent keeps sampling managed ones.
//!
//! A sample proves its cgroup belongs to this incarnation: the sentry's start
//! ticks before and after, its unique unified cgroup (the OCI config's path,
//! or one named by the container id), contained in the cgroup root, and the
//! same directory inode throughout. Unknown or stale data stays unknown.

use std::collections::HashMap;
use std::ffi::CString;
use std::fs::File;
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::Clock;
use super::budget::ReclaimBudget;

/// `max_age_seconds`: a live sample is at most this old.
pub const SAMPLE_MAX_AGE_SECONDS: f64 = 2.5;

/// `ResidentMemorySample`.
#[derive(Clone, Debug, PartialEq)]
pub struct ResidentSample {
    pub current_bytes: u64,
    pub anonymous_bytes: u64,
    pub file_bytes: u64,
    pub dirty_bytes: u64,
    pub writeback_bytes: u64,
    pub refault_file_pages: u64,
    /// The unified cgroup path as `/proc/<pid>/cgroup` names it (leading `/`).
    pub cgroup_path: String,
    pub cgroup_device: u64,
    pub cgroup_inode: u64,
    pub sentry_pid: u32,
    pub sentry_start_time_ticks: u64,
    /// Monotonic seconds.
    pub sampled_at: f64,
    pub shared_memory_bytes: u64,
    /// memory.peak (0 when the kernel lacks it).
    pub peak_bytes: u64,
    /// memory.swap.current: a paused runtime's pages moved out. Not
    /// resident, but its footprint: a thaw or capture faults them in.
    pub swap_bytes: u64,
}

impl ResidentSample {
    pub fn clean_file_bytes(&self) -> u64 {
        let clean = self.file_bytes as i128 - self.shared_memory_bytes as i128 - self.dirty_bytes as i128 - self.writeback_bytes as i128;
        clean.min(self.current_bytes as i128).max(0) as u64
    }
}

pub type Key = (String, u64);

/// `ResidentMemorySampler`.
pub struct ResidentSampler {
    pub cgroup_root: PathBuf,
    pub proc_root: PathBuf,
    clock: Arc<dyn Clock>,
    state: Mutex<SamplerState>,
}

#[derive(Default)]
struct SamplerState {
    samples: HashMap<Key, ResidentSample>,
    /// (cgroup path, dev, inode) whose containment canonicalize already proved.
    contained: HashMap<Key, (String, u64, u64)>,
}

fn parse_i64(path: &Path) -> Option<i64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// A counter file that may be absent (`FileNotFoundError` → 0 in Python);
/// any other failure voids the sample.
fn optional_counter(path: &Path) -> Result<i64, ()> {
    match std::fs::read_to_string(path) {
        Ok(text) => text.trim().parse().map_err(|_| ()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(_) => Err(()),
    }
}

impl ResidentSampler {
    pub fn new(cgroup_root: PathBuf, proc_root: PathBuf, clock: Arc<dyn Clock>) -> Self {
        ResidentSampler { cgroup_root, proc_root, clock, state: Mutex::new(SamplerState::default()) }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, SamplerState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// A live sample (at most SAMPLE_MAX_AGE_SECONDS old), or `None`.
    pub fn get(&self, key: &Key) -> Option<ResidentSample> {
        let sample = self.state().samples.get(key).cloned()?;
        (self.clock.monotonic() - sample.sampled_at <= SAMPLE_MAX_AGE_SECONDS).then_some(sample)
    }

    /// Drop every incarnation but these.
    pub fn retain(&self, keys: &std::collections::HashSet<Key>) {
        let mut state = self.state();
        state.samples.retain(|key, _| keys.contains(key));
        state.contained.retain(|key, _| keys.contains(key));
    }

    pub fn forget(&self, key: &Key) {
        let mut state = self.state();
        state.samples.remove(key);
        state.contained.remove(key);
    }

    #[cfg(test)]
    pub(crate) fn insert_for_tests(&self, key: &Key, current: u64, swap: u64, sampled_at: f64) {
        let sample = ResidentSample {
            current_bytes: current,
            anonymous_bytes: 0,
            file_bytes: 0,
            dirty_bytes: 0,
            writeback_bytes: 0,
            refault_file_pages: 0,
            cgroup_path: format!("/test/{}", key.0),
            cgroup_device: 0,
            cgroup_inode: 0,
            sentry_pid: 1,
            sentry_start_time_ticks: 1,
            sampled_at,
            shared_memory_bytes: 0,
            peak_bytes: 0,
            swap_bytes: swap,
        };
        self.state().samples.insert(key.clone(), sample);
    }

    /// Sample one incarnation's cgroup; `None` (and the cache entry dropped)
    /// on any doubt.
    pub fn sample(&self, key: &Key, pid: u32, start_time_ticks: u64, container_id: &str, expected_path: Option<&str>) -> Option<ResidentSample> {
        let result = self.measure(key, pid, start_time_ticks, container_id, expected_path);
        let mut state = self.state();
        match result {
            Some((sample, proof)) => {
                state.samples.insert(key.clone(), sample.clone());
                state.contained.insert(key.clone(), proof);
                Some(sample)
            }
            None => {
                state.samples.remove(key);
                state.contained.remove(key);
                None
            }
        }
    }

    fn measure(&self, key: &Key, pid: u32, ticks: u64, container_id: &str, expected_path: Option<&str>) -> Option<(ResidentSample, (String, u64, u64))> {
        let same_sentry = || crate::runsc::start_time_ticks(&self.proc_root, pid).is_ok_and(|now| now == ticks);
        if !same_sentry() {
            return None;
        }
        let memberships = std::fs::read_to_string(self.proc_root.join(pid.to_string()).join("cgroup")).ok()?;
        let unified: Vec<&str> = memberships.lines().filter_map(|line| line.strip_prefix("0::")).collect();
        let [raw] = unified[..] else { return None };
        let relative = raw.strip_prefix('/')?;
        let parts: Vec<&str> = relative.split('/').filter(|part| !part.is_empty()).collect();
        if parts.is_empty() || raw.split('/').skip(1).any(|part| part == "." || part == "..") {
            return None;
        }
        match expected_path {
            Some(expected) if raw != expected => return None,
            None if parts.last() != Some(&container_id) => return None, // Never a shared parent.
            _ => {}
        }
        let path = self.cgroup_root.join(relative);
        let mut identity = std::fs::metadata(&path).ok()?;
        let proof = self.state().contained.get(key).cloned();
        if proof != Some((raw.to_string(), identity.dev(), identity.ino())) || !identity.is_dir() {
            if std::fs::canonicalize(&path).ok()? != path || !path.is_dir() {
                return None; // Escaped the trusted root.
            }
            identity = std::fs::metadata(&path).ok()?;
        }
        let current = parse_i64(&path.join("memory.current"))?;
        let peak = optional_counter(&path.join("memory.peak")).ok()?;
        let swap = optional_counter(&path.join("memory.swap.current")).ok()?;
        let mut counters: HashMap<String, i64> = HashMap::new();
        for line in std::fs::read_to_string(path.join("memory.stat")).ok()?.lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let [name, value] = fields[..] else { return None };
            counters.insert(name.to_string(), value.parse().ok()?);
        }
        let required = ["anon", "file", "file_dirty", "file_writeback", "workingset_refault_file"];
        let mut values = [0u64; 5];
        for (slot, name) in values.iter_mut().zip(required) {
            *slot = u64::try_from(*counters.get(name)?).ok()?;
        }
        let shmem = u64::try_from(*counters.get("shmem")?).ok()?;
        let current = u64::try_from(current).ok()?;
        if !same_sentry() {
            return None;
        }
        let after = std::fs::metadata(&path).ok()?;
        if (identity.dev(), identity.ino()) != (after.dev(), after.ino()) {
            return None; // Replaced while sampling.
        }
        let peak = u64::try_from(peak.max(0)).unwrap_or(0);
        let sample = ResidentSample {
            current_bytes: current,
            anonymous_bytes: values[0],
            file_bytes: values[1],
            dirty_bytes: values[2],
            writeback_bytes: values[3],
            refault_file_pages: values[4],
            cgroup_path: raw.to_string(),
            cgroup_device: identity.dev(),
            cgroup_inode: identity.ino(),
            sentry_pid: pid,
            sentry_start_time_ticks: ticks,
            sampled_at: self.clock.monotonic(),
            shared_memory_bytes: shmem,
            peak_bytes: if peak != 0 { peak.max(current) } else { 0 },
            swap_bytes: u64::try_from(swap.max(0)).unwrap_or(0),
        };
        Some((sample, (raw.to_string(), identity.dev(), identity.ino())))
    }
}

/// `ResidentReclaimResult`.
#[derive(Clone, Debug, PartialEq)]
pub struct ReclaimResult {
    pub requested_bytes: u64,
    pub reclaimed_bytes: u64,
    pub elapsed_seconds: f64,
    pub refault_file_pages: u64,
    /// target_reached, superseded, not_shrinking, partial_reclaim,
    /// no_reclaimable_cache, cgroup_changed, short_write, unsupported or kernel_error.
    pub reason: &'static str,
}

/// The `memory.reclaim` write (a seam for tests).
pub type Writer = Arc<dyn Fn(&File, &[u8]) -> std::io::Result<usize> + Send + Sync>;

pub fn kernel_write() -> Writer {
    Arc::new(|mut file: &File, payload: &[u8]| file.write(payload))
}

/// `ResidentMemoryReclaimer`: best-effort reclaim against one already fenced
/// live incarnation. swappiness 0 evicts clean file cache only; a paused
/// runtime passes 200 to move its tmpfs and anonymous pages to swap, paced by
/// the node's budget and stopped once the cgroup no longer shrinks. A thaw
/// cancels later windows; one already in the kernel may finish after it.
pub struct Reclaimer<'a> {
    pub sampler: &'a ResidentSampler,
    pub write: Writer,
}

/// Open `name` in the directory `directory` without following a link.
fn open_at(directory: &File, name: &str, flags: libc::c_int) -> std::io::Result<File> {
    let name = CString::new(name).map_err(std::io::Error::other)?;
    // SAFETY: a valid directory descriptor and a NUL-terminated name.
    let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags | libc::O_CLOEXEC, 0 as libc::c_uint) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: a descriptor this call just opened and nothing else owns.
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[derive(Clone, Copy)]
pub struct ReclaimRequest {
    pub target_bytes: u64,
    pub window_bytes: u64,
    pub swappiness: u32,
}

impl Reclaimer<'_> {
    pub fn reclaim(&self, key: &Key, sample: &ResidentSample, request: ReclaimRequest, is_current: &dyn Fn() -> bool, budget: Option<&ReclaimBudget>) -> ReclaimResult {
        let ReclaimRequest { target_bytes, window_bytes, swappiness } = request;
        assert!(target_bytes > 0 && window_bytes > 0 && swappiness <= 200, "reclaim byte budgets must be positive");
        let clock = &self.sampler.clock;
        let reclaimable = |observed: &ResidentSample| if swappiness != 0 { observed.current_bytes } else { observed.clean_file_bytes() };
        let started = clock.monotonic();
        let target = target_bytes.min(reclaimable(sample));
        if target == 0 {
            return ReclaimResult { requested_bytes: 0, reclaimed_bytes: 0, elapsed_seconds: 0.0, refault_file_pages: 0, reason: "no_reclaimable_cache" };
        }
        let relative = sample.cgroup_path.trim_start_matches('/');
        let path = self.sampler.cgroup_root.join(relative);
        let container_id = Path::new(relative).file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default();
        let resample = || self.sampler.sample(key, sample.sentry_pid, sample.sentry_start_time_ticks, &container_id, Some(&sample.cgroup_path));
        let same_cgroup = |observed: &ResidentSample| (observed.cgroup_device, observed.cgroup_inode) == (sample.cgroup_device, sample.cgroup_inode);
        let (mut requested, mut current, mut reason) = (0u64, sample.clone(), "target_reached");
        let outcome = (|| -> std::io::Result<()> {
            let directory = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&path)?;
            let identity = directory.metadata()?;
            if (identity.dev(), identity.ino()) != (sample.cgroup_device, sample.cgroup_inode) {
                reason = "cgroup_changed";
                return Ok(());
            }
            let control = open_at(&directory, "memory.reclaim", libc::O_WRONLY | libc::O_NOFOLLOW)?;
            while requested < target {
                if !is_current() {
                    reason = "superseded";
                    break;
                }
                let Some(observed) = resample().filter(same_cgroup) else {
                    reason = "cgroup_changed";
                    break;
                };
                if swappiness != 0 && requested != 0 && (current.current_bytes as i128 - observed.current_bytes as i128) < i128::from(window_bytes / 16) {
                    reason = "not_shrinking";
                    break;
                }
                current = observed;
                if sample.current_bytes as i128 - current.current_bytes as i128 >= i128::from(target) {
                    break;
                }
                let amount = window_bytes.min(target - requested).min(reclaimable(&current));
                if amount == 0 {
                    reason = "no_reclaimable_cache";
                    break;
                }
                if budget.is_some_and(|budget| !budget.admit(amount, is_current)) || !is_current() {
                    reason = "superseded";
                    break;
                }
                let payload = format!("{amount} swappiness={swappiness}");
                requested += amount;
                let written = match budget {
                    Some(budget) => budget.background(|| (self.write)(&control, payload.as_bytes())),
                    None => (self.write)(&control, payload.as_bytes()),
                };
                match written {
                    Ok(count) if count == payload.len() => {}
                    Ok(_) => {
                        reason = "short_write";
                        break;
                    }
                    Err(error) => match error.raw_os_error() {
                        Some(libc::EAGAIN) => reason = "partial_reclaim",
                        Some(libc::EINVAL | libc::ENOENT | libc::EOPNOTSUPP) => {
                            reason = "unsupported";
                            break;
                        }
                        _ => {
                            reason = "kernel_error";
                            break;
                        }
                    },
                }
            }
            if let Some(observed) = resample().filter(same_cgroup) {
                current = observed;
            }
            Ok(())
        })();
        if let Err(error) = outcome {
            reason = match error.raw_os_error() {
                Some(libc::ENOENT | libc::EOPNOTSUPP | libc::EACCES) => "unsupported",
                _ => "kernel_error",
            };
        }
        ReclaimResult {
            requested_bytes: requested,
            reclaimed_bytes: sample.current_bytes.saturating_sub(current.current_bytes),
            elapsed_seconds: clock.monotonic() - started,
            refault_file_pages: current.refault_file_pages.saturating_sub(sample.refault_file_pages),
            reason,
        }
    }
}

/// The cgroup path an OCI bundle's config names (`linux.cgroupsPath`):
/// `Ok(None)` when it names none, `Err` when unreadable or not a string.
pub fn bundle_cgroup_path(bundle: &Path) -> Result<Option<String>, String> {
    let text = std::fs::read(bundle.join("config.json")).map_err(|error| error.to_string())?;
    let config: serde_json::Value = serde_json::from_slice(&text).map_err(|error| error.to_string())?;
    match config.get("linux").and_then(|linux| linux.get("cgroupsPath")) {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(path)) => Ok(Some(path.clone())),
        Some(_) => Err("linux.cgroupsPath is not a string".into()),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::pause::tests::TempDir;
    use crate::pause_policy::budget::tests::FakeNice;
    use crate::pause_policy::tests::FakeClock;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// tests/test_resident_memory.py's fake cgroup tree: sentry 123 started
    /// at tick 456, in `/ucloud-sandboxes/<64 a>`.
    pub(crate) struct Tree {
        pub dir: TempDir,
        pub proc: PathBuf,
        pub container: String,
        pub path: PathBuf,
        pub clock: Arc<FakeClock>,
        pub sampler: ResidentSampler,
    }

    impl Tree {
        pub fn new() -> Tree {
            let dir = TempDir::new("resident");
            let root = std::fs::canonicalize(&dir.0).unwrap();
            let (proc, cgroup) = (root.join("proc"), root.join("cgroup"));
            let container = "a".repeat(64);
            let path = cgroup.join("ucloud-sandboxes").join(&container);
            std::fs::create_dir_all(&path).unwrap();
            std::fs::create_dir_all(proc.join("123")).unwrap();
            let clock = FakeClock::new();
            let sampler = ResidentSampler::new(cgroup.clone(), proc.clone(), clock.clone());
            let tree = Tree { dir, proc, container: container.clone(), path: path.clone(), clock, sampler };
            tree.set_ticks(456);
            std::fs::write(tree.proc.join("123/cgroup"), format!("0::/ucloud-sandboxes/{container}")).unwrap();
            std::fs::write(path.join("memory.current"), "1000").unwrap();
            std::fs::write(path.join("memory.stat"), "shmem 0\nanon 100\nfile 800\nfile_dirty 50\nfile_writeback 20\nworkingset_refault_file 7\n").unwrap();
            tree
        }

        pub fn set_ticks(&self, ticks: u64) {
            std::fs::write(self.proc.join("123/stat"), format!("123 (sentry) S {}{ticks} 0", "0 ".repeat(18))).unwrap();
        }

        pub fn sample(&self, expected: Option<&str>) -> Option<ResidentSample> {
            self.sampler.sample(&("s".into(), 1), 123, 456, &self.container, expected)
        }

        /// `_anonymous(current)`: all of it tmpfs.
        pub fn anonymous(&self, current: u64) {
            std::fs::write(self.path.join("memory.current"), current.to_string()).unwrap();
            std::fs::write(
                self.path.join("memory.stat"),
                format!("shmem {current}\nanon 0\nfile {current}\nfile_dirty 0\nfile_writeback 0\nworkingset_refault_file 0\n"),
            )
            .unwrap();
        }

        pub fn reclaimer(&self, write: Writer) -> Reclaimer<'_> {
            Reclaimer { sampler: &self.sampler, write }
        }
    }

    fn key() -> Key {
        ("s".into(), 1)
    }

    fn request(target: u64, window: u64, swappiness: u32) -> ReclaimRequest {
        ReclaimRequest { target_bytes: target, window_bytes: window, swappiness }
    }

    #[test]
    fn samples_actual_charge_and_cost_counters_without_process_rss() {
        let tree = Tree::new();
        let expected = format!("/ucloud-sandboxes/{}", tree.container);
        let sample = tree.sample(Some(&expected)).unwrap();
        assert_eq!((sample.current_bytes, sample.clean_file_bytes(), sample.refault_file_pages), (1000, 730, 7));
        assert_eq!((sample.peak_bytes, sample.swap_bytes), (0, 0));
        assert_eq!(tree.sampler.get(&key()), Some(sample));
        tree.clock.advance(3.0);
        assert_eq!(tree.sampler.get(&key()), None);
        std::fs::write(tree.path.join("memory.peak"), "900").unwrap();
        std::fs::write(tree.path.join("memory.swap.current"), "400").unwrap();
        let sample = tree.sample(None).unwrap();
        assert_eq!((sample.peak_bytes, sample.swap_bytes), (1000, 400)); // Peak is at least current.
    }

    #[test]
    fn a_replaced_or_escaping_cgroup_is_proved_again() {
        let tree = Tree::new();
        let expected = format!("/ucloud-sandboxes/{}", tree.container);
        for _ in 0..3 {
            assert!(tree.sample(Some(&expected)).is_some());
        }
        let moved = tree.path.with_file_name("old");
        std::fs::rename(&tree.path, &moved).unwrap();
        std::fs::create_dir(&tree.path).unwrap();
        for name in ["memory.current", "memory.stat"] {
            std::fs::copy(moved.join(name), tree.path.join(name)).unwrap();
        }
        let replaced = tree.sample(Some(&expected)).unwrap();
        assert_eq!(replaced.cgroup_inode, std::fs::metadata(&tree.path).unwrap().ino());
        std::fs::remove_dir_all(&tree.path).unwrap();
        std::os::unix::fs::symlink(&tree.dir.0, &tree.path).unwrap();
        assert!(tree.sample(Some(&expected)).is_none());
    }

    #[test]
    fn pid_reuse_invalidates_observation_and_no_configured_limit_fallback() {
        let tree = Tree::new();
        assert!(tree.sample(None).is_some());
        tree.set_ticks(999);
        assert!(tree.sample(None).is_none());
        assert!(tree.sampler.get(&key()).is_none());
    }

    #[test]
    fn wrong_cgroup_and_incomplete_counters_stay_unknown() {
        let tree = Tree::new();
        assert!(tree.sample(Some("/another/runtime")).is_none());
        std::fs::write(tree.path.join("memory.stat"), "shmem 0\nanon 100\nfile 800\n").unwrap();
        assert!(tree.sample(None).is_none());
        std::fs::write(tree.path.join("memory.stat"), "shmem 0\nanon -1\nfile 800\nfile_dirty 50\nfile_writeback 20\nworkingset_refault_file 7\n").unwrap();
        assert!(tree.sample(None).is_none()); // Negative accounting.
        tree.sampler.retain(&Default::default());
        assert!(tree.sampler.get(&key()).is_none());
    }

    #[test]
    fn shared_parent_cgroup_cannot_be_misattributed_to_incarnation() {
        let tree = Tree::new();
        std::fs::write(tree.proc.join("123/cgroup"), "0::/ucloud-sandboxes").unwrap();
        assert!(tree.sample(None).is_none());
        std::fs::write(tree.proc.join("123/cgroup"), format!("0::/ucloud-sandboxes/../{}", tree.container)).unwrap();
        assert!(tree.sample(None).is_none());
        std::fs::write(tree.proc.join("123/cgroup"), format!("0::/x/{c}\n0::/y/{c}\n", c = tree.container)).unwrap();
        assert!(tree.sample(None).is_none());
    }

    #[test]
    fn tmpfs_pages_are_not_counted_as_reclaimable_without_swap() {
        let tree = Tree::new();
        std::fs::write(tree.path.join("memory.stat"), "shmem 800\nanon 100\nfile 800\nfile_dirty 50\nfile_writeback 20\nworkingset_refault_file 7\n").unwrap();
        let sample = tree.sample(None).unwrap();
        assert_eq!(sample.clean_file_bytes(), 0);
        let report = tree.reclaimer(kernel_write()).reclaim(&key(), &sample, request(900, 16 << 20, 0), &|| true, None);
        assert_eq!((report.reason, report.requested_bytes), ("no_reclaimable_cache", 0));
    }

    #[test]
    fn reclaim_windows_report_actual_progress_and_cancel_on_wake() {
        let tree = Tree::new();
        std::fs::write(tree.path.join("memory.reclaim"), "").unwrap();
        let sample = tree.sample(None).unwrap();
        let alive = Arc::new(AtomicBool::new(true));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (path, flag, seen) = (tree.path.clone(), alive.clone(), calls.clone());
        let write: Writer = Arc::new(move |_, data| {
            seen.lock().unwrap().push(String::from_utf8(data.to_vec()).unwrap());
            std::fs::write(path.join("memory.current"), "900").unwrap();
            flag.store(false, Ordering::SeqCst);
            Ok(data.len())
        });
        let report = tree.reclaimer(write).reclaim(&key(), &sample, request(700, 200, 0), &|| alive.load(Ordering::SeqCst), None);
        assert_eq!(*calls.lock().unwrap(), ["200 swappiness=0"]);
        assert_eq!((report.requested_bytes, report.reclaimed_bytes, report.reason), (200, 100, "superseded"));
    }

    #[test]
    fn unsupported_reclaim_does_not_fall_back_to_anonymous_swap() {
        let tree = Tree::new();
        std::fs::write(tree.path.join("memory.reclaim"), "").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let write: Writer = Arc::new(move |_, _| {
            counted.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::from_raw_os_error(libc::EINVAL))
        });
        let sample = tree.sample(None).unwrap();
        let report = tree.reclaimer(write).reclaim(&key(), &sample, request(500, 16 << 20, 0), &|| true, None);
        assert_eq!((calls.load(Ordering::SeqCst), report.reason, report.reclaimed_bytes), (1, "unsupported", 0));
    }

    #[test]
    fn cgroup_replacement_cannot_receive_reclaim() {
        let tree = Tree::new();
        let sample = tree.sample(None).unwrap();
        std::fs::rename(&tree.path, tree.path.with_file_name("old")).unwrap();
        std::fs::create_dir(&tree.path).unwrap();
        std::fs::write(tree.path.join("memory.reclaim"), "").unwrap();
        let write: Writer = Arc::new(|_, _| panic!("a replaced cgroup received a reclaim"));
        let report = tree.reclaimer(write).reclaim(&key(), &sample, request(500, 16 << 20, 0), &|| true, None);
        assert_eq!(report.reason, "cgroup_changed");
        // No memory.reclaim at all: unsupported, before any window.
        let tree = Tree::new();
        let sample = tree.sample(None).unwrap();
        let report = tree.reclaimer(kernel_write()).reclaim(&key(), &sample, request(500, 16 << 20, 0), &|| true, None);
        assert_eq!((report.reason, report.requested_bytes), ("unsupported", 0));
    }

    /// A write that shrinks the cgroup by `step` each time, down to `floor`.
    fn shrinking(tree: &Tree, from: u64, step: u64, writes: Arc<Mutex<Vec<String>>>, floor_after: usize) -> Writer {
        let path = tree.path.clone();
        let level = Arc::new(Mutex::new(from));
        Arc::new(move |_, data| {
            let mut writes = writes.lock().unwrap();
            writes.push(String::from_utf8(data.to_vec()).unwrap());
            let mut level = level.lock().unwrap();
            if writes.len() <= floor_after {
                *level -= step;
            }
            let current = *level;
            std::fs::write(path.join("memory.current"), current.to_string()).unwrap();
            std::fs::write(path.join("memory.stat"), format!("shmem {current}\nanon 0\nfile {current}\nfile_dirty 0\nfile_writeback 0\nworkingset_refault_file 0\n")).unwrap();
            Ok(data.len())
        })
    }

    #[test]
    fn swappiness_reclaim_moves_tmpfs_until_the_cgroup_stops_shrinking() {
        let tree = Tree::new();
        std::fs::write(tree.path.join("memory.reclaim"), "").unwrap();
        tree.anonymous(1000);
        let sample = tree.sample(None).unwrap();
        let writes = Arc::new(Mutex::new(Vec::new()));
        let write = shrinking(&tree, 1000, 200, writes.clone(), 2); // Shrinks twice, then stalls.
        let report = tree.reclaimer(write).reclaim(&key(), &sample, request(1000, 200, 200), &|| true, None);
        assert_eq!(writes.lock().unwrap()[0], "200 swappiness=200");
        std::fs::write(tree.path.join("memory.swap.current"), "400").unwrap();
        assert_eq!(tree.sample(None).unwrap().swap_bytes, 400); // Footprint, not resident.
        assert_eq!((report.reason, report.reclaimed_bytes, writes.lock().unwrap().len()), ("not_shrinking", 400, 3));
    }

    #[test]
    fn thaw_mid_reclaim_cancels_the_next_window() {
        let tree = Tree::new();
        std::fs::write(tree.path.join("memory.reclaim"), "").unwrap();
        tree.anonymous(1000);
        let thawed = Arc::new(AtomicBool::new(false));
        let (path, flag) = (tree.path.clone(), thawed.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let write: Writer = Arc::new(move |_, data| {
            counted.fetch_add(1, Ordering::SeqCst);
            std::fs::write(path.join("memory.current"), "800").unwrap();
            flag.store(true, Ordering::SeqCst); // A thaw lands while this window is in the kernel.
            Ok(data.len())
        });
        let sample = tree.sample(None).unwrap();
        let report = tree.reclaimer(write).reclaim(&key(), &sample, request(1000, 200, 200), &|| !thawed.load(Ordering::SeqCst), None);
        assert_eq!((calls.load(Ordering::SeqCst), report.reason, report.reclaimed_bytes), (1, "superseded", 200));
    }

    #[test]
    fn node_budget_paces_windows() {
        let tree = Tree::new();
        std::fs::write(tree.path.join("memory.reclaim"), "").unwrap();
        tree.anonymous(1000);
        let advance = tree.clock.clone();
        let budget = ReclaimBudget::new(2, 3000.0, tree.clock.clone(), Arc::new(move |seconds| advance.advance(seconds)), FakeNice::new());
        let writes = Arc::new(Mutex::new(Vec::new()));
        let sample = tree.sample(None).unwrap();
        let started = tree.clock.monotonic();
        let report = tree.reclaimer(shrinking(&tree, 1000, 100, writes.clone(), usize::MAX)).reclaim(&key(), &sample, request(300, 100, 200), &|| true, Some(&budget));
        assert_eq!((writes.lock().unwrap().len(), report.reason), (3, "target_reached"));
        assert!(tree.clock.monotonic() - started >= 0.0666); // Windows at 0, 33, 67 ms.
        assert!((report.elapsed_seconds - (tree.clock.monotonic() - started)).abs() < 1e-9);
    }

    #[test]
    fn a_thaw_while_waiting_for_the_budget_cancels_before_any_write() {
        let tree = Tree::new();
        std::fs::write(tree.path.join("memory.reclaim"), "").unwrap();
        tree.anonymous(1000);
        let thawed = Arc::new(AtomicBool::new(false));
        let (advance, flag) = (tree.clock.clone(), thawed.clone());
        // Next window in a second; the thaw lands 0.1 s into the wait.
        let budget = ReclaimBudget::new(2, 100.0, tree.clock.clone(), Arc::new(move |seconds| {
            advance.advance(seconds);
            if advance.monotonic() >= 0.1 {
                flag.store(true, Ordering::SeqCst);
            }
        }), FakeNice::new());
        let writes = Arc::new(Mutex::new(Vec::new()));
        let sample = tree.sample(None).unwrap();
        let report = tree.reclaimer(shrinking(&tree, 1000, 100, writes.clone(), usize::MAX)).reclaim(&key(), &sample, request(300, 100, 200), &|| !thawed.load(Ordering::SeqCst), Some(&budget));
        assert_eq!((writes.lock().unwrap().len(), report.reason), (1, "superseded"));
        assert!(tree.clock.monotonic() < 0.5);
    }

    #[test]
    fn only_the_kernel_write_runs_at_background_priority_even_when_it_fails() {
        let tree = Tree::new();
        std::fs::write(tree.path.join("memory.reclaim"), "").unwrap();
        tree.anonymous(1000);
        let nice = FakeNice::new();
        let advance = tree.clock.clone();
        let mut budget = ReclaimBudget::new(2, 1e12, tree.clock.clone(), Arc::new(move |seconds| advance.advance(seconds)), nice.clone());
        budget.set_restorable(true); // As root (CAP_SYS_NICE) on a node.
        nice.calls.lock().unwrap().clear();
        let priorities = Arc::new(Mutex::new(Vec::new()));
        let (seen, level) = (priorities.clone(), nice.clone());
        let inner = shrinking(&tree, 1000, 100, Arc::new(Mutex::new(Vec::new())), usize::MAX);
        let write: Writer = Arc::new(move |file, data| {
            seen.lock().unwrap().push(*level.value.lock().unwrap());
            inner(file, data)
        });
        let sample = tree.sample(None).unwrap();
        tree.reclaimer(write).reclaim(&key(), &sample, request(200, 100, 200), &|| true, Some(&budget));
        assert_eq!((priorities.lock().unwrap().clone(), nice.calls.lock().unwrap().clone(), *nice.value.lock().unwrap()), (vec![19, 19], vec![19, 0, 19, 0], 0));
        let failing: Writer = Arc::new(|_, _| Err(std::io::Error::from_raw_os_error(libc::EIO)));
        let sample = tree.sample(None).unwrap();
        let report = tree.reclaimer(failing).reclaim(&key(), &sample, request(200, 100, 200), &|| true, Some(&budget));
        assert_eq!((report.reason, *nice.value.lock().unwrap()), ("kernel_error", 0)); // A failed write too.
    }

    #[test]
    fn a_partial_reclaim_continues_with_the_next_window() {
        let tree = Tree::new();
        std::fs::write(tree.path.join("memory.reclaim"), "").unwrap();
        tree.anonymous(1000);
        let writes = Arc::new(Mutex::new(Vec::new()));
        let inner = shrinking(&tree, 1000, 100, writes.clone(), usize::MAX);
        let write: Writer = Arc::new(move |file, data| {
            inner(file, data)?;
            Err(std::io::Error::from_raw_os_error(libc::EAGAIN))
        });
        let sample = tree.sample(None).unwrap();
        let report = tree.reclaimer(write).reclaim(&key(), &sample, request(200, 100, 200), &|| true, None);
        assert_eq!((writes.lock().unwrap().len(), report.reason, report.reclaimed_bytes), (2, "partial_reclaim", 200));
    }

    #[test]
    fn bundle_cgroup_paths_are_read_from_the_oci_config() {
        let dir = TempDir::new("bundle");
        assert!(bundle_cgroup_path(&dir.0).is_err());
        std::fs::write(dir.0.join("config.json"), r#"{"linux":{"cgroupsPath":"/ucloud-sandboxes/c"}}"#).unwrap();
        assert_eq!(bundle_cgroup_path(&dir.0), Ok(Some("/ucloud-sandboxes/c".into())));
        std::fs::write(dir.0.join("config.json"), r#"{"linux":{}}"#).unwrap();
        assert_eq!(bundle_cgroup_path(&dir.0), Ok(None));
        std::fs::write(dir.0.join("config.json"), r#"{"linux":{"cgroupsPath":3}}"#).unwrap();
        assert!(bundle_cgroup_path(&dir.0).is_err());
    }
}
