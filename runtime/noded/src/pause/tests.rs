//! The pause tier against a fake runsc (a shell script that keeps the
//! container's status in a file) and a fake /proc and cgroup tree, ported from
//! tests/test_pause_tier.py `PauseWardenTests` and `ThawPrefetchWardenTests`.
//! Nothing here needs root or a real runsc.

use std::fs::File;
use std::io;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::*;
use crate::journal::JournalStore;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;
const PID: u32 = 4242;
const TICKS: u64 = 777_777;

/// A private scratch directory, removed on drop.
pub(crate) struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(label: &str) -> TempDir {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "noded-pause-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&path).unwrap();
        TempDir(path)
    }
}

/// Poll `condition` for up to 2 s. A lock just released can outlive its
/// close for a moment: a process another test thread is spawning holds a copy
/// of every descriptor between its fork and exec (O_CLOEXEC closes them only
/// at the exec).
pub(crate) fn eventually(condition: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !condition() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const RUNSC: &str = r#"#!/bin/sh
fake='@FAKE@'
verb=''
for arg in "$@"; do
  case "$arg" in --*) ;; *) verb="$arg"; break ;; esac
done
case "$verb" in
state)
  printf '{"id":"%s","pid":@PID@,"status":"%s"}\n' "$3" "$(cat "$fake/status")" ;;
pause|resume)
  echo "$verb" >> "$fake/commands"
  if [ -e "$fake/sleep-$verb" ]; then sleep "$(cat "$fake/sleep-$verb")"; fi
  if [ -e "$fake/fail-$verb" ]; then echo "injected $verb failure" >&2; exit 1; fi
  want=paused
  [ "$verb" = resume ] && want=running
  if [ "$(cat "$fake/status")" = "$want" ]; then echo "container is already $want" >&2; exit 1; fi
  echo "$want" > "$fake/status" ;;
esac
"#;

struct Fake {
    dir: TempDir,
    sandbox: Sandbox,
    config: PauseConfig,
}

impl Fake {
    /// A running sandbox; `ram`: its memory is RAM-backed and 8 MiB of it
    /// exists, with 64 MiB of its cgroup swapped out.
    fn new(ram: bool) -> Fake {
        let dir = TempDir::new("tier");
        let root = dir.0.clone();
        let make = |path: &Path| std::fs::DirBuilder::new().recursive(true).mode(0o700).create(path).unwrap();
        for sub in ["bin", "fake", "runtime", "journals", "bundles/b-1", "memory", "proc/sys/kernel/random"] {
            make(&root.join(sub));
        }
        let sandbox = Sandbox {
            sandbox_id: "sb-1".into(),
            generation: 1,
            container_id: "c-sb-1".into(),
            bundle: root.join("bundles/b-1"),
            memory_directory: "mem-sb-1".into(),
            spec_sha256: "a".repeat(64),
        };
        let runsc = root.join("bin/runsc");
        std::fs::write(&runsc, RUNSC.replace("@FAKE@", &root.join("fake").display().to_string()).replace("@PID@", &PID.to_string())).unwrap();
        std::fs::set_permissions(&runsc, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(root.join("fake/status"), "running\n").unwrap();
        // The sentry as /proc shows it (runsc::owned_process_ticks checks all of this).
        let process = root.join("proc").join(PID.to_string());
        make(&process);
        let fields: Vec<String> = (0..19).map(|i| if i == 0 { "S".to_string() } else { "0".to_string() }).collect();
        std::fs::write(process.join("stat"), format!("{PID} (runsc-sandbox) {} {TICKS} 0 0\n", fields.join(" "))).unwrap();
        let runtime_root = root.join("runtime");
        let cmdline = format!(
            "runsc-sandbox\0--root={}\0--bundle={}\0boot\0{}\0",
            runtime_root.display(),
            sandbox.bundle.display(),
            sandbox.container_id
        );
        std::fs::write(process.join("cmdline"), cmdline).unwrap();
        std::os::unix::fs::symlink(&runsc, process.join("exe")).unwrap();
        std::fs::write(process.join("cgroup"), format!("0::/ucloud-sandboxes/{}\n", sandbox.container_id)).unwrap();
        std::fs::write(root.join("proc/sys/kernel/random/boot_id"), "0123456789abcdef0123456789abcdef\n").unwrap();
        make(&root.join("cgroup/ucloud-sandboxes").join(&sandbox.container_id));
        std::fs::write(root.join("zswap_enabled"), "N\n").unwrap();
        JournalStore::new(root.join("journals"))
            .initialize_running(&sandbox.sandbox_id, sandbox.generation, &sandbox.spec_sha256, "create-1", PID as u64, TICKS)
            .unwrap();
        let application_memory_root = ram.then(|| root.join("ram"));
        if let Some(ram) = &application_memory_root {
            make(&ram.join(&sandbox.memory_directory));
            let memory = ram.join(&sandbox.memory_directory).join(ACTIVE_APPLICATION_MEMORY);
            let data: Vec<u8> = (0..8 * MIB).map(|i| (i * 13 + 1) as u8).collect();
            std::fs::write(&memory, data).unwrap();
            std::fs::set_permissions(&memory, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let warden = WardenConfig {
            runsc: runsc.clone(),
            runtime_root,
            bundle_root: root.join("bundles"),
            journal_root: root.join("journals"),
            memory_root: root.join("memory"),
            application_memory_root,
            network: "none".into(),
            reflink_memory_restore: false,
            proc_root: root.join("proc"),
            command_timeout: Duration::from_secs(10),
            stop_timeout: Duration::from_secs(1),
        };
        let mut config = PauseConfig::new(warden, true);
        config.cgroup_root = root.join("cgroup");
        config.zswap_enabled = root.join("zswap_enabled");
        let fake = Fake { dir, sandbox, config };
        fake.set_swap(ram.then_some(64 * MIB));
        warm_up(&runsc);
        fake
    }

    /// A tier over the same roots: a new instance models a daemon restart.
    fn tier(&self) -> PauseTier {
        PauseTier::new(self.config.clone(), None)
    }

    fn tier_with(&self, configure: impl FnOnce(&mut PauseConfig), read_at: Arc<ReadAt>) -> PauseTier {
        let mut config = self.config.clone();
        configure(&mut config);
        PauseTier::with_reader(config, None, read_at)
    }

    fn path(&self, sub: &str) -> PathBuf {
        self.dir.0.join(sub)
    }

    fn status(&self) -> String {
        std::fs::read_to_string(self.path("fake/status")).unwrap().trim().to_string()
    }

    fn verbs(&self) -> Vec<String> {
        std::fs::read_to_string(self.path("fake/commands")).unwrap_or_default().lines().map(str::to_string).collect()
    }

    fn inject(&self, what: &str, value: Option<&str>) {
        let path = self.path("fake").join(what);
        match value {
            Some(value) => std::fs::write(path, value).unwrap(),
            None => {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    fn set_swap(&self, bytes: Option<u64>) {
        let path = self.path("cgroup/ucloud-sandboxes").join(&self.sandbox.container_id).join("memory.swap.current");
        match bytes {
            Some(bytes) => std::fs::write(path, format!("{bytes}\n")).unwrap(),
            None => {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    fn journal(&self) -> PathBuf {
        self.path("journals/sb-1.sandbox-1.json")
    }

    fn set_journal_state(&self, state: &str) {
        let text = std::fs::read_to_string(self.journal()).unwrap();
        std::fs::write(self.journal(), text.replace(r#""state":"running""#, &format!(r#""state":"{state}""#))).unwrap();
    }

    fn memory(&self) -> PathBuf {
        self.path("ram").join(&self.sandbox.memory_directory).join(ACTIVE_APPLICATION_MEMORY)
    }

    fn paused(&self, tier: &PauseTier) -> bool {
        tier.is_paused(&self.sandbox.sandbox_id, self.sandbox.generation)
    }

    fn key(&self) -> (String, u64) {
        (self.sandbox.sandbox_id.clone(), self.sandbox.generation)
    }
}

/// Exec a freshly written script until no forked sibling still holds it open
/// for writing (ETXTBSY); after one success no writer can exist.
fn warm_up(script: &Path) {
    for _ in 0..200 {
        match std::process::Command::new(script).arg("--root=/nonexistent").arg("warm-up").output() {
            Err(error) if error.raw_os_error() == Some(libc::ETXTBSY) => std::thread::sleep(Duration::from_millis(5)),
            result => {
                result.unwrap();
                return;
            }
        }
    }
    panic!("the fake runsc stayed busy");
}

fn stats(tier: &PauseTier) -> (i64, i64, i64, i64) {
    let stats = tier.stats();
    (stats.get(Counter::Pauses), stats.get(Counter::Thaws), stats.get(Counter::ThawPrefetches), stats.get(Counter::ThawPrefetchedBytes))
}

/// A read hook: `observe` runs before every read, then `delay`, then pread.
fn reader(observe: impl Fn() + Send + Sync + 'static, delay: Duration) -> Arc<ReadAt> {
    Arc::new(move |file: &File, buffer: &mut [u8], offset: u64| {
        observe();
        std::thread::sleep(delay);
        prefetch::pread(file, buffer, offset)
    })
}

#[tokio::test]
async fn pause_keeps_live_authority_and_a_thaw_resumes() {
    let fake = Fake::new(false);
    std::fs::write(fake.path("zswap_enabled"), "Y\n").unwrap();
    let cgroup = fake.path("cgroup/ucloud-sandboxes/c-sb-1");
    std::fs::write(cgroup.join("memory.max"), format!("{}\n", 2 * GIB)).unwrap();
    std::fs::write(cgroup.join("memory.zswap.max"), "max\n").unwrap();
    let journal = std::fs::read(fake.journal()).unwrap();
    let tier = fake.tier();
    assert!(tier.pause(&fake.sandbox).await.unwrap());
    assert_eq!(fake.status(), "paused");
    assert!(fake.paused(&tier));
    let marker = tier.marker_path("sb-1", 1).unwrap();
    assert_eq!(marker, fake.path("runtime/warden-paused/sb-1.sandbox-1"));
    assert_eq!(std::fs::read(&marker).unwrap(), b"c-sb-1");
    assert_eq!(std::fs::metadata(&marker).unwrap().permissions().mode() & 0o777, 0o600);
    assert_eq!(std::fs::metadata(fake.path("runtime/warden-paused")).unwrap().permissions().mode() & 0o777, 0o700);
    // zswap was capped through the journal's sentry pid.
    assert_eq!(std::fs::read_to_string(cgroup.join("memory.zswap.max")).unwrap(), (GIB / 2).to_string());
    // No ownership change: the journal is untouched.
    assert_eq!(std::fs::read(fake.journal()).unwrap(), journal);
    assert!(tier.thaw(&fake.sandbox, true).await.unwrap().is_some());
    assert_eq!(fake.status(), "running");
    assert!(!fake.paused(&tier) && !marker.exists());
    assert!(tier.thaw(&fake.sandbox, true).await.unwrap().is_none()); // Fast path: one lstat.
    assert_eq!(fake.verbs(), ["pause", "resume"]);
    assert_eq!(stats(&tier), (1, 1, 0, 0));
    let snapshot = tier.stats().snapshot();
    assert!(snapshot["thaw_ms_max"].as_i64().unwrap() <= snapshot["thaw_ms_total"].as_i64().unwrap());
}

#[tokio::test]
async fn a_keep_paused_read_thaws_and_pauses_again_under_one_warden_hold() {
    let fake = Fake::new(false);
    let tier = fake.tier();
    tier.pause(&fake.sandbox).await.unwrap();
    std::fs::write(fake.path("runtime/warden-paused/.sb-1.sandbox-1.x.tmp"), b"").unwrap();
    {
        let lock = tier.lock(&fake.sandbox).await.unwrap();
        let record = tier.journal_record(&fake.sandbox).await.unwrap().unwrap();
        assert!(tier.thaw_locked(&fake.sandbox, &lock, false).await.unwrap().is_some());
        assert_eq!(fake.status(), "running");
        tier.pause_locked(&fake.sandbox, &lock, &record).await.unwrap();
    }
    assert_eq!((fake.status(), tier.paused_keys().unwrap()), ("paused".to_string(), vec![fake.key()]));
    assert_eq!(stats(&tier), (2, 1, 0, 0));
}

#[tokio::test]
async fn pause_requires_the_flag_and_a_running_journal() {
    let fake = Fake::new(false);
    let disabled = fake.tier_with(|config| config.enabled = false, Arc::new(prefetch::pread));
    let error = disabled.pause(&fake.sandbox).await.unwrap_err();
    assert!(error.to_string().contains("disabled"), "{error}");
    let tier = fake.tier();
    let unjournaled = Sandbox { generation: 2, ..fake.sandbox.clone() };
    assert!(!tier.pause(&unjournaled).await.unwrap()); // No journal.
    fake.set_journal_state("parked");
    assert!(!tier.pause(&fake.sandbox).await.unwrap());
    assert!(!fake.paused(&tier) && !tier.is_paused("sb-1", 2));
    assert_eq!((fake.verbs().len(), stats(&tier)), (0, (0, 0, 0, 0)));
}

#[tokio::test]
async fn a_crash_after_the_marker_before_runsc_pause_is_harmless() {
    let fake = Fake::new(false);
    fake.inject("sleep-pause", Some("5"));
    let tier = fake.tier();
    // The daemon dies (the future drops, runsc is killed) mid `runsc pause`.
    assert!(tokio::time::timeout(Duration::from_millis(500), tier.pause(&fake.sandbox)).await.is_err());
    assert!(fake.paused(&tier));
    assert_eq!(fake.status(), "running");
    fake.inject("sleep-pause", None);
    let restarted = fake.tier();
    // Resume fails; `runsc state` proves the sentry running.
    assert!(restarted.thaw(&fake.sandbox, true).await.unwrap().is_some());
    assert!(!fake.paused(&restarted));
    assert_eq!(fake.status(), "running");
    assert_eq!(stats(&restarted), (0, 1, 0, 0));
}

#[tokio::test]
async fn a_restart_keeps_a_paused_sandbox_paused_and_thaws_it_on_demand() {
    let fake = Fake::new(false);
    fake.tier().pause(&fake.sandbox).await.unwrap();
    let restarted = fake.tier();
    assert!(fake.paused(&restarted));
    assert_eq!(fake.status(), "paused");
    // A repeated pause is idempotent: runsc refuses, state proves this sentry paused.
    assert!(restarted.pause(&fake.sandbox).await.unwrap());
    assert_eq!(restarted.paused_keys().unwrap(), vec![fake.key()]);
    assert_eq!(fake.status(), "paused");
    assert!(restarted.thaw(&fake.sandbox, true).await.unwrap().is_some());
    assert_eq!(fake.status(), "running");
    assert_eq!(restarted.paused_keys().unwrap(), vec![]);
    assert_eq!(stats(&restarted), (1, 1, 0, 0));
}

#[tokio::test]
async fn a_failed_pause_leaves_the_runtime_running_without_a_marker() {
    let fake = Fake::new(false);
    fake.inject("fail-pause", Some(""));
    let tier = fake.tier();
    let error = tier.pause(&fake.sandbox).await.unwrap_err();
    assert!(error.to_string().starts_with("runsc pause failed: injected pause failure"), "{error}");
    assert_eq!(fake.status(), "running");
    assert!(!fake.paused(&tier));
    assert_eq!(tier.stats().get(Counter::Pauses), 0);
}

#[tokio::test]
async fn a_pause_of_another_sentry_is_not_adopted() {
    let fake = Fake::new(false);
    fake.inject("fail-pause", Some(""));
    fake.tier().pause(&fake.sandbox).await.unwrap_err();
    // runsc says paused, but the journal names another sentry incarnation.
    std::fs::write(fake.path("fake/status"), "paused\n").unwrap();
    let text = std::fs::read_to_string(fake.journal()).unwrap();
    std::fs::write(fake.journal(), text.replace(&format!(":{TICKS},"), ":1,")).unwrap();
    let error = fake.tier().pause(&fake.sandbox).await.unwrap_err();
    assert!(error.to_string().starts_with("runsc pause failed"), "{error}");
    assert_eq!(fake.status(), "running"); // Rolled back through a resume.
    assert!(!fake.path("runtime/warden-paused/sb-1.sandbox-1").exists());
}

#[tokio::test]
async fn a_failed_resume_keeps_the_marker_and_releases_the_thaw() {
    let fake = Fake::new(false);
    let tier = fake.tier();
    tier.pause(&fake.sandbox).await.unwrap();
    fake.inject("fail-resume", Some(""));
    let error = tier.thaw(&fake.sandbox, true).await.unwrap_err();
    assert!(error.to_string().starts_with("runsc resume of a paused sandbox failed: injected resume failure"), "{error}");
    assert!(fake.paused(&tier));
    assert!(eventually(|| !tier.thawing("sb-1", 1) && tier.settled("sb-1", 1)));
    fake.inject("fail-resume", None);
    assert!(tier.thaw(&fake.sandbox, true).await.unwrap().is_some());
    assert_eq!(stats(&tier), (1, 1, 0, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_thaw_racing_pauses_always_leaves_the_runtime_running_under_the_lock() {
    let fake = Fake::new(false);
    let tier = fake.tier();
    let stop = Arc::new(AtomicBool::new(false));
    let pauser = {
        let (tier, sandbox, stop) = (tier.clone(), fake.sandbox.clone(), stop.clone());
        tokio::spawn(async move {
            let mut pauses = 0;
            while !stop.load(Ordering::SeqCst) {
                tier.pause(&sandbox).await.unwrap();
                pauses += 1;
                tokio::task::yield_now().await;
            }
            pauses
        })
    };
    let mut paused_execs = 0;
    for _ in 0..25 {
        let lock = tier.lock(&fake.sandbox).await.unwrap();
        tier.thaw_locked(&fake.sandbox, &lock, true).await.unwrap();
        if fake.status() != "running" {
            paused_execs += 1; // An exec here would reach a paused runtime.
        }
        drop(lock);
        tokio::task::yield_now().await;
    }
    stop.store(true, Ordering::SeqCst);
    assert!(pauser.await.unwrap() > 0);
    assert_eq!(paused_execs, 0);
}

#[tokio::test]
async fn a_thaw_reads_swapped_memory_back_before_resume() {
    let fake = Fake::new(true);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observer = {
        let (seen, status, config) = (seen.clone(), fake.path("fake/status"), fake.config.clone());
        let probe = PauseTier::new(config, None);
        move || {
            let status = std::fs::read_to_string(&status).unwrap().trim().to_string();
            // Reclaim sees the thaw before the marker goes.
            seen.lock().unwrap().push((status, probe.is_paused("sb-1", 1), probe.thawing("sb-1", 1), probe.settled("sb-1", 1)));
        }
    };
    let tier = fake.tier_with(|_| {}, reader(observer, Duration::ZERO));
    tier.pause(&fake.sandbox).await.unwrap();
    assert!(tier.settled("sb-1", 1));
    tier.thaw(&fake.sandbox, true).await.unwrap().unwrap();
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 8); // 8 MiB in 1 MiB reads.
    assert!(seen.iter().all(|observed| observed == &("paused".to_string(), true, true, false)), "{seen:?}");
    assert_eq!((fake.status(), fake.paused(&tier), tier.thawing("sb-1", 1)), ("running".to_string(), false, false));
    assert_eq!(stats(&tier), (1, 1, 1, 8 * MIB as i64));
    assert!(tier.inner.prefetches.lock().unwrap().is_empty());
}

#[tokio::test]
async fn no_prefetch_without_swap_for_file_memory_or_a_keep_paused_read() {
    let fake = Fake::new(true);
    let reads = Arc::new(AtomicUsize::new(0));
    let counted = {
        let reads = reads.clone();
        reader(move || { reads.fetch_add(1, Ordering::SeqCst); }, Duration::ZERO)
    };
    let tier = fake.tier_with(|_| {}, counted.clone());
    for swap in [None, Some(0), Some(PREFETCH_MIN_SWAP_BYTES - 1)] {
        fake.set_swap(swap); // No reclaim moved anything out.
        tier.pause(&fake.sandbox).await.unwrap();
        tier.thaw(&fake.sandbox, true).await.unwrap().unwrap();
        assert_eq!(fake.status(), "running");
    }
    fake.set_swap(Some(GIB));
    tier.pause(&fake.sandbox).await.unwrap();
    tier.thaw(&fake.sandbox, false).await.unwrap().unwrap(); // A status read's thaw.
    fake.inject("fail-pause", Some("")); // A failed pause rolls back without a prefetch.
    tier.pause(&fake.sandbox).await.unwrap_err();
    fake.inject("fail-pause", None);
    // The journal's pid now belongs to another process: no cgroup to trust.
    let stat = fake.path("proc").join(PID.to_string()).join("stat");
    let original = std::fs::read_to_string(&stat).unwrap();
    tier.pause(&fake.sandbox).await.unwrap();
    std::fs::write(&stat, original.replace(&format!(" {TICKS} "), " 1 ")).unwrap();
    tier.thaw(&fake.sandbox, true).await.unwrap().unwrap();
    std::fs::write(&stat, original).unwrap();
    // File memory: host reads would charge the daemon's cgroup.
    let file = fake.tier_with(|config| config.warden.application_memory_root = None, counted.clone());
    file.pause(&fake.sandbox).await.unwrap();
    file.thaw(&fake.sandbox, true).await.unwrap().unwrap();
    // The memory-backing store's placement wins over the configured root.
    struct FileMode;
    impl ModeSource for FileMode {
        fn application_memory_mode(&self, _: &str, _: u64) -> Option<MemoryMode> {
            Some(MemoryMode::File)
        }
    }
    let placed = PauseTier::with_reader(fake.config.clone(), Some(Arc::new(FileMode)), counted);
    placed.pause(&fake.sandbox).await.unwrap();
    placed.thaw(&fake.sandbox, true).await.unwrap().unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert_eq!(tier.stats().get(Counter::ThawPrefetches), 0);
}

#[tokio::test]
async fn a_failed_slow_or_foreign_prefetch_still_resumes() {
    let fake = Fake::new(true);
    let failing: Arc<ReadAt> = Arc::new(|_: &File, _: &mut [u8], _: u64| Err(io::Error::from_raw_os_error(libc::EIO)));
    let tier = fake.tier_with(|_| {}, failing);
    tier.pause(&fake.sandbox).await.unwrap();
    tier.thaw(&fake.sandbox, true).await.unwrap().unwrap();
    assert_eq!((fake.status(), fake.paused(&tier), stats(&tier)), ("running".to_string(), false, (1, 1, 0, 0)));

    let slow = fake.tier_with(|config| config.prefetch_seconds = Duration::from_millis(50), reader(|| {}, Duration::from_millis(100)));
    slow.pause(&fake.sandbox).await.unwrap();
    let started = Instant::now();
    slow.thaw(&fake.sandbox, true).await.unwrap().unwrap();
    assert!(started.elapsed() < Duration::from_secs(1));
    let (_, _, prefetches, read) = stats(&slow);
    assert_eq!(prefetches, 1);
    assert!(read <= 2 * MIB as i64, "{read}"); // At most one read per reader, not four.

    std::fs::set_permissions(fake.memory(), std::fs::Permissions::from_mode(0o644)).unwrap();
    slow.pause(&fake.sandbox).await.unwrap(); // Only ever a privately owned memory file.
    slow.thaw(&fake.sandbox, true).await.unwrap().unwrap();
    assert_eq!((fake.status(), slow.stats().get(Counter::ThawPrefetches)), ("running".to_string(), 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delete_cancels_an_inflight_prefetch() {
    let fake = Fake::new(true);
    let reading = Arc::new(AtomicBool::new(false));
    let flag = reading.clone();
    let tier = fake.tier_with(
        |config| config.prefetch_seconds = Duration::from_secs(60),
        reader(move || flag.store(true, Ordering::SeqCst), Duration::from_millis(500)),
    );
    assert!(!tier.cancel_prefetch(&fake.sandbox));
    tier.pause(&fake.sandbox).await.unwrap();
    let thaw = {
        let (tier, sandbox) = (tier.clone(), fake.sandbox.clone());
        tokio::spawn(async move { tier.thaw(&sandbox, true).await })
    };
    while !reading.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let started = Instant::now();
    assert!(tier.cancel_prefetch(&fake.sandbox)); // Uncancelled: 4 reads of 0.5 s per reader.
    thaw.await.unwrap().unwrap().unwrap();
    assert!(started.elapsed() < Duration::from_millis(1500));
    assert!(tier.stats().get(Counter::ThawPrefetchedBytes) < 8 * MIB as i64);
    assert_eq!((tier.paused_keys().unwrap(), tier.inner.prefetches.lock().unwrap().len()), (vec![], 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_during_prefetch_leaves_it_paused_and_a_restart_prefetches() {
    let fake = Fake::new(true);
    let tier = fake.tier_with(|_| {}, reader(|| {}, Duration::from_millis(300)));
    tier.pause(&fake.sandbox).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(100), tier.thaw(&fake.sandbox, true)).await.is_err());
    assert_eq!((fake.status(), fake.paused(&tier)), ("paused".to_string(), true));
    // The abandoned readers still hold the marker: the thaw stays visible.
    assert!(tier.thawing("sb-1", 1));
    assert!(tier.cancel_prefetch(&fake.sandbox));
    let restarted = fake.tier();
    restarted.thaw(&fake.sandbox, true).await.unwrap().unwrap();
    assert_eq!((fake.status(), stats(&restarted)), ("running".to_string(), (0, 1, 1, 8 * MIB as i64)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thaw_is_visible_until_its_marker_is_gone() {
    let fake = Fake::new(false);
    let tier = fake.tier();
    tier.pause(&fake.sandbox).await.unwrap();
    fake.inject("sleep-resume", Some("0.3"));
    let thaw = {
        let (tier, sandbox) = (tier.clone(), fake.sandbox.clone());
        tokio::spawn(async move { tier.thaw(&sandbox, false).await })
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !tier.thawing("sb-1", 1) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    // Mid `runsc resume`: paused, marker present, a thaw in progress.
    assert!(tier.thawing("sb-1", 1) && tier.is_paused("sb-1", 1) && !tier.settled("sb-1", 1));
    thaw.await.unwrap().unwrap().unwrap();
    assert!(!tier.thawing("sb-1", 1) && !tier.is_paused("sb-1", 1) && !tier.settled("sb-1", 1));
}

#[tokio::test]
async fn unsafe_ids_name_no_marker() {
    let fake = Fake::new(false);
    let tier = fake.tier();
    let escaping = Sandbox { sandbox_id: "../x".into(), ..fake.sandbox.clone() };
    assert!(tier.marker_path("../x", 1).is_none());
    assert!(!tier.is_paused("../x", 1) && !tier.thawing("../x", 1) && !tier.settled("../x", 1));
    assert!(tier.thaw(&escaping, true).await.unwrap().is_none());
}
