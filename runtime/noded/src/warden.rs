//! The create half of the Warden (ucloud_sandboxes/direct_warden.py): the
//! per-incarnation warden lock, `discard_unjournaled`, the runtime delete with
//! its metadata fence, and `create` (runsc create, start, state, sentry
//! verification, lifecycle journal). Everything after a sandbox runs stays with
//! the Python agent, which reads the same journal, locks and runsc state.

use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use crate::fsutil::{FileLock, atomic_write, ensure_private_dir};
use crate::journal::{JournalError, JournalStore};
use crate::runsc::{self, Role, RunscError, SentryOwner};

#[derive(Debug)]
pub enum WardenError {
    /// Python: DirectWardenError.
    Warden(String),
    Journal(JournalError),
    Io(std::io::Error),
}

impl std::fmt::Display for WardenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WardenError::Warden(message) => f.write_str(message),
            WardenError::Journal(error) => write!(f, "{error}"),
            WardenError::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for WardenError {}

impl From<std::io::Error> for WardenError {
    fn from(error: std::io::Error) -> Self {
        WardenError::Io(error)
    }
}

impl From<JournalError> for WardenError {
    fn from(error: JournalError) -> Self {
        WardenError::Journal(error)
    }
}

impl From<RunscError> for WardenError {
    fn from(error: RunscError) -> Self {
        WardenError::Warden(error.to_string())
    }
}

fn fail(message: impl Into<String>) -> WardenError {
    WardenError::Warden(message.into())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryMode {
    Ram,
    File,
}

#[derive(Debug, Clone)]
pub struct WardenConfig {
    pub runsc: PathBuf,
    pub runtime_root: PathBuf,
    pub bundle_root: PathBuf,
    pub journal_root: PathBuf,
    /// The volume mount root (file-backed application memory lives here).
    pub memory_root: PathBuf,
    pub application_memory_root: Option<PathBuf>,
    pub network: String,
    pub reflink_memory_restore: bool,
    pub proc_root: PathBuf,
    pub command_timeout: Duration,
    pub stop_timeout: Duration,
}

/// One incarnation's runtime identity (Python `DirectSandbox`).
#[derive(Debug, Clone)]
pub struct Sandbox {
    pub sandbox_id: String,
    pub generation: u64,
    pub container_id: String,
    pub bundle: PathBuf,
    pub memory_directory: String,
    pub spec_sha256: String,
}

/// Cheap to clone: blocking steps run on a clone in `spawn_blocking`.
#[derive(Clone)]
pub struct Warden {
    config: WardenConfig,
    journal: JournalStore,
}

/// Run blocking filesystem work (flocks, fences, fsyncs) off the reactor.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> Result<T, WardenError> + Send + 'static) -> Result<T, WardenError> {
    tokio::task::spawn_blocking(work).await.map_err(|error| fail(format!("warden worker failed: {error}")))?
}

const SHARED_PARENT_CGROUP_BUSY: &str = "removing cgroup path";

impl Warden {
    pub fn new(config: WardenConfig) -> Self {
        let journal = JournalStore::new(config.journal_root.clone());
        Warden { config, journal }
    }

    pub fn journal(&self) -> &JournalStore {
        &self.journal
    }

    fn prefix(&self) -> Vec<String> {
        vec![self.config.runsc.display().to_string(), format!("--root={}", self.config.runtime_root.display())]
    }

    /// `_common`: the global flags of `runsc create`.
    fn common(&self, mode: MemoryMode) -> Result<Vec<String>, WardenError> {
        let root = match mode {
            MemoryMode::Ram => self.config.application_memory_root.clone()
                .ok_or_else(|| fail("RAM application memory requires an application memory root"))?,
            MemoryMode::File => self.config.memory_root.clone(),
        };
        let mut argv = self.prefix();
        argv.extend(["--platform=systrap".into(), format!("--network={}", self.config.network),
                     format!("--application-memory-file-dir={}", root.display())]);
        if mode == MemoryMode::Ram {
            argv.push("--application-memory-ram-backing=true".into());
        } else if self.config.reflink_memory_restore {
            argv.push("--application-memory-reflink-restore=true".into());
        }
        argv.push("--allow-connected-on-save=true".into());
        Ok(argv)
    }

    fn owner<'a>(&'a self, sandbox: &'a Sandbox) -> SentryOwner<'a> {
        SentryOwner {
            proc_root: &self.config.proc_root,
            runsc: &self.config.runsc,
            runtime_root: &self.config.runtime_root,
            bundle: &sandbox.bundle,
            container_id: &sandbox.container_id,
        }
    }

    /// The per-incarnation warden flock, shared with the Python agent.
    pub async fn lock(&self, sandbox: &Sandbox) -> Result<FileLock, WardenError> {
        let directory = self.config.runtime_root.join("warden-locks");
        let name = format!(".{}.sandbox-{}.warden.lock", sandbox.sandbox_id, sandbox.generation);
        blocking(move || {
            ensure_private_dir(&directory)?;
            Ok(FileLock::acquire(&directory.join(name), false)?)
        })
        .await
    }

    async fn journaled(&self, sandbox: &Sandbox) -> Result<bool, WardenError> {
        let (journal, id, generation) = (self.journal.clone(), sandbox.sandbox_id.clone(), sandbox.generation);
        blocking(move || Ok(journal.load(&id, generation)?.is_some())).await
    }

    /// Refuse if a journal exists; else delete any runtime this container id left.
    pub async fn discard_unjournaled(&self, sandbox: &Sandbox) -> Result<(), WardenError> {
        let _lock = self.lock(sandbox).await?;
        if self.journaled(sandbox).await? {
            return Err(fail("refusing to discard a backend with a lifecycle journal"));
        }
        self.delete_runtime(sandbox, false).await
    }

    /// `_validate_bundle`: a private directory under bundle_root whose config
    /// names this incarnation's application-memory directory.
    fn validate_bundle(&self, sandbox: &Sandbox) -> Result<(), WardenError> {
        let bundle = std::fs::canonicalize(&sandbox.bundle)
            .map_err(|_| fail("sandbox bundle must be a durable directory below bundle_root"))?;
        let root = std::fs::canonicalize(&self.config.bundle_root)
            .map_err(|_| fail("sandbox bundle must be a durable directory below bundle_root"))?;
        if !bundle.starts_with(&root) {
            return Err(fail("sandbox bundle must be a durable directory below bundle_root"));
        }
        let meta = std::fs::symlink_metadata(&bundle)?;
        if !meta.is_dir() || meta.uid() != crate::fsutil::euid() || meta.mode() & 0o022 != 0 {
            return Err(fail("sandbox bundle is not a private directory"));
        }
        let config: Value = serde_json::from_slice(&std::fs::read(bundle.join("config.json"))?)
            .map_err(|_| fail("bundle lacks a valid application-memory-directory annotation"))?;
        let configured = config.pointer("/annotations/dev.gvisor.internal.application-memory-directory")
            .and_then(Value::as_str)
            .ok_or_else(|| fail("bundle lacks a valid application-memory-directory annotation"))?;
        if configured != sandbox.memory_directory {
            return Err(fail("bundle application-memory-directory does not match Warden state"));
        }
        Ok(())
    }

    /// Start a planned, rooted incarnation and journal it as running.
    /// `require_memory` re-validates the split memory allocation (if any).
    pub async fn create(
        &self,
        sandbox: &Sandbox,
        operation_id: &str,
        mode: MemoryMode,
        require_memory: impl FnOnce() -> Result<(), WardenError> + Send + 'static,
        timings: &mut crate::timings::Timings,
    ) -> Result<Map<String, Value>, WardenError> {
        let _lock = self.lock(sandbox).await?;
        // Under the lock: an incarnation someone already journaled is never
        // created again, nor deleted by this create's cleanup.
        if self.journaled(sandbox).await? {
            return Err(fail("refusing to create a backend with a lifecycle journal"));
        }
        let active_root = match mode {
            MemoryMode::Ram => self.config.application_memory_root.clone().ok_or_else(|| fail("no application memory root"))?,
            MemoryMode::File => self.config.memory_root.clone(),
        };
        let (this, checked) = (self.clone(), sandbox.clone());
        blocking(move || {
            this.validate_bundle(&checked)?;
            require_memory()?;
            let active = active_root.join(&checked.memory_directory);
            {
                use std::os::unix::fs::DirBuilderExt;
                std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&active)?;
            }
            ensure_private_dir(&active)?;
            Ok(())
        })
        .await?;
        let mut create = self.common(mode)?;
        create.extend(["create".into(), format!("--bundle={}", sandbox.bundle.display()), sandbox.container_id.clone()]);
        let started = timings.start();
        let created = runsc::checked(&create, self.config.command_timeout).await;
        timings.add("runsc_create", started);
        // Unlike Python, a failed create also deletes whatever runsc left (spec §7).
        let result = match created {
            Err(error) => Err(error.into()),
            Ok(_) => self.start_and_journal(sandbox, operation_id, timings).await,
        };
        if result.is_err() && !self.journaled(sandbox).await.unwrap_or(true) {
            let _ = self.delete_runtime(sandbox, false).await;
        }
        result
    }

    async fn start_and_journal(
        &self,
        sandbox: &Sandbox,
        operation_id: &str,
        timings: &mut crate::timings::Timings,
    ) -> Result<Map<String, Value>, WardenError> {
        let mut start = self.prefix();
        start.extend(["start".into(), sandbox.container_id.clone()]);
        let started = timings.start();
        runsc::checked(&start, self.config.command_timeout).await?;
        timings.add("runsc_start", started);
        let started = timings.start();
        let mut state = self.prefix();
        state.extend(["state".into(), sandbox.container_id.clone()]);
        let result = runsc::checked(&state, self.config.command_timeout).await?;
        let (pid, status) = runsc::parse_state(&result.stdout)?;
        if status != "running" && status != "paused" {
            return Err(fail(format!("runsc state is not live: {status}")));
        }
        let (this, owned) = (self.clone(), sandbox.clone());
        let ticks = blocking(move || {
            runsc::sentry_identity(&this.owner(&owned), pid, None).map_err(|_| fail("cannot read sentry process identity"))
        })
        .await?;
        timings.add("runsc_state", started);
        let started = timings.start();
        let (journal, journaled, operation_id) = (self.journal.clone(), sandbox.clone(), operation_id.to_string());
        let record = blocking(move || {
            Ok(journal.initialize_running(
                &journaled.sandbox_id, journaled.generation, &journaled.spec_sha256, &operation_id, pid as u64, ticks)?)
        })
        .await?;
        timings.add("journal_commit", started);
        Ok(record)
    }

    /// `_delete_runtime`: fence runsc's metadata (kill verified owners, clear
    /// their PIDs), then `runsc delete --force`.
    pub async fn delete_runtime(&self, sandbox: &Sandbox, checked: bool) -> Result<(), WardenError> {
        let (this, fenced) = (self.clone(), sandbox.clone());
        blocking(move || this.fence_delete_metadata(&fenced)).await?;
        let mut argv = self.prefix();
        argv.extend(["delete".into(), "--force".into(), sandbox.container_id.clone()]);
        let result = runsc::run(&argv, self.config.command_timeout).await?;
        let busy_parent = result.stderr.contains(SHARED_PARENT_CGROUP_BUSY)
            && result.stderr.contains("/sys/fs/cgroup/ucloud-sandboxes")
            && result.stderr.contains("device or resource busy");
        if result.returncode == 0 || (busy_parent && self.runtime_absent(sandbox).await) {
            let marker = self.config.runtime_root.join("warden-process-owners").join(&sandbox.container_id);
            let _ = std::fs::remove_file(marker);
        } else if checked {
            return Err(fail(format!("runsc cleanup failed: {}", result.stderr)));
        }
        Ok(())
    }

    async fn runtime_absent(&self, sandbox: &Sandbox) -> bool {
        let mut argv = self.prefix();
        argv.extend(["list".into(), "--format=json".into()]);
        let Ok(listed) = runsc::run(&argv, self.config.command_timeout).await else { return false };
        if listed.returncode != 0 {
            return false;
        }
        match serde_json::from_str::<Value>(&listed.stdout) {
            Ok(Value::Null) => true,
            Ok(Value::Array(items)) => !items.iter().any(|item| item.get("id").and_then(Value::as_str) == Some(sandbox.container_id.as_str())),
            _ => false,
        }
    }

    fn fence_delete_metadata(&self, sandbox: &Sandbox) -> Result<(), WardenError> {
        let stem = format!("{0}_sandbox:{0}", sandbox.container_id);
        let path = self.config.runtime_root.join(format!("{stem}.state"));
        let lock = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(false)
            .mode(0o600).custom_flags(libc::O_NOFOLLOW).open(self.config.runtime_root.join(format!("{stem}.lock")))?;
        let deadline = Instant::now() + self.config.command_timeout;
        // SAFETY: valid descriptor; LOCK_NB polled so a stuck runsc cannot wedge us.
        while unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            if Instant::now() >= deadline {
                return Err(fail("timed out acquiring runsc cleanup metadata lock"));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let file = match std::fs::OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let meta = file.metadata()?;
        if !meta.is_file() || meta.uid() != crate::fsutil::euid() || meta.mode() & 0o022 != 0 {
            return Err(fail("runsc cleanup metadata is not private"));
        }
        let limit = 4 * 1024 * 1024;
        let mut raw = Vec::new();
        file.take(limit + 1).read_to_end(&mut raw)?;
        if raw.len() as u64 > limit {
            return Err(fail("runsc cleanup metadata exceeds its bound"));
        }
        let proof = |message: String| fail(format!("cannot prove runsc cleanup process ownership: {message}"));
        let Value::Object(mut state) = serde_json::from_slice(&raw).map_err(|e| proof(e.to_string()))? else {
            return Err(fail("runsc cleanup metadata is not an object"));
        };
        let runtime_id = state.get("sandbox").map(|runtime| runtime.get("id").cloned());
        if state.get("id").and_then(Value::as_str) != Some(sandbox.container_id.as_str())
            || runtime_id.as_ref().is_some_and(|id| id.as_ref().and_then(Value::as_str) != Some(sandbox.container_id.as_str()))
        {
            return Err(fail("runsc cleanup metadata has another owner"));
        }
        let sentry_pid = state.get("sandbox").and_then(|r| r.get("pid")).cloned().unwrap_or(Value::from(0));
        let gofer_pid = state.get("goferPid").cloned().unwrap_or(Value::from(0));
        let mut handles = Vec::new();
        for (role, pid) in [(Role::Sandbox, sentry_pid), (Role::Gofer, gofer_pid)] {
            let pid = pid.as_u64().filter(|pid| *pid != 1 && *pid <= u32::MAX as u64)
                .ok_or_else(|| fail("runsc cleanup metadata has an unsafe PID"))? as u32;
            if pid == 0 {
                continue;
            }
            match self.open_cleanup_fence(sandbox, pid, role) {
                Ok(handle) => handles.push(handle),
                Err(FenceError::Gone) => continue, // Exited: nothing to signal; still clear its PID.
                Err(FenceError::Failed(error)) => return Err(error),
            }
        }
        // Every target is verified before the first signal.
        for handle in &handles {
            handle.terminate(self.config.stop_timeout)?;
        }
        state.insert("goferPid".into(), Value::from(0));
        if let Some(Value::Object(runtime)) = state.get_mut("sandbox") {
            runtime.insert("pid".into(), Value::from(0));
        }
        // Python json.dump: default separators, insertion order.
        let text = serde_json::to_string(&Value::Object(state)).map_err(|e| fail(e.to_string()))?;
        atomic_write(&path, python_default_separators(&text).as_bytes())?;
        Ok(())
    }

    fn open_cleanup_fence(&self, sandbox: &Sandbox, pid: u32, role: Role) -> Result<PidFd, FenceError> {
        let ticks = runsc::start_time_ticks(&self.config.proc_root, pid).map_err(|_| FenceError::Gone)?;
        let handle = match PidFd::open(&self.config.proc_root, pid, ticks) {
            Ok(handle) => handle,
            Err(_) => {
                // Exit between stat and pidfd_open is normal; a replacement owner
                // fails provenance and is never killed.
                return match runsc::owned_process_ticks(&self.owner(sandbox), role, pid, Some(ticks)) {
                    Err(RunscError::ProcessLookup(_)) => Err(FenceError::Gone),
                    Err(error) => Err(FenceError::Failed(error.into())),
                    Ok(_) => Err(FenceError::Failed(fail("could not open sentry pidfd"))),
                };
            }
        };
        let verified = match role {
            Role::Sandbox => runsc::sentry_identity(&self.owner(sandbox), pid, Some(ticks)).map(|_| ()),
            Role::Gofer => runsc::boot_id(&self.config.proc_root)
                .and_then(|_| runsc::owned_process_ticks(&self.owner(sandbox), role, pid, Some(ticks)).map(|_| ())),
        };
        match verified {
            Ok(()) => Ok(handle),
            Err(error) => {
                if !handle.alive() || handle.wait_exit(self.config.stop_timeout) {
                    Err(FenceError::Gone)
                } else {
                    Err(FenceError::Failed(error.into()))
                }
            }
        }
    }
}

enum FenceError {
    Gone,
    Failed(WardenError),
}

/// `json.dumps` without separators: ", " and ": " outside strings.
fn python_default_separators(compact: &str) -> String {
    let mut out = String::with_capacity(compact.len() + compact.len() / 8);
    let (mut in_string, mut escaped) = (false, false);
    for c in compact.chars() {
        out.push(c);
        if in_string {
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
        } else if c == '"' {
            in_string = true;
        } else if c == ',' || c == ':' {
            out.push(' ');
        }
    }
    out
}

/// An exact process reference (Python `LinuxPidfdHandle`).
pub struct PidFd {
    fd: OwnedFd,
    pid: u32,
    ticks: u64,
    proc_root: PathBuf,
}

impl PidFd {
    pub fn open(proc_root: &Path, pid: u32, ticks: u64) -> Result<PidFd, WardenError> {
        if pid <= 1 {
            return Err(fail("refusing to fence a system process PID"));
        }
        let matches = || runsc::start_time_ticks(proc_root, pid).is_ok_and(|now| now == ticks);
        if !matches() {
            return Err(fail("sentry identity changed before fencing"));
        }
        // SAFETY: pidfd_open(pid, 0) returns a new descriptor or -1.
        let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::c_int, 0) };
        if raw < 0 {
            return Err(fail("could not open sentry pidfd"));
        }
        // SAFETY: raw is a fresh descriptor we own.
        let fd = unsafe { OwnedFd::from_raw_fd(raw as i32) };
        if !matches() {
            return Err(fail("sentry identity changed while fencing"));
        }
        Ok(PidFd { fd, pid, ticks, proc_root: proc_root.to_path_buf() })
    }

    fn readable(&self, timeout_ms: i32) -> bool {
        let mut poll = libc::pollfd { fd: self.fd.as_raw_fd(), events: libc::POLLIN, revents: 0 };
        // SAFETY: one valid pollfd.
        unsafe { libc::poll(&mut poll, 1, timeout_ms) > 0 }
    }

    pub fn alive(&self) -> bool {
        !self.readable(0) && runsc::start_time_ticks(&self.proc_root, self.pid).is_ok_and(|now| now == self.ticks)
    }

    fn wait_exit(&self, timeout: Duration) -> bool {
        self.readable(timeout.as_millis().clamp(1, i32::MAX as u128) as i32)
    }

    pub fn terminate(&self, timeout: Duration) -> Result<(), WardenError> {
        if self.alive() {
            // SAFETY: pidfd_send_signal(fd, SIGKILL, NULL, 0).
            let sent = unsafe {
                libc::syscall(libc::SYS_pidfd_send_signal, self.fd.as_raw_fd(), libc::SIGKILL, std::ptr::null::<libc::siginfo_t>(), 0)
            };
            if sent != 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
                return Err(fail("could not signal the fenced process"));
            }
        }
        if !self.wait_exit(timeout) {
            return Err(fail("fenced process did not exit"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_separators_match_python_json_dump() {
        assert_eq!(python_default_separators(r#"{"id":"a,b:c","sandbox":{"pid":0},"goferPid":0}"#),
                   r#"{"id": "a,b:c", "sandbox": {"pid": 0}, "goferPid": 0}"#);
    }

    #[test]
    fn pidfd_fences_and_kills_only_the_pinned_process() {
        let mut child = std::process::Command::new("sleep").arg("30").spawn().unwrap();
        let pid = child.id();
        let ticks = runsc::start_time_ticks(Path::new("/proc"), pid).unwrap();
        assert!(PidFd::open(Path::new("/proc"), pid, ticks + 1).is_err());
        let handle = PidFd::open(Path::new("/proc"), pid, ticks).unwrap();
        assert!(handle.alive());
        handle.terminate(Duration::from_secs(5)).unwrap();
        let _ = child.wait();
        assert!(!handle.alive());
    }
}
