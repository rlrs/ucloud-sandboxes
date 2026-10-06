//! runsc invocation and sentry identity, as ucloud_sandboxes/direct_warden.py
//! (`SubprocessCommandRunner`, `_runsc_state`, `_sentry_identity`) and
//! runtime_process.py (`owned_runtime_process_ticks`) do them.

use std::ffi::CString;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::FromRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde_json::Value;

use crate::fsutil::fsync_dir;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandResult {
    pub argv: Vec<String>,
    /// The exit status, or -signal when killed by one (Python's returncode).
    pub returncode: i32,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug)]
pub enum RunscError {
    /// A command exited nonzero (Python: DirectWardenError "command failed ...").
    Failed(CommandResult),
    Timeout(Vec<String>),
    /// The sentry process exited or is gone.
    ProcessLookup(u32),
    Invalid(String),
    Io(io::Error),
}

impl std::fmt::Display for RunscError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunscError::Failed(result) => write!(
                f,
                "command failed ({}): {:?}; stdout={:?}; stderr={:?}",
                result.returncode, result.argv, result.stdout, result.stderr
            ),
            RunscError::Timeout(argv) => write!(f, "command timed out: {argv:?}"),
            RunscError::ProcessLookup(pid) => write!(f, "process {pid} is gone"),
            RunscError::Invalid(message) => f.write_str(message),
            RunscError::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for RunscError {}

impl From<io::Error> for RunscError {
    fn from(error: io::Error) -> Self {
        RunscError::Io(error)
    }
}

fn memfd() -> io::Result<File> {
    let name = CString::new("sandbox-command").expect("no NUL");
    // SAFETY: a valid C string; the returned descriptor is owned by the File.
    let fd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fd is a fresh, owned descriptor.
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn read_all(mut file: File) -> io::Result<String> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// Run a command with stdout and stderr in memfds, never pipes: runsc create
/// and restore daemonize a sentry and gofer that inherit the descriptors, and
/// waiting for a pipe's EOF would wait for them. stdin is inherited, as in Python.
pub async fn run(argv: &[String], timeout: Duration) -> Result<CommandResult, RunscError> {
    let (stdout, stderr) = (memfd()?, memfd()?);
    let mut child = tokio::process::Command::new(&argv[0])
        .args(&argv[1..])
        .stdout(Stdio::from(stdout.try_clone()?))
        .stderr(Stdio::from(stderr.try_clone()?))
        .kill_on_drop(true)
        .spawn()?;
    let status = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(status) => status?,
        Err(_) => {
            let _ = child.kill().await;
            return Err(RunscError::Timeout(argv.to_vec()));
        }
    };
    use std::os::unix::process::ExitStatusExt;
    let returncode = status.code().unwrap_or_else(|| -status.signal().unwrap_or(0));
    Ok(CommandResult { argv: argv.to_vec(), returncode, stdout: read_all(stdout)?, stderr: read_all(stderr)? })
}

/// Python `_checked`: a nonzero exit is an error carrying the whole result.
pub async fn checked(argv: &[String], timeout: Duration) -> Result<CommandResult, RunscError> {
    let result = run(argv, timeout).await?;
    if result.returncode != 0 {
        return Err(RunscError::Failed(result));
    }
    Ok(result)
}

/// `runsc state` stdout: (pid, status).
pub fn parse_state(stdout: &str) -> Result<(u32, String), RunscError> {
    let invalid = || RunscError::Invalid("runsc state returned invalid JSON".into());
    let payload: Value = serde_json::from_str(stdout).map_err(|_| invalid())?;
    let pid = match &payload["pid"] {
        Value::Number(number) => number.as_u64().and_then(|n| u32::try_from(n).ok()).ok_or_else(invalid)?,
        Value::String(text) => text.trim().parse().map_err(|_| invalid())?,
        _ => return Err(invalid()),
    };
    let status = match &payload["status"] {
        Value::String(status) => status.clone(),
        Value::Null => return Err(invalid()),
        other => other.to_string(),
    };
    Ok((pid, status))
}

/// /proc/<pid>/stat field 22 (starttime), located after the last ')'.
pub fn start_time_ticks(proc_root: &Path, pid: u32) -> Result<u64, RunscError> {
    let raw = std::fs::read(proc_root.join(pid.to_string()).join("stat")).map_err(|_| RunscError::ProcessLookup(pid))?;
    let raw = String::from_utf8(raw).map_err(|_| RunscError::ProcessLookup(pid))?;
    let closing = raw.rfind(')').filter(|&c| c >= 2 && c + 2 < raw.len())
        .ok_or_else(|| RunscError::Invalid("process stat has an invalid format".into()))?;
    let ticks: u64 = raw[closing + 2..]
        .split_whitespace()
        .nth(19)
        .and_then(|field| field.parse().ok())
        .ok_or_else(|| RunscError::Invalid("process stat is missing starttime".into()))?;
    if ticks == 0 {
        return Err(RunscError::Invalid("process start time must be positive".into()));
    }
    Ok(ticks)
}

fn exited(proc_root: &Path, pid: u32) -> bool {
    match std::fs::read_to_string(proc_root.join(pid.to_string()).join("stat")) {
        Err(_) => true,
        Ok(raw) => match raw.rfind(')') {
            Some(c) => matches!(raw[c + 1..].split_whitespace().next(), Some("Z" | "X")),
            None => false,
        },
    }
}

fn unique_flag<'a>(argv: &'a [String], name: &str) -> Option<&'a str> {
    let prefix = format!("{name}=");
    let mut values = Vec::new();
    for (index, arg) in argv.iter().enumerate() {
        if let Some(value) = arg.strip_prefix(&prefix) {
            values.push(value);
        } else if arg == name && index + 1 < argv.len() {
            values.push(argv[index + 1].as_str());
        }
    }
    if values.len() == 1 { Some(values[0]) } else { None }
}

fn same_file(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (std::fs::metadata(a), std::fs::metadata(b)) {
        (Ok(x), Ok(y)) => x.dev() == y.dev() && x.ino() == y.ino(),
        _ => false,
    }
}

/// Where a sandbox's sentry must come from.
pub struct SentryOwner<'a> {
    pub proc_root: &'a Path,
    pub runsc: &'a Path,
    pub runtime_root: &'a Path,
    pub bundle: &'a Path,
    pub container_id: &'a str,
}

/// `owned_runtime_process_ticks(role="sandbox")`: the process's command line,
/// executable and cgroup name this container, bracketed by unchanged ticks.
pub fn owned_sentry_ticks(owner: &SentryOwner, pid: u32, expected: Option<u64>) -> Result<u64, RunscError> {
    if pid <= 1 {
        return Err(RunscError::Invalid("runtime process PID must be greater than one".into()));
    }
    let ticks = start_time_ticks(owner.proc_root, pid)?;
    if expected.is_some_and(|expected| expected != ticks) {
        return Err(RunscError::Invalid("runtime process start time changed".into()));
    }
    let process = owner.proc_root.join(pid.to_string());
    let verify = || -> Result<(), RunscError> {
        let mut raw = Vec::new();
        File::open(process.join("cmdline"))?.take(65537).read_to_end(&mut raw)?;
        if raw.is_empty() {
            return Err(RunscError::Invalid("runtime process has no command line".into()));
        }
        if raw.len() > 65536 || raw.last() != Some(&0) {
            return Err(RunscError::Invalid("runtime process command line is invalid".into()));
        }
        let argv: Vec<String> = raw[..raw.len() - 1]
            .split(|&b| b == 0)
            .map(|arg| String::from_utf8(arg.to_vec()))
            .collect::<Result<_, _>>()
            .map_err(|_| RunscError::Invalid("runtime process command line is invalid".into()))?;
        let runtime_root = owner.runtime_root.to_string_lossy();
        let bundle = owner.bundle.to_string_lossy();
        if argv[0] != "runsc-sandbox"
            || argv.last().map(String::as_str) != Some(owner.container_id)
            || argv.iter().filter(|arg| *arg == "boot").count() != 1
            || unique_flag(&argv, "--root") != Some(runtime_root.as_ref())
            || unique_flag(&argv, "--bundle") != Some(bundle.as_ref())
        {
            return Err(RunscError::Invalid("runtime process invocation has another owner".into()));
        }
        let binary = std::fs::canonicalize(owner.runsc)?;
        let sidecar = binary.parent().map(|dir| dir.join("gvisor-bin").join("gvisor_sentry"));
        let exe = process.join("exe");
        if ![Some(binary), sidecar].into_iter().flatten().any(|candidate| candidate.is_file() && same_file(&exe, &candidate)) {
            return Err(RunscError::Invalid("runtime process executable is not trusted".into()));
        }
        let groups = std::fs::read_to_string(process.join("cgroup"))?;
        let owned = groups.lines().any(|line| {
            let fields: Vec<&str> = line.splitn(3, ':').collect();
            fields.len() == 3 && fields[2].trim_end_matches('/').rsplit('/').next() == Some(owner.container_id)
        });
        if !owned {
            return Err(RunscError::Invalid("runtime process cgroup has another owner".into()));
        }
        if start_time_ticks(owner.proc_root, pid)? != ticks {
            return Err(RunscError::Invalid("runtime process changed during verification".into()));
        }
        Ok(())
    };
    match verify() {
        Ok(()) => Ok(ticks),
        Err(_) if exited(owner.proc_root, pid) => Err(RunscError::ProcessLookup(pid)),
        Err(error) => Err(error),
    }
}

/// The current boot id, normalized like Python's `str(UUID(...))`.
pub fn boot_id(proc_root: &Path) -> Result<String, RunscError> {
    let raw = std::fs::read_to_string(proc_root.join("sys/kernel/random/boot_id"))?;
    let hex: String = raw.trim().chars().filter(|c| *c != '-').collect::<String>().to_ascii_lowercase();
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(RunscError::Invalid("invalid boot id".into()));
    }
    Ok(format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32]))
}

/// `_sentry_identity`: verify the sentry, then record which boot it belongs to
/// (`<runtime_root>/warden-process-owners/<container_id>`), once.
pub fn sentry_identity(owner: &SentryOwner, pid: u32, expected: Option<u64>) -> Result<u64, RunscError> {
    let boot = boot_id(owner.proc_root)?;
    let marker: PathBuf = owner.runtime_root.join("warden-process-owners").join(owner.container_id);
    if marker.exists() && std::fs::read_to_string(&marker)?.trim() != boot {
        return Err(RunscError::Invalid("runtime identity belongs to another boot".into()));
    }
    let ticks = owned_sentry_ticks(owner, pid, expected)?;
    if !marker.exists() {
        let parent = marker.parent().expect("has a parent");
        std::fs::DirBuilder::new().mode_0700().create_if_missing(parent)?;
        let mut file = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(&marker)?;
        file.write_all(format!("{boot}\n").as_bytes())?;
        file.sync_all()?;
        fsync_dir(parent)?;
    }
    Ok(ticks)
}

trait DirBuilderPrivate {
    fn mode_0700(&mut self) -> &mut Self;
    fn create_if_missing(&self, path: &Path) -> io::Result<()>;
}

impl DirBuilderPrivate for std::fs::DirBuilder {
    fn mode_0700(&mut self) -> &mut Self {
        use std::os::unix::fs::DirBuilderExt;
        self.mode(0o700)
    }
    fn create_if_missing(&self, path: &Path) -> io::Result<()> {
        match self.create(path) {
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn runs_with_captured_output_and_checks_failures() {
        let ok = run(&args(&["sh", "-c", "echo out; echo err >&2"]), Duration::from_secs(5)).await.unwrap();
        assert_eq!((ok.returncode, ok.stdout.as_str(), ok.stderr.as_str()), (0, "out\n", "err\n"));
        match checked(&args(&["sh", "-c", "echo nope >&2; exit 3"]), Duration::from_secs(5)).await {
            Err(RunscError::Failed(result)) => assert_eq!((result.returncode, result.stderr.as_str()), (3, "nope\n")),
            other => panic!("{other:?}"),
        }
        assert!(matches!(run(&args(&["sleep", "5"]), Duration::from_millis(100)).await, Err(RunscError::Timeout(_))));
    }

    #[tokio::test]
    async fn a_daemonized_child_holding_stdout_does_not_block_completion() {
        let started = std::time::Instant::now();
        let result = run(&args(&["sh", "-c", "(sleep 3 &) ; echo done"]), Duration::from_secs(10)).await.unwrap();
        assert_eq!(result.stdout, "done\n");
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn parses_runsc_state_and_proc_stat() {
        assert_eq!(parse_state(r#"{"id":"c","pid":4242,"status":"running"}"#).unwrap(), (4242, "running".to_string()));
        assert!(parse_state(r#"{"status":"running"}"#).is_err());
        let ticks = start_time_ticks(Path::new("/proc"), std::process::id()).unwrap();
        assert!(ticks > 0);
        let boot = boot_id(Path::new("/proc")).unwrap();
        assert_eq!(boot.len(), 36);
    }
}
