//! The split memory-backing allocator's create path, as
//! ucloud_sandboxes/memory_backing.py's `MemoryBackingStore` does it: the same
//! SQLite journal (`memory-backing.sqlite`, schema version 3), the same
//! per-allocation mutation flocks, ownership markers, XFS project quotas and
//! RAM directories. The Python agent keeps opening the same journal: it parks,
//! wakes, retains checkpoints, deletes and changes limits; the daemon only
//! prepares and requires allocations for the sandboxes it creates.
//!
//! Every method blocks (flock, SQLite busy waits of up to 30 s, fsync,
//! quota syscalls or `xfs_quota`): call them from a blocking thread.
//!
//! The journal's columns are read by position, as Python reads `SELECT *`:
//! `0 allocation_id, 1 sandbox_id, 2 generation, 3 project_id, 4 quota_bytes,
//! 5 state, 6 active_mode, 7 limit_bytes`. Both processes group-commit
//! their writes (savepoints in one `BEGIN IMMEDIATE` transaction per group,
//! `JournalWriter` here) on their own connections, so SQLite's file locks
//! serialize the two processes.

use std::collections::HashMap;
use std::ffi::{CStr, CString, OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::Value;

use crate::fsutil::{FileLock, euid, fsync_dir};

/// The ownership marker inside each allocation directory.
pub const MARKER: &str = ".memory-owner.json";
/// `PRAGMA user_version` this code writes; versions 0 to 3 are migrated on open.
pub const JOURNAL_VERSION: i64 = 3;
/// The first XFS project id the shared counter hands out.
pub const FIRST_PROJECT_ID: i64 = 600_000;
/// Python's `sqlite3.connect(timeout=30)`.
const BUSY_TIMEOUT: Duration = Duration::from_secs(30);
/// Marker files are tiny; anything larger is not ours.
const MAX_MARKER_BYTES: u64 = 64 * 1024;

#[derive(Debug)]
pub enum MemoryBackingError {
    /// Python `MemoryBackingError` (a RuntimeError): 503 `{"error"}` on create.
    Backing(String),
    /// Python `MemoryBackingBusyError`: 503 `memory_publication_draining`,
    /// `Retry-After: 1`. Only publication-reader locks raise it; the create
    /// path never takes them.
    Busy(String),
    /// Python `ValueError` (invalid identities, configuration, an unparsable
    /// marker's `JSONDecodeError`): 400 `{"error"}` on create.
    Invalid(String),
    /// Python `OSError`: not a RuntimeError, so the agent's handler does not
    /// map it.
    Io(io::Error),
    /// Python `sqlite3.Error`, likewise unmapped.
    Sqlite(rusqlite::Error),
    /// Python `subprocess.CalledProcessError`, likewise unmapped.
    Command { argv: Vec<String>, status: Option<i32>, stderr: String },
}

impl MemoryBackingError {
    /// The Python exception class the agent would have raised.
    pub fn python_class(&self) -> &'static str {
        match self {
            MemoryBackingError::Backing(_) => "MemoryBackingError",
            MemoryBackingError::Busy(_) => "MemoryBackingBusyError",
            MemoryBackingError::Invalid(_) => "ValueError",
            MemoryBackingError::Io(_) => "OSError",
            MemoryBackingError::Sqlite(_) => "sqlite3.Error",
            MemoryBackingError::Command { .. } => "CalledProcessError",
        }
    }
}

impl std::fmt::Display for MemoryBackingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryBackingError::Backing(m) | MemoryBackingError::Busy(m) | MemoryBackingError::Invalid(m) => {
                f.write_str(m)
            }
            MemoryBackingError::Io(e) => write!(f, "{e}"),
            MemoryBackingError::Sqlite(e) => write!(f, "{e}"),
            MemoryBackingError::Command { argv, status, stderr } => {
                write!(f, "Command '{argv:?}' returned non-zero exit status {}.", status.unwrap_or(-1))?;
                if !stderr.trim().is_empty() {
                    write!(f, " {}", stderr.trim())?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for MemoryBackingError {}

impl From<io::Error> for MemoryBackingError {
    fn from(error: io::Error) -> Self {
        MemoryBackingError::Io(error)
    }
}

impl From<rusqlite::Error> for MemoryBackingError {
    fn from(error: rusqlite::Error) -> Self {
        MemoryBackingError::Sqlite(error)
    }
}

pub type Result<T> = std::result::Result<T, MemoryBackingError>;

fn fail<T>(message: &str) -> Result<T> {
    Err(MemoryBackingError::Backing(message.to_string()))
}

/// `[A-Za-z0-9][A-Za-z0-9_.:-]{0,239}` (checkpoint_components.py `_ID`).
pub fn is_allocation_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 240
        && bytes[0].is_ascii_alphanumeric()
        && bytes.iter().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'))
}

/// Python `MemoryBackingRef`: the allocation's identity and its
/// identity-bearing ceiling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryBackingRef {
    pub allocation_id: String,
    pub quota_bytes: u64,
}

impl MemoryBackingRef {
    pub fn new(allocation_id: impl Into<String>, quota_bytes: u64) -> Result<Self> {
        let allocation_id = allocation_id.into();
        if !is_allocation_id(&allocation_id) {
            return Err(MemoryBackingError::Invalid("invalid memory allocation identity".into()));
        }
        // SQLite integers are 64-bit signed.
        if quota_bytes == 0 || quota_bytes > i64::MAX as u64 {
            return Err(MemoryBackingError::Invalid("invalid memory allocation quota".into()));
        }
        Ok(MemoryBackingRef { allocation_id, quota_bytes })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActiveMode {
    Ram,
    File,
}

impl ActiveMode {
    pub fn as_str(self) -> &'static str {
        match self {
            ActiveMode::Ram => "ram",
            ActiveMode::File => "file",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryBackingLease {
    pub reference: MemoryBackingRef,
    pub sandbox_id: String,
    pub sandbox_generation: u64,
    pub project_id: i64,
    /// `<root>/<allocation_id>`.
    pub path: PathBuf,
    pub active_mode: ActiveMode,
}

/// The Linux boundary: `findmnt`, the project-quota syscalls (or
/// `xfs_quota`) and `FS_IOC_FSGETXATTR`. [`XfsMemoryQuota`] is the real one;
/// tests inject fakes.
pub trait MemoryQuota: Send + Sync {
    /// Called once on open, after `root` exists (Python `validate_root`).
    fn validate_root(&mut self, root: &Path) -> Result<()>;
    /// The RAM root must be tmpfs, with `noswap` unless `ram_swappable`.
    fn validate_active_root(&self, active_root: &Path, ram_swappable: bool) -> Result<()>;
    /// Assign the project, set its limits, then [`MemoryQuota::validate_project`].
    fn provision(&self, path: &Path, project_id: i64, limit_bytes: i64) -> Result<()>;
    fn validate_project(&self, path: &Path, project_id: i64) -> Result<()>;
}

/// How [`XfsMemoryQuota::provision`] reaches the kernel.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum QuotaBackend {
    /// `FS_IOC_FSSETXATTR` and `quotactl(Q_XSETQLIM)`, the calls `xfs_quota`
    /// itself makes; `xfs_quota` when the kernel refuses them (ENOSYS,
    /// EINVAL) or the filesystem's block device cannot be proven.
    #[default]
    Syscalls,
    /// Python's two `xfs_quota` processes, always.
    XfsQuota,
}

/// Python `XfsMemoryQuota` (create-path half).
#[derive(Debug, Default)]
pub struct XfsMemoryQuota {
    backend: QuotaBackend,
    filesystem_root: Option<PathBuf>,
    /// The filesystem's `st_dev` (the project walk stays on it, as
    /// `nftw(FTW_MOUNT)` does) and a block device node proven to be it.
    filesystem: Option<(u64, Option<CString>)>,
    fallback_logged: AtomicBool,
}

impl XfsMemoryQuota {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_backend(backend: QuotaBackend) -> Self {
        XfsMemoryQuota { backend, ..Self::default() }
    }

    /// The mountpoint `xfs_quota` runs against, once validated.
    pub fn filesystem_root(&self) -> Option<&Path> {
        self.filesystem_root.as_deref()
    }

    /// The block device `quotactl` addresses, once validated and proven.
    pub fn block_device(&self) -> Option<&CStr> {
        self.filesystem.as_ref().and_then(|(_, device)| device.as_deref())
    }

    fn mountpoint(&self) -> Result<&Path> {
        self.filesystem_root.as_deref().ok_or_else(|| MemoryBackingError::Backing("memory backing root was not validated".into()))
    }

    fn log_fallback(&self, reason: &dyn std::fmt::Display) {
        if !self.fallback_logged.swap(true, Ordering::SeqCst) {
            eprintln!("ucloud-noded: memory backing quotas fall back to xfs_quota: {reason}");
        }
    }

    fn provision_xfs_quota(&self, path: &Path, project_id: i64, limit_bytes: i64) -> Result<()> {
        for argv in provision_commands(self.mountpoint()?, path, project_id, limit_bytes) {
            run_checked(&argv)?;
        }
        Ok(())
    }
}

/// The two `xfs_quota` invocations `provision` runs, exactly as Python builds them.
pub fn provision_commands(filesystem_root: &Path, path: &Path, project_id: i64, limit_bytes: i64) -> [Vec<OsString>; 2] {
    let mut project = OsString::from("project -s -p ");
    project.push(path.as_os_str());
    project.push(format!(" {project_id}"));
    let limit = OsString::from(format!("limit -p bsoft={limit_bytes} bhard={limit_bytes} {project_id}"));
    [project, limit].map(|command| {
        vec!["xfs_quota".into(), "-x".into(), "-c".into(), command, filesystem_root.as_os_str().to_owned()]
    })
}

/// `subprocess.run(argv, check=True, capture_output=True, text=True).stdout`.
fn run_checked(argv: &[OsString]) -> Result<String> {
    let output = Command::new(&argv[0]).args(&argv[1..]).stdin(Stdio::null()).output()?;
    if !output.status.success() {
        return Err(MemoryBackingError::Command {
            argv: argv.iter().map(|a| a.to_string_lossy().into_owned()).collect(),
            status: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn findmnt(columns: &str, target: &Path) -> Result<String> {
    run_checked(&["findmnt", "-n", "-o", columns, "--target"].map(OsString::from).into_iter().chain([target.as_os_str().to_owned()]).collect::<Vec<_>>())
}

/// `struct fsxattr` (linux/fs.h), 28 bytes.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FsXattr {
    pub xflags: u32,
    pub extsize: u32,
    pub nextents: u32,
    pub projid: u32,
    pub cowextsize: u32,
    pub pad: [u8; 8],
}

/// `struct fs_disk_quota` (linux/dqblk_xfs.h), 112 bytes.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FsDiskQuota {
    pub d_version: i8,
    pub d_flags: i8,
    pub d_fieldmask: u16,
    pub d_id: u32,
    pub d_blk_hardlimit: u64,
    pub d_blk_softlimit: u64,
    pub d_ino_hardlimit: u64,
    pub d_ino_softlimit: u64,
    pub d_bcount: u64,
    pub d_icount: u64,
    pub d_itimer: i32,
    pub d_btimer: i32,
    pub d_iwarns: u16,
    pub d_bwarns: u16,
    pub d_itimer_hi: i8,
    pub d_btimer_hi: i8,
    pub d_rtbtimer_hi: i8,
    pub d_padding2: i8,
    pub d_rtb_hardlimit: u64,
    pub d_rtb_softlimit: u64,
    pub d_rtbcount: u64,
    pub d_rtbtimer: i32,
    pub d_rtbwarns: u16,
    pub d_padding3: i16,
    pub d_padding4: [u8; 8],
}

const _: () = assert!(std::mem::size_of::<FsXattr>() == 28 && std::mem::size_of::<FsDiskQuota>() == 112);

/// `_IOR('X', 31, struct fsxattr)`.
pub const FS_IOC_FSGETXATTR: libc::c_ulong = 0x801C_581F;
/// `_IOW('X', 32, struct fsxattr)`.
pub const FS_IOC_FSSETXATTR: libc::c_ulong = 0x401C_5820;
pub const FS_XFLAG_PROJINHERIT: u32 = 0x200;
/// `XQM_CMD(4)`: set an XFS dquot's limits.
pub const Q_XSETQLIM: u32 = ((b'X' as u32) << 8) + 4;
pub const PRJQUOTA: u32 = 2;
pub const FS_DQUOT_VERSION: i8 = 1;
pub const FS_PROJ_QUOTA: i8 = 2;
pub const FS_DQ_BSOFT: u16 = 1 << 2;
pub const FS_DQ_BHARD: u16 = 1 << 3;
/// XFS quota limits are in 512-byte basic blocks.
const BBSHIFT: u32 = 9;

/// `QCMD(cmd, type)` (linux/quota.h).
pub const fn qcmd(cmd: u32, kind: u32) -> u32 {
    (cmd << 8) | (kind & 0x00ff)
}

impl FsDiskQuota {
    /// What `xfs_quota -c "limit -p bsoft=N bhard=N ID"` passes: both block
    /// limits, in basic blocks rounded down (`extractb`).
    pub fn project_block_limits(project_id: u32, limit_bytes: u64) -> FsDiskQuota {
        let blocks = limit_bytes >> BBSHIFT;
        FsDiskQuota {
            d_version: FS_DQUOT_VERSION,
            d_flags: FS_PROJ_QUOTA,
            d_fieldmask: FS_DQ_BSOFT | FS_DQ_BHARD,
            d_id: project_id,
            d_blk_hardlimit: blocks,
            d_blk_softlimit: blocks,
            ..FsDiskQuota::default()
        }
    }
}

/// `struct fsxattr` for an open file.
pub fn get_fsxattr(file: &File) -> io::Result<FsXattr> {
    let mut attributes = FsXattr::default();
    // SAFETY: FS_IOC_FSGETXATTR writes one struct fsxattr, which FsXattr is.
    if unsafe { libc::ioctl(file.as_raw_fd(), FS_IOC_FSGETXATTR as _, &mut attributes as *mut FsXattr) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(attributes)
}

/// `struct fsxattr`'s `(fsx_xflags, fsx_projid)` for an open file.
pub fn fsxattr(file: &File) -> io::Result<(u32, u32)> {
    get_fsxattr(file).map(|attributes| (attributes.xflags, attributes.projid))
}

/// `xfs_quota`'s `setup_project` for one inode: read the attributes, set the
/// project and PROJINHERIT (XFS keeps the flag on directories only), write
/// them back.
fn assign_project(file: &File, project_id: u32) -> io::Result<()> {
    let mut attributes = get_fsxattr(file)?;
    attributes.projid = project_id;
    attributes.xflags |= FS_XFLAG_PROJINHERIT;
    // SAFETY: FS_IOC_FSSETXATTR reads one struct fsxattr, which FsXattr is.
    if unsafe { libc::ioctl(file.as_raw_fd(), FS_IOC_FSSETXATTR as _, &attributes as *const FsXattr) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// `project -s -p PATH ID`: `nftw(PATH, FTW_PHYS | FTW_MOUNT)` in preorder,
/// assigning every directory and regular file on the filesystem and
/// skipping symlinks and special files. Unlike `xfs_quota`, the first
/// failure stops the walk.
fn assign_project_tree(path: &Path, filesystem: u64, project_id: u32) -> io::Result<()> {
    let meta = fs::symlink_metadata(path)?;
    let kind = meta.file_type();
    if meta.dev() != filesystem || !(kind.is_dir() || kind.is_file()) {
        return Ok(());
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NOCTTY | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)?;
    // The open file must be the inode that was classified.
    let opened = file.metadata()?;
    if (opened.dev(), opened.ino()) != (meta.dev(), meta.ino()) {
        return Err(io::Error::other(format!("{} changed during project assignment", path.display())));
    }
    assign_project(&file, project_id)?;
    if kind.is_dir() {
        for entry in fs::read_dir(path)? {
            assign_project_tree(&entry?.path(), filesystem, project_id)?;
        }
    }
    Ok(())
}

/// `quotactl(QCMD(Q_XSETQLIM, PRJQUOTA), device, id, &fs_disk_quota)`,
/// through `syscall(2)`: musl's wrapper is not relied upon.
fn set_project_limit(device: &CStr, project_id: u32, limit_bytes: u64) -> io::Result<()> {
    let mut quota = FsDiskQuota::project_block_limits(project_id, limit_bytes);
    // SAFETY: the kernel reads one struct fs_disk_quota, which FsDiskQuota
    // is, and a NUL-terminated device path; both outlive the call.
    let status = unsafe {
        libc::syscall(
            libc::SYS_quotactl,
            qcmd(Q_XSETQLIM, PRJQUOTA) as libc::c_int,
            device.as_ptr(),
            project_id as libc::c_int,
            &mut quota as *mut FsDiskQuota,
        )
    };
    if status != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// The kernel cannot do this through the syscall path at all.
pub fn syscall_unsupported(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::ENOSYS) | Some(libc::EINVAL))
}

/// Run `syscalls`; when the kernel refuses them as unsupported, run
/// `fallback` instead (it redoes every step, which is idempotent) and log the
/// first time. Any other failure is the caller's. True when it fell back.
pub fn provision_with_fallback(
    syscalls: impl FnOnce() -> io::Result<()>,
    fallback: impl FnOnce() -> Result<()>,
    log: impl FnOnce(&io::Error),
) -> Result<bool> {
    match syscalls() {
        Ok(()) => Ok(false),
        Err(error) if syscall_unsupported(&error) => {
            log(&error);
            fallback().map(|()| true)
        }
        Err(error) => Err(error.into()),
    }
}

/// glibc's `gnu_dev_major`/`gnu_dev_minor`, in Rust: build_pinned.sh wants
/// every `libc::` call linked, and libc's `major`/`minor` are not symbols.
pub fn device_numbers(device: u64) -> (u64, u64) {
    let major = ((device >> 32) & 0xffff_f000) | ((device >> 8) & 0x0fff);
    let minor = ((device >> 12) & 0xffff_ff00) | (device & 0x00ff);
    (major, minor)
}

/// A block device node whose `st_rdev` is the filesystem's `st_dev`: findmnt's
/// SOURCE (less a bind mount's `[/path]`), else udev's `/dev/block/MAJ:MIN`.
fn block_device(root: &Path, filesystem: u64) -> Option<CString> {
    let source = findmnt("SOURCE", root).ok().map(|s| s.trim().split('[').next().unwrap_or("").to_string());
    let (major, minor) = device_numbers(filesystem);
    let fallback = format!("/dev/block/{major}:{minor}");
    source.into_iter().chain([fallback]).find_map(|candidate| {
        let meta = fs::metadata(&candidate).ok()?;
        (meta.file_type().is_block_device() && meta.rdev() == filesystem).then(|| CString::new(candidate).ok()).flatten()
    })
}

impl MemoryQuota for XfsMemoryQuota {
    fn validate_root(&mut self, root: &Path) -> Result<()> {
        let output = findmnt("FSTYPE,OPTIONS", root)?;
        let fields: Vec<&str> = output.split_whitespace().collect();
        if fields.len() != 2 || fields[0] != "xfs" || !fields[1].split(',').any(|o| o == "prjquota" || o == "pquota") {
            return fail("memory backing requires an XFS project-quota filesystem");
        }
        // xfs_quota accepts a mountpoint, not an arbitrary directory within it.
        let target = PathBuf::from(findmnt("TARGET", root)?.trim());
        if !target.is_absolute() {
            return fail("memory backing mountpoint is invalid");
        }
        self.filesystem_root = Some(target);
        if self.backend == QuotaBackend::Syscalls {
            let filesystem = fs::metadata(root)?.dev();
            let device = block_device(root, filesystem);
            if device.is_none() {
                self.log_fallback(&format_args!("no block device node for {}", root.display()));
            }
            self.filesystem = Some((filesystem, device));
        }
        Ok(())
    }

    fn validate_active_root(&self, active_root: &Path, ram_swappable: bool) -> Result<()> {
        let output = findmnt("FSTYPE,OPTIONS", active_root)?;
        let fields: Vec<&str> = output.split_whitespace().collect();
        if fields.len() != 2 || fields[0] != "tmpfs" || fields[1].split(',').any(|o| o == "noswap") == ram_swappable {
            return fail("active RAM backing requires tmpfs, swappable only on pause-tier nodes");
        }
        Ok(())
    }

    /// `xfs_quota -c "project -s -p PATH ID"` then `-c "limit -p bsoft=L
    /// bhard=L ID"`, as the syscalls those make, then the same validation.
    fn provision(&self, path: &Path, project_id: i64, limit_bytes: i64) -> Result<()> {
        let xfs_quota = || self.provision_xfs_quota(path, project_id, limit_bytes);
        match &self.filesystem {
            Some((filesystem, Some(device))) if self.backend == QuotaBackend::Syscalls => {
                let syscalls = || {
                    let invalid = |what| io::Error::new(io::ErrorKind::InvalidInput, format!("memory allocation {what} is out of range"));
                    let project = u32::try_from(project_id).map_err(|_| invalid("project id"))?;
                    let limit = u64::try_from(limit_bytes).map_err(|_| invalid("limit"))?;
                    assign_project_tree(path, *filesystem, project)?;
                    set_project_limit(device, project, limit)
                };
                provision_with_fallback(syscalls, xfs_quota, |error| self.log_fallback(error))?;
            }
            _ => xfs_quota()?,
        }
        self.validate_project(path, project_id)
    }

    fn validate_project(&self, path: &Path, project_id: i64) -> Result<()> {
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let (flags, actual) = fsxattr(&directory)?;
        if i64::from(actual) != project_id || flags & FS_XFLAG_PROJINHERIT == 0 {
            return fail("memory directory lost project-quota ownership");
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct MemoryBackingConfig {
    /// `--volume-mount-root`: the XFS prjquota directory holding allocations.
    pub root: PathBuf,
    /// `<state_root>/memory-backing.sqlite`.
    pub journal: PathBuf,
    /// `--memory-backing-hard-capacity-bytes`.
    pub hard_capacity_bytes: u64,
    /// `--application-memory-root` (tmpfs), when RAM backing is configured.
    pub active_root: Option<PathBuf>,
    /// `--pause-tier`: the RAM root may be swappable.
    pub ram_swappable: bool,
}

/// One `SELECT *` row, by position. Identity columns stay raw SQLite values
/// so a legacy row of another type compares unequal, as in Python.
#[derive(Debug)]
struct AllocationRow {
    sandbox_id: SqlValue,
    generation: SqlValue,
    project_id: i64,
    quota_bytes: SqlValue,
    state: Option<String>,
    active_mode: Option<String>,
    limit_bytes: i64,
}

impl AllocationRow {
    fn owned_by(&self, sandbox_id: &str, generation: u64, quota_bytes: u64) -> bool {
        self.sandbox_id == SqlValue::Text(sandbox_id.to_string())
            && i64::try_from(generation).is_ok_and(|g| self.generation == SqlValue::Integer(g))
            && i64::try_from(quota_bytes).is_ok_and(|q| self.quota_bytes == SqlValue::Integer(q))
    }

    fn state_is(&self, state: &str) -> bool {
        self.state.as_deref() == Some(state)
    }
}

fn text(value: SqlValue) -> Option<String> {
    match value {
        SqlValue::Text(text) => Some(text),
        _ => None,
    }
}

fn select_allocation(conn: &Connection, allocation_id: &str) -> Result<Option<AllocationRow>> {
    let row = conn
        .query_row("SELECT * FROM allocations WHERE allocation_id=?", [allocation_id], |row| {
            Ok(AllocationRow {
                sandbox_id: row.get(1)?,
                generation: row.get(2)?,
                project_id: row.get(3)?,
                quota_bytes: row.get(4)?,
                state: text(row.get(5)?),
                active_mode: text(row.get(6)?),
                limit_bytes: row.get(7)?,
            })
        })
        .optional()?;
    Ok(row)
}

/// Python's `sqlite3.connect(path, timeout=30)`, plus the writer pragmas when asked.
fn connect(path: &Path, writer: bool) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    if writer {
        let _mode: String = conn.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
        conn.execute_batch("PRAGMA synchronous=FULL")?;
    }
    Ok(conn)
}

/// Python `_private_directory`: lstat, a directory we own, no group or other bits.
fn private_directory(path: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.uid() != euid() || meta.mode() & 0o077 != 0 {
        return fail("memory backing directory is not privately owned");
    }
    Ok(())
}

/// Python `Path.mkdir(mode=0o700, parents=..., exist_ok=True)`: created
/// parents get the default mode, the leaf 0700; an existing directory (even
/// through a symlink) is accepted and left to the private check.
fn mkdir(path: &Path, parents: bool) -> Result<()> {
    if parents
        && let Some(parent) = path.parent()
        && !parent.is_dir()
    {
        fs::DirBuilder::new().recursive(true).create(parent)?;
    }
    match fs::DirBuilder::new().mode(0o700).create(path) {
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => Ok(()),
        result => Ok(result?),
    }
}

/// `os.path.lexists`.
fn lexists(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Python `str.isspace`, which also counts the ASCII separators 0x1c to 0x1f.
fn has_space(path: &Path) -> bool {
    path.to_string_lossy().chars().any(|c| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("strings serialize")
}

/// The marker exactly as `json.dump(data, f, sort_keys=True)` writes it:
/// default separators `", "` and `": "`, no trailing newline.
pub fn marker_bytes(lease: &MemoryBackingLease) -> Vec<u8> {
    format!(
        "{{\"allocation_id\": {}, \"project_id\": {}, \"quota_bytes\": {}, \"sandbox_generation\": {}, \"sandbox_id\": {}, \"version\": 1}}",
        json_string(&lease.reference.allocation_id),
        lease.project_id,
        lease.reference.quota_bytes,
        lease.sandbox_generation,
        json_string(&lease.sandbox_id),
    )
    .into_bytes()
}

/// A JSON number, or a bool as Python's `True == 1` sees it.
fn python_number(value: &Value) -> Option<Value> {
    match value {
        Value::Number(_) => Some(value.clone()),
        Value::Bool(b) => Some(Value::from(u8::from(*b))),
        _ => None,
    }
}

/// Python equality of `json.loads` results: `1 == 1.0 == True`.
fn python_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| python_eq(v, w)))
        }
        (Value::Array(x), Value::Array(y)) => x.len() == y.len() && x.iter().zip(y).all(|(v, w)| python_eq(v, w)),
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Null, Value::Null) => true,
        _ => match (python_number(a), python_number(b)) {
            (Some(Value::Number(x)), Some(Value::Number(y))) => match (x.as_i128(), y.as_i128()) {
                (Some(x), Some(y)) => x == y,
                _ => x.as_f64().zip(y.as_f64()).is_some_and(|(x, y)| x == y),
            },
            _ => false,
        },
    }
}

/// `marker.is_symlink() or json.loads(marker.read_text()) != expected`, inverted.
/// A missing marker is Python's FileNotFoundError.
fn marker_matches(marker: &Path, lease: &MemoryBackingLease) -> Result<bool> {
    if fs::symlink_metadata(marker).is_ok_and(|meta| meta.file_type().is_symlink()) {
        return Ok(false);
    }
    let file = OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC).open(marker)?;
    let mut bytes = Vec::new();
    file.take(MAX_MARKER_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_MARKER_BYTES {
        return Ok(false);
    }
    let found: Value = serde_json::from_slice(&bytes).map_err(|e| MemoryBackingError::Invalid(e.to_string()))?;
    let expected: Value = serde_json::from_slice(&marker_bytes(lease)).expect("the marker is JSON");
    Ok(python_eq(&found, &expected))
}

pub struct MemoryBackingStore {
    root: PathBuf,
    journal: PathBuf,
    lease_root: PathBuf,
    hard_capacity_bytes: i64,
    active_root: Option<PathBuf>,
    quota: Box<dyn MemoryQuota>,
    journal_identity: (u64, u64),
    writer: JournalWriter,
    reader: Mutex<Connection>,
    active_modes: Mutex<HashMap<(String, u64), ActiveMode>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panicking journal writer abandons its group, so the state stays usable.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Writers one journal transaction serves at most (Python
/// `DurableSqliteBatch`'s `max_operations`).
pub const GROUP_COMMIT_MAX: usize = 64;

/// The same failure for every writer of a lost group.
fn duplicate(error: &MemoryBackingError) -> MemoryBackingError {
    match error {
        MemoryBackingError::Backing(m) => MemoryBackingError::Backing(m.clone()),
        MemoryBackingError::Busy(m) => MemoryBackingError::Busy(m.clone()),
        MemoryBackingError::Invalid(m) => MemoryBackingError::Invalid(m.clone()),
        MemoryBackingError::Io(e) => MemoryBackingError::Io(io::Error::new(e.kind(), e.to_string())),
        MemoryBackingError::Sqlite(rusqlite::Error::SqliteFailure(code, message)) => {
            MemoryBackingError::Sqlite(rusqlite::Error::SqliteFailure(*code, message.clone()))
        }
        MemoryBackingError::Sqlite(other) => MemoryBackingError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
            Some(other.to_string()),
        )),
        MemoryBackingError::Command { argv, status, stderr } => {
            MemoryBackingError::Command { argv: argv.clone(), status: *status, stderr: stderr.clone() }
        }
    }
}

/// One open journal transaction that queued writers share, and its outcome.
#[derive(Default)]
struct Group {
    outcome: Mutex<Option<std::result::Result<(), MemoryBackingError>>>,
    done: Condvar,
}

impl Group {
    fn finish(&self, outcome: Result<()>) {
        *lock(&self.outcome) = Some(outcome);
        self.done.notify_all();
    }

    fn wait(&self) -> Result<()> {
        let mut outcome = lock(&self.outcome);
        loop {
            match outcome.as_ref() {
                Some(Ok(())) => return Ok(()),
                Some(Err(error)) => return Err(duplicate(error)),
                None => outcome = self.done.wait(outcome).unwrap_or_else(PoisonError::into_inner),
            }
        }
    }
}

/// State behind the writer turn: the writer connection and its open group.
#[derive(Default)]
struct WriterTurn {
    connection: Option<Connection>,
    group: Option<Arc<Group>>,
    members: usize,
}

/// The journal's group commit, as the registry's owner writes and Python's
/// `DurableSqliteBatch` do: writers queue for one turn and share one open
/// `BEGIN IMMEDIATE` transaction, each under its own SAVEPOINT. A failed
/// writer undoes only its savepoint and returns at once; the others return
/// after the one `synchronous=FULL` COMMIT holding their changes, which the
/// last queued writer (or the 64th) issues. SQLite's file locks still
/// serialize the group against the Python agent's writers.
struct JournalWriter {
    path: PathBuf,
    turn: Mutex<WriterTurn>,
    waiters: AtomicUsize,
    commits: AtomicU64,
}

impl JournalWriter {
    fn new(path: PathBuf, connection: Connection) -> Self {
        JournalWriter {
            path,
            turn: Mutex::new(WriterTurn { connection: Some(connection), ..WriterTurn::default() }),
            waiters: AtomicUsize::new(0),
            commits: AtomicU64::new(0),
        }
    }

    /// Run `body` as one durable journal write. `check` runs first, holding
    /// the turn; its failure fails only this writer.
    fn write<T>(&self, check: impl FnOnce() -> Result<()>, body: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        self.waiters.fetch_add(1, Ordering::SeqCst);
        let mut turn = lock(&self.turn);
        self.waiters.fetch_sub(1, Ordering::SeqCst);
        let mut group = None;
        let result = check().and_then(|()| {
            let joined = self.join(&mut turn)?;
            group = Some(joined);
            self.member(&mut turn, body)
        });
        if result.is_ok() {
            turn.members += 1;
        }
        // Whoever holds the turn last commits the open group, failed or not.
        let committed = if turn.group.is_some()
            && (self.waiters.load(Ordering::SeqCst) == 0 || turn.members >= GROUP_COMMIT_MAX)
        {
            self.commit(&mut turn)
        } else {
            Ok(())
        };
        drop(turn);
        let value = result?;
        committed?;
        group.expect("a member joined a group").wait()?;
        Ok(value)
    }

    /// The open group, else BEGIN IMMEDIATE a new one.
    fn join(&self, turn: &mut WriterTurn) -> Result<Arc<Group>> {
        if let Some(group) = &turn.group {
            return Ok(group.clone());
        }
        if turn.connection.is_none() {
            turn.connection = Some(connect(&self.path, true)?);
        }
        turn.connection.as_ref().expect("connected above").execute_batch("BEGIN IMMEDIATE")?;
        turn.members = 0;
        let group = Arc::new(Group::default());
        turn.group = Some(group.clone());
        Ok(group)
    }

    /// One member under its SAVEPOINT. A savepoint statement that fails
    /// loses the whole group, as does a panic.
    fn member<T>(&self, turn: &mut WriterTurn, body: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let connection = turn.connection.as_ref().expect("an open group has a connection");
        let (result, lost) = match connection.execute_batch("SAVEPOINT member") {
            Err(error) => {
                let error = MemoryBackingError::from(error);
                let lost = duplicate(&error);
                (Err(error), Some(lost))
            }
            Ok(()) => match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(connection))) {
                Err(panic) => {
                    let _ = connection.execute_batch("ROLLBACK TO member; RELEASE member");
                    self.abandon(turn, MemoryBackingError::Backing("memory journal writer panicked".into()));
                    std::panic::resume_unwind(panic);
                }
                Ok(Ok(value)) => match connection.execute_batch("RELEASE member") {
                    Ok(()) => (Ok(value), None),
                    Err(error) => {
                        let error = MemoryBackingError::from(error);
                        let lost = duplicate(&error);
                        (Err(error), Some(lost))
                    }
                },
                Ok(Err(failure)) => {
                    let undone = connection.execute_batch("ROLLBACK TO member; RELEASE member");
                    (Err(failure), undone.err().map(MemoryBackingError::from))
                }
            },
        };
        if let Some(cause) = lost {
            self.abandon(turn, cause);
        }
        result
    }

    /// One COMMIT for the group. An uncertain COMMIT drops the connection.
    fn commit(&self, turn: &mut WriterTurn) -> Result<()> {
        let group = turn.group.take().expect("an open group");
        let committed = match &turn.connection {
            Some(connection) => connection.execute_batch("COMMIT").map_err(MemoryBackingError::from),
            None => fail("memory journal writer was lost"),
        };
        match committed {
            Ok(()) => {
                self.commits.fetch_add(1, Ordering::SeqCst);
                group.finish(Ok(()));
                Ok(())
            }
            Err(error) => {
                turn.connection = None;
                group.finish(Err(duplicate(&error)));
                Err(error)
            }
        }
    }

    /// Closing the connection rolls back the open group.
    fn abandon(&self, turn: &mut WriterTurn, error: MemoryBackingError) {
        turn.connection = None;
        if let Some(group) = turn.group.take() {
            group.finish(Err(error));
        }
    }
}

impl MemoryBackingStore {
    /// Python `MemoryBackingStore.__init__`: validate the roots, then create
    /// or migrate the journal in one `BEGIN IMMEDIATE` transaction with
    /// exactly Python's statements, so the schema text is identical.
    pub fn open(config: MemoryBackingConfig, mut quota: Box<dyn MemoryQuota>) -> Result<Self> {
        let MemoryBackingConfig { root, journal, hard_capacity_bytes, active_root, ram_swappable } = config;
        if !root.is_absolute() || !journal.is_absolute() || hard_capacity_bytes == 0 || hard_capacity_bytes > i64::MAX as u64 {
            return Err(MemoryBackingError::Invalid("memory backing paths/capacity are invalid".into()));
        }
        // xfs_quota's command parser does not implement shell quoting.
        if has_space(&root) {
            return Err(MemoryBackingError::Invalid("memory backing root cannot contain whitespace".into()));
        }
        if let Some(active) = &active_root {
            if !active.is_absolute() || *active == root {
                return Err(MemoryBackingError::Invalid("RAM memory root must be a distinct absolute path".into()));
            }
            private_directory(active)?;
            quota.validate_active_root(active, ram_swappable)?;
        }
        let parent = journal.parent().ok_or_else(|| MemoryBackingError::Invalid("memory backing paths/capacity are invalid".into()))?;
        let mut lease_name = journal.file_name().unwrap_or(OsStr::new("")).to_owned();
        lease_name.push(".leases");
        let lease_root = parent.join(lease_name);
        mkdir(&lease_root, true)?;
        private_directory(&lease_root)?;
        mkdir(&root, true)?;
        if !parent.is_dir() {
            mkdir(parent, true)?;
        }
        private_directory(&root)?;
        quota.validate_root(&root)?;

        let mut modes = HashMap::new();
        {
            let mut conn = connect(&journal, true)?;
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let version: i64 = tx.query_row("PRAGMA user_version", [], |row| row.get(0))?;
            if !(0..=JOURNAL_VERSION).contains(&version) {
                return fail("unsupported memory backing journal version");
            }
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS allocations (\
                 allocation_id TEXT PRIMARY KEY, sandbox_id TEXT NOT NULL, \
                 generation INTEGER NOT NULL, project_id INTEGER UNIQUE NOT NULL, \
                 quota_bytes INTEGER NOT NULL, state TEXT NOT NULL)",
            )?;
            tx.execute_batch("CREATE TABLE IF NOT EXISTS counter (value INTEGER NOT NULL)")?;
            tx.execute_batch("CREATE TABLE IF NOT EXISTS features (name TEXT PRIMARY KEY)")?;
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS retained_checkpoints (\
                 allocation_id TEXT NOT NULL, hibernation_generation INTEGER NOT NULL, \
                 manifest_sha256 TEXT NOT NULL, allocated_bytes INTEGER NOT NULL, \
                 device INTEGER NOT NULL, inode INTEGER NOT NULL, \
                 project_id INTEGER UNIQUE NOT NULL, state TEXT NOT NULL, \
                 PRIMARY KEY(allocation_id,hibernation_generation))",
            )?;
            let columns: Vec<String> = tx
                .prepare("PRAGMA table_info(allocations)")?
                .query_map([], |row| row.get(1))?
                .collect::<rusqlite::Result<_>>()?;
            if !columns.iter().any(|c| c == "active_mode") {
                // A journal from before per-owner placement: its layout was the worker's.
                tx.execute_batch("ALTER TABLE allocations ADD COLUMN active_mode TEXT NOT NULL DEFAULT 'file'")?;
                if active_root.is_some() {
                    tx.execute_batch("UPDATE allocations SET active_mode='ram'")?;
                }
            }
            if !columns.iter().any(|c| c == "limit_bytes") {
                tx.execute_batch("ALTER TABLE allocations ADD COLUMN limit_bytes INTEGER NOT NULL DEFAULT -1")?;
                tx.execute_batch("UPDATE allocations SET limit_bytes=quota_bytes WHERE limit_bytes<0")?;
            }
            let counters: i64 = tx.query_row("SELECT COUNT(*) FROM counter", [], |row| row.get(0))?;
            if counters == 0 {
                tx.execute_batch("INSERT INTO counter VALUES (600000)")?;
            }
            tx.execute_batch("PRAGMA user_version=3")?;
            tx.commit()?;
            let mut statement =
                conn.prepare("SELECT sandbox_id,generation,active_mode FROM allocations WHERE state!='deleted'")?;
            let rows = statement.query_map([], |row| Ok((row.get::<_, SqlValue>(0)?, row.get::<_, SqlValue>(1)?, row.get::<_, SqlValue>(2)?)))?;
            for row in rows {
                let (sandbox_id, generation, mode) = row?;
                let mode = Self::parse_mode(active_root.is_some(), text(mode).as_deref())?;
                // Python keys its cache by whatever the row holds; a create can
                // only ever look up a text id and a positive generation.
                if let (SqlValue::Text(sandbox_id), SqlValue::Integer(generation)) = (sandbox_id, generation)
                    && let Ok(generation) = u64::try_from(generation)
                {
                    modes.insert((sandbox_id, generation), mode);
                }
            }
        }

        let meta = fs::metadata(&journal)?;
        let writer = connect(&journal, true)?;
        let reader = connect(&journal, false)?;
        Ok(MemoryBackingStore {
            root,
            lease_root,
            hard_capacity_bytes: hard_capacity_bytes as i64,
            active_root,
            quota,
            journal_identity: (meta.dev(), meta.ino()),
            writer: JournalWriter::new(journal.clone(), writer),
            journal,
            reader: Mutex::new(reader),
            active_modes: Mutex::new(modes),
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn journal(&self) -> &Path {
        &self.journal
    }

    /// `<journal>.leases/`, home of the per-allocation flocks.
    pub fn lease_root(&self) -> &Path {
        &self.lease_root
    }

    pub fn active_root(&self) -> Option<&Path> {
        self.active_root.as_deref()
    }

    /// Durable journal COMMITs this store has issued: one per group of
    /// writers, so at most two per prepare and fewer under concurrency.
    pub fn journal_commits(&self) -> u64 {
        self.writer.commits.load(Ordering::SeqCst)
    }

    fn parse_mode(ram_configured: bool, mode: Option<&str>) -> Result<ActiveMode> {
        match mode {
            Some("ram") if ram_configured => Ok(ActiveMode::Ram),
            Some("file") => Ok(ActiveMode::File),
            _ => fail("memory allocation has an unsupported active backing mode"),
        }
    }

    fn remember_mode(&self, sandbox_id: &str, generation: u64, mode: Option<&str>) -> Result<ActiveMode> {
        let mode = Self::parse_mode(self.active_root.is_some(), mode)?;
        lock(&self.active_modes).insert((sandbox_id.to_string(), generation), mode);
        Ok(mode)
    }

    /// Cached placement, never I/O: admission reads it under its capacity
    /// guard. `None` when this process has not seen the incarnation; the
    /// caller then falls back to RAM if a RAM root is configured, else file.
    pub fn active_mode(&self, sandbox_id: &str, generation: u64) -> Option<ActiveMode> {
        lock(&self.active_modes).get(&(sandbox_id.to_string(), generation)).copied()
    }

    fn check_journal_identity(&self) -> Result<()> {
        let meta = fs::metadata(&self.journal)?;
        if (meta.dev(), meta.ino()) != self.journal_identity {
            return fail("memory journal file was replaced");
        }
        Ok(())
    }

    /// Serializes an allocation's durable state and filesystem side effects
    /// across processes (and a service restart overlap).
    fn mutation_lock(&self, reference: &MemoryBackingRef) -> Result<FileLock> {
        Ok(FileLock::acquire(&self.lease_root.join(format!("{}.mutation", reference.allocation_id)), false)?)
    }

    /// Python `prepare`: claim capacity and a project id, then write the
    /// directory, marker and project quota; idempotent for its incarnation.
    /// `limit_bytes` (default: the ceiling) starts the project's bhard below it.
    pub fn prepare(
        &self,
        reference: &MemoryBackingRef,
        sandbox_id: &str,
        sandbox_generation: u64,
        limit_bytes: Option<u64>,
    ) -> Result<MemoryBackingLease> {
        // SQLite integers are signed 64-bit; Python would overflow there instead.
        if reference.allocation_id != format!("{sandbox_id}.sandbox-{sandbox_generation}")
            || !(1..=i64::MAX as u64).contains(&sandbox_generation)
        {
            return fail("memory allocation belongs to another incarnation");
        }
        let limit = limit_bytes.unwrap_or(reference.quota_bytes);
        if limit == 0 || limit > reference.quota_bytes {
            return fail("memory allocation limit exceeds its ceiling");
        }
        let _mutation = self.mutation_lock(reference)?;
        let (row, created) = self.claim_allocation(reference, sandbox_id, sandbox_generation, limit as i64)?;
        if !created && (!row.owned_by(sandbox_id, sandbox_generation, reference.quota_bytes) || row.state_is("deleted")) {
            return fail("memory allocation identity/claim conflicts");
        }
        let path = self.root.join(&reference.allocation_id);
        let mode = row.active_mode.as_deref();
        if row.state_is("deleting") {
            return fail("memory allocation is being deleted");
        }
        if row.state_is("ready") {
            return self.validate(reference, sandbox_id, sandbox_generation, row.project_id, mode);
        }
        mkdir(&path, false)?;
        private_directory(&path)?;
        let marker = path.join(MARKER);
        let probe = MemoryBackingLease {
            reference: reference.clone(),
            sandbox_id: sandbox_id.to_string(),
            sandbox_generation,
            project_id: row.project_id,
            path: path.clone(),
            active_mode: ActiveMode::File,
        };
        if marker.exists() {
            if !marker_matches(&marker, &probe)? {
                return fail("memory allocation marker conflicts");
            }
        } else {
            // A crash between mkdir and the marker is recoverable only while empty.
            if fs::read_dir(&path)?.next().is_some() {
                return fail("unmarked memory allocation is not empty");
            }
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&marker)?;
            file.write_all(&marker_bytes(&probe))?;
            file.sync_all()?;
            drop(file);
            fsync_dir(&path)?;
            fsync_dir(&self.root)?;
        }
        self.quota.provision(&path, row.project_id, row.limit_bytes)?;
        self.writer.write(
            || self.check_journal_identity(),
            |conn| {
                conn.execute(
                    "UPDATE allocations SET state='ready' WHERE allocation_id=? AND state='preparing'",
                    [&reference.allocation_id],
                )?;
                Ok(())
            },
        )?;
        self.prepare_active(reference, mode)?;
        let active_mode = self.remember_mode(sandbox_id, sandbox_generation, mode)?;
        Ok(MemoryBackingLease { active_mode, ..probe })
    }

    /// Reserve capacity and a project id in one durable commit.
    fn claim_allocation(
        &self,
        reference: &MemoryBackingRef,
        sandbox_id: &str,
        sandbox_generation: u64,
        limit_bytes: i64,
    ) -> Result<(AllocationRow, bool)> {
        self.writer.write(
            || self.check_journal_identity(),
            |tx| self.claim_body(tx, reference, sandbox_id, sandbox_generation, limit_bytes),
        )
    }

    /// `claim_allocation`'s group member: a failure undoes only its savepoint.
    fn claim_body(
        &self,
        tx: &Connection,
        reference: &MemoryBackingRef,
        sandbox_id: &str,
        sandbox_generation: u64,
        limit_bytes: i64,
    ) -> Result<(AllocationRow, bool)> {
        let mut row = select_allocation(tx, &reference.allocation_id)?;
        let mut active_mode = Some(if self.active_root.is_some() { "ram" } else { "file" }.to_string());
        if let Some(deleted) = row.as_ref().filter(|r| r.state_is("deleted")) {
            if !deleted.owned_by(sandbox_id, sandbox_generation, reference.quota_bytes) {
                return fail("reimported memory identity/claim conflicts");
            }
            if lexists(&self.root.join(&reference.allocation_id))? {
                return fail("deleted memory allocation path still exists");
            }
            tx.execute("DELETE FROM allocations WHERE allocation_id=?", [&reference.allocation_id])?;
            // Reimporting an incarnation never reverses its placement.
            active_mode = deleted.active_mode.clone();
            row = None;
        }
        let created = row.is_none();
        let row = match row {
            Some(row) => row,
            None => {
                let reserved: i64 = tx.query_row(
                    "SELECT COALESCE(SUM(limit_bytes),0) FROM allocations WHERE state!='deleted'",
                    [],
                    |r| r.get(0),
                )?;
                if i128::from(reserved) + i128::from(limit_bytes) > i128::from(self.hard_capacity_bytes) {
                    return fail("memory backing hard capacity exhausted");
                }
                let project: i64 = tx.query_row("SELECT value FROM counter", [], |r| r.get(0))?;
                tx.execute_batch("UPDATE counter SET value=value+1")?;
                tx.execute(
                    "INSERT INTO allocations VALUES (?,?,?,?,?,?,?,?)",
                    params![
                        reference.allocation_id,
                        sandbox_id,
                        sandbox_generation as i64,
                        project,
                        reference.quota_bytes as i64,
                        "preparing",
                        active_mode,
                        limit_bytes
                    ],
                )?;
                AllocationRow {
                    sandbox_id: SqlValue::Text(sandbox_id.to_string()),
                    generation: SqlValue::Integer(sandbox_generation as i64),
                    project_id: project,
                    quota_bytes: SqlValue::Integer(reference.quota_bytes as i64),
                    state: Some("preparing".into()),
                    active_mode,
                    limit_bytes,
                }
            }
        };
        Ok((row, created))
    }

    /// Python `require` (the Warden's `_require_memory_allocation`): the row
    /// must be `ready` for this incarnation; then the marker, the project and
    /// the RAM directory are revalidated.
    pub fn require(&self, reference: &MemoryBackingRef, sandbox_id: &str, sandbox_generation: u64) -> Result<MemoryBackingLease> {
        let _mutation = self.mutation_lock(reference)?;
        let row = {
            let conn = lock(&self.reader);
            self.check_journal_identity()?;
            select_allocation(&conn, &reference.allocation_id)?
        };
        let Some(row) = row.filter(|r| r.owned_by(sandbox_id, sandbox_generation, reference.quota_bytes) && r.state_is("ready"))
        else {
            return fail("memory allocation is not retained by this incarnation");
        };
        self.validate(reference, sandbox_id, sandbox_generation, row.project_id, row.active_mode.as_deref())
    }

    fn validate(
        &self,
        reference: &MemoryBackingRef,
        sandbox_id: &str,
        sandbox_generation: u64,
        project_id: i64,
        mode: Option<&str>,
    ) -> Result<MemoryBackingLease> {
        let path = self.root.join(&reference.allocation_id);
        private_directory(&path)?;
        let lease = MemoryBackingLease {
            reference: reference.clone(),
            sandbox_id: sandbox_id.to_string(),
            sandbox_generation,
            project_id,
            path,
            active_mode: ActiveMode::File,
        };
        if !marker_matches(&lease.path.join(MARKER), &lease)? {
            return fail("memory allocation marker conflicts");
        }
        self.quota.validate_project(&lease.path, project_id)?;
        self.prepare_active(reference, mode)?;
        let active_mode = self.remember_mode(sandbox_id, sandbox_generation, mode)?;
        Ok(MemoryBackingLease { active_mode, ..lease })
    }

    /// A RAM-mode allocation's `<active_root>/<allocation_id>` (tmpfs empties on reboot).
    fn prepare_active(&self, reference: &MemoryBackingRef, mode: Option<&str>) -> Result<()> {
        let Some(active_root) = &self.active_root else { return Ok(()) };
        if mode != Some("ram") {
            return Ok(());
        }
        let path = active_root.join(&reference.allocation_id);
        mkdir(&path, false)?;
        private_directory(&path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(project_id: i64) -> MemoryBackingLease {
        MemoryBackingLease {
            reference: MemoryBackingRef::new("sb-1.sandbox-1", 4_294_967_296).unwrap(),
            sandbox_id: "sb-1".into(),
            sandbox_generation: 1,
            project_id,
            path: PathBuf::from("/m/sb-1.sandbox-1"),
            active_mode: ActiveMode::File,
        }
    }

    #[test]
    fn marker_bytes_match_python_json_dump() {
        assert_eq!(
            String::from_utf8(marker_bytes(&lease(600000))).unwrap(),
            r#"{"allocation_id": "sb-1.sandbox-1", "project_id": 600000, "quota_bytes": 4294967296, "sandbox_generation": 1, "sandbox_id": "sb-1", "version": 1}"#
        );
    }

    #[test]
    fn marker_comparison_is_python_equality() {
        let expected: Value = serde_json::from_slice(&marker_bytes(&lease(7))).unwrap();
        let mut found = expected.clone();
        found["version"] = serde_json::json!(1.0);
        assert!(python_eq(&found, &expected));
        found["version"] = serde_json::json!(true);
        assert!(python_eq(&found, &expected));
        found["version"] = serde_json::json!(2);
        assert!(!python_eq(&found, &expected));
        found["version"] = serde_json::json!(1);
        found["extra"] = serde_json::json!(null);
        assert!(!python_eq(&found, &expected));
    }

    #[test]
    fn xfs_quota_argv_matches_python() {
        let [project, limit] = provision_commands(Path::new("/var/lib/s/mounts"), Path::new("/var/lib/s/mounts/a.sandbox-1"), 600001, 67108864);
        assert_eq!(project, ["xfs_quota", "-x", "-c", "project -s -p /var/lib/s/mounts/a.sandbox-1 600001", "/var/lib/s/mounts"].map(OsString::from));
        assert_eq!(limit, ["xfs_quota", "-x", "-c", "limit -p bsoft=67108864 bhard=67108864 600001", "/var/lib/s/mounts"].map(OsString::from));
    }

    #[test]
    fn identities_follow_python_ref_validation() {
        assert!(MemoryBackingRef::new("a".repeat(240), 1).is_ok());
        assert!(matches!(MemoryBackingRef::new("a".repeat(241), 1), Err(MemoryBackingError::Invalid(_))));
        assert!(MemoryBackingRef::new("-a", 1).is_err());
        assert!(MemoryBackingRef::new("a/b", 1).is_err());
        assert!(MemoryBackingRef::new("a", 0).is_err());
        assert!(has_space(Path::new("/a\u{1f}b")) && has_space(Path::new("/a b")) && !has_space(Path::new("/ab")));
    }

    #[test]
    fn syscall_structs_have_the_kernel_layout() {
        use std::mem::{offset_of, size_of};
        assert_eq!(size_of::<FsXattr>(), 28);
        assert_eq!((offset_of!(FsXattr, xflags), offset_of!(FsXattr, projid), offset_of!(FsXattr, pad)), (0, 12, 20));
        assert_eq!(size_of::<FsDiskQuota>(), 112);
        assert_eq!(std::mem::align_of::<FsDiskQuota>(), 8);
        let offsets = [
            offset_of!(FsDiskQuota, d_version),
            offset_of!(FsDiskQuota, d_flags),
            offset_of!(FsDiskQuota, d_fieldmask),
            offset_of!(FsDiskQuota, d_id),
            offset_of!(FsDiskQuota, d_blk_hardlimit),
            offset_of!(FsDiskQuota, d_blk_softlimit),
            offset_of!(FsDiskQuota, d_ino_hardlimit),
            offset_of!(FsDiskQuota, d_ino_softlimit),
            offset_of!(FsDiskQuota, d_bcount),
            offset_of!(FsDiskQuota, d_icount),
            offset_of!(FsDiskQuota, d_itimer),
            offset_of!(FsDiskQuota, d_btimer),
            offset_of!(FsDiskQuota, d_iwarns),
            offset_of!(FsDiskQuota, d_bwarns),
            offset_of!(FsDiskQuota, d_itimer_hi),
            offset_of!(FsDiskQuota, d_btimer_hi),
            offset_of!(FsDiskQuota, d_rtbtimer_hi),
            offset_of!(FsDiskQuota, d_padding2),
            offset_of!(FsDiskQuota, d_rtb_hardlimit),
            offset_of!(FsDiskQuota, d_rtb_softlimit),
            offset_of!(FsDiskQuota, d_rtbcount),
            offset_of!(FsDiskQuota, d_rtbtimer),
            offset_of!(FsDiskQuota, d_rtbwarns),
            offset_of!(FsDiskQuota, d_padding3),
            offset_of!(FsDiskQuota, d_padding4),
        ];
        // linux/dqblk_xfs.h, field by field.
        assert_eq!(offsets, [0, 1, 2, 4, 8, 16, 24, 32, 40, 48, 56, 60, 64, 66, 68, 69, 70, 71, 72, 80, 88, 96, 100, 102, 104]);
    }

    #[test]
    fn syscall_numbers_match_the_kernel_macros() {
        // _IOC(dir, 'X', nr, sizeof(struct fsxattr)): read 2, write 1.
        let ioc = |dir: u64, nr: u64| ((dir << 30) | (28 << 16) | ((b'X' as u64) << 8) | nr) as libc::c_ulong;
        assert_eq!((FS_IOC_FSGETXATTR, FS_IOC_FSSETXATTR), (ioc(2, 31), ioc(1, 32)));
        // QCMD(Q_XSETQLIM, PRJQUOTA), as strace prints xfs_quota's call.
        assert_eq!(Q_XSETQLIM, 0x5804);
        assert_eq!(qcmd(Q_XSETQLIM, PRJQUOTA), 0x0058_0402);
        assert_eq!(qcmd(Q_XSETQLIM, 0x1_02), 0x0058_0402);
        assert_eq!((FS_DQ_BSOFT | FS_DQ_BHARD, FS_PROJ_QUOTA, FS_DQUOT_VERSION), (0xc, 2, 1));
    }

    #[test]
    fn project_limits_are_what_xfs_quota_sends() {
        // strace of `xfs_quota -x -c "limit -p bsoft=67108864 bhard=67108864 600123"`.
        let quota = FsDiskQuota::project_block_limits(600123, 67_108_864);
        assert_eq!(
            quota,
            FsDiskQuota {
                d_version: 1,
                d_flags: 2,
                d_fieldmask: 0xc,
                d_id: 600123,
                d_blk_hardlimit: 131072,
                d_blk_softlimit: 131072,
                ..FsDiskQuota::default()
            }
        );
        // extractb shifts bytes to basic blocks, rounding down.
        assert_eq!(FsDiskQuota::project_block_limits(1, 1023).d_blk_hardlimit, 1);
        assert_eq!(FsDiskQuota::project_block_limits(1, 511).d_blk_softlimit, 0);
    }

    #[test]
    fn device_numbers_decode_st_rdev() {
        let null = fs::metadata("/dev/null").unwrap();
        assert_eq!(device_numbers(null.rdev()), (1, 3));
        let makedev = |major: u64, minor: u64| {
            ((major & 0xffff_f000) << 32) | ((major & 0x0fff) << 8) | ((minor & 0xffff_ff00) << 12) | (minor & 0x00ff)
        };
        for (major, minor) in [(7, 27), (259, 1), (0x12345, 0x6789a), (4095, 255), (4096, 256)] {
            assert_eq!(device_numbers(makedev(major, minor)), (major, minor));
        }
    }

    #[test]
    fn only_unsupported_syscalls_fall_back_to_xfs_quota() {
        let run = |errno: Option<i32>| {
            let (fell_back, logged) = (std::cell::Cell::new(false), std::cell::Cell::new(None));
            let result = provision_with_fallback(
                || errno.map_or(Ok(()), |e| Err(io::Error::from_raw_os_error(e))),
                || {
                    fell_back.set(true);
                    Ok(())
                },
                |error| logged.set(error.raw_os_error()),
            );
            (result.map_err(|e| e.python_class()), fell_back.get(), logged.get())
        };
        assert_eq!(run(None), (Ok(false), false, None));
        assert_eq!(run(Some(libc::ENOSYS)), (Ok(true), true, Some(libc::ENOSYS)));
        assert_eq!(run(Some(libc::EINVAL)), (Ok(true), true, Some(libc::EINVAL)));
        // Anything else is the node's failure, as a failed xfs_quota was.
        assert_eq!(run(Some(libc::EPERM)), (Err("OSError"), false, None));
        assert_eq!(run(Some(libc::ENOTTY)), (Err("OSError"), false, None));
        // The fallback's own failure is its CalledProcessError.
        let failed = provision_with_fallback(
            || Err(io::Error::from_raw_os_error(libc::ENOSYS)),
            || Err(MemoryBackingError::Command { argv: vec!["xfs_quota".into()], status: Some(1), stderr: String::new() }),
            |_| {},
        );
        assert_eq!(failed.unwrap_err().python_class(), "CalledProcessError");
        let quota = XfsMemoryQuota::new();
        quota.log_fallback(&"first");
        quota.log_fallback(&"second");
        assert!(quota.fallback_logged.load(Ordering::SeqCst));
    }

    #[test]
    fn xfs_quota_backend_never_resolves_a_device() {
        let quota = XfsMemoryQuota::with_backend(QuotaBackend::XfsQuota);
        assert_eq!((quota.block_device(), quota.filesystem_root()), (None, None));
        assert_eq!(XfsMemoryQuota::new().backend, QuotaBackend::Syscalls);
    }

    // ---- The journal's group commit. ----

    struct Journal {
        dir: PathBuf,
        writer: JournalWriter,
    }

    impl Drop for Journal {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }

    impl Journal {
        fn new(name: &str) -> Journal {
            let dir = std::env::temp_dir().join(format!("noded-journal-{name}-{}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();
            let path = dir.join("journal.sqlite");
            let conn = connect(&path, true).unwrap();
            conn.execute_batch("CREATE TABLE t (k TEXT PRIMARY KEY)").unwrap();
            Journal { writer: JournalWriter::new(path, conn), dir }
        }

        fn insert(&self, key: &str) -> Result<()> {
            self.writer.write(|| Ok(()), |c| Ok(c.execute("INSERT INTO t VALUES (?)", [key]).map(|_| ())?))
        }

        /// What a fresh connection sees: only committed rows.
        fn committed(&self) -> Vec<String> {
            let conn = Connection::open(self.dir.join("journal.sqlite")).unwrap();
            let mut statement = conn.prepare("SELECT k FROM t ORDER BY k").unwrap();
            statement.query_map([], |r| r.get(0)).unwrap().map(|k| k.unwrap()).collect()
        }

        fn wait_for_waiters(&self, count: usize) {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while self.writer.waiters.load(Ordering::SeqCst) < count {
                assert!(std::time::Instant::now() < deadline, "writers never queued");
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }

    #[test]
    fn queued_writers_share_one_commit_and_fail_alone() {
        let journal = Journal::new("group");
        let (inside_tx, inside_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = Mutex::new(release_rx);
        std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                journal.writer.write(
                    || Ok(()),
                    |c| {
                        c.execute("INSERT INTO t VALUES ('a')", [])?;
                        inside_tx.send(()).unwrap();
                        release_rx.lock().unwrap().recv().unwrap();
                        Ok(())
                    },
                )
            });
            inside_rx.recv().unwrap();
            let second = scope.spawn(|| journal.insert("b"));
            let failing = scope.spawn(|| {
                journal.writer.write(
                    || Ok(()),
                    |c| {
                        c.execute("INSERT INTO t VALUES ('c')", [])?;
                        Err::<(), _>(MemoryBackingError::Backing("changed my mind".into()))
                    },
                )
            });
            let refused = scope.spawn(|| journal.writer.write(|| fail("memory journal file was replaced"), |_| Ok(())));
            journal.wait_for_waiters(3);
            // Written but not committed: no other connection sees it.
            assert!(journal.committed().is_empty());
            release_tx.send(()).unwrap();
            first.join().unwrap().unwrap();
            second.join().unwrap().unwrap();
            assert_eq!(failing.join().unwrap().unwrap_err().to_string(), "changed my mind");
            assert_eq!(refused.join().unwrap().unwrap_err().to_string(), "memory journal file was replaced");
        });
        // One COMMIT (one fsync) for both durable writers; the failure undid only its own row.
        assert_eq!(journal.writer.commits.load(Ordering::SeqCst), 1);
        assert_eq!(journal.committed(), ["a", "b"]);
    }

    #[test]
    fn a_refused_last_writer_still_commits_the_open_group() {
        let journal = Journal::new("refused");
        let (inside_tx, inside_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = Mutex::new(release_rx);
        std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                journal.writer.write(
                    || Ok(()),
                    |c| {
                        c.execute("INSERT INTO t VALUES ('a')", [])?;
                        inside_tx.send(()).unwrap();
                        release_rx.lock().unwrap().recv().unwrap();
                        Ok(())
                    },
                )
            });
            inside_rx.recv().unwrap();
            // The only queued writer fails its check: it must commit for the first.
            let refused = scope.spawn(|| journal.writer.write(|| fail("refused"), |_| Ok(())));
            journal.wait_for_waiters(1);
            release_tx.send(()).unwrap();
            first.join().unwrap().unwrap();
            assert!(refused.join().unwrap().is_err());
        });
        assert_eq!(journal.committed(), ["a"]);
        assert_eq!(journal.writer.commits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_panicking_member_abandons_its_group_and_leaves_nothing() {
        let journal = Journal::new("panic");
        let (inside_tx, inside_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = Mutex::new(release_rx);
        std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                journal.writer.write(
                    || Ok(()),
                    |c| {
                        c.execute("INSERT INTO t VALUES ('a')", [])?;
                        inside_tx.send(()).unwrap();
                        release_rx.lock().unwrap().recv().unwrap();
                        Ok(())
                    },
                )
            });
            inside_rx.recv().unwrap();
            let panicking = scope.spawn(|| {
                journal.writer.write(
                    || Ok(()),
                    |c| -> Result<()> {
                        c.execute("INSERT INTO t VALUES ('p')", [])?;
                        panic!("writer bug");
                    },
                )
            });
            journal.wait_for_waiters(1);
            release_tx.send(()).unwrap();
            // The group the first writer joined is lost, not half-committed.
            assert_eq!(first.join().unwrap().unwrap_err().to_string(), "memory journal writer panicked");
            assert!(panicking.join().is_err());
        });
        assert!(journal.committed().is_empty());
        // The writer reconnects for the next group.
        journal.insert("b").unwrap();
        assert_eq!(journal.committed(), ["b"]);
    }

    #[test]
    fn concurrent_writers_batch_and_all_commit() {
        let journal = Journal::new("burst");
        std::thread::scope(|scope| {
            for i in 0..128 {
                let journal = &journal;
                scope.spawn(move || journal.insert(&format!("k{i:03}")).unwrap());
            }
        });
        assert_eq!(journal.committed().len(), 128);
        let commits = journal.writer.commits.load(Ordering::SeqCst);
        assert!((2..=128).contains(&commits), "{commits} commits");
    }

    #[test]
    fn fsxattr_reads_the_kernel_project() {
        // Every directory starts in project 0 without PROJINHERIT; filesystems
        // without the ioctl (ENOTTY) cannot host allocations at all.
        let dir = std::env::temp_dir();
        let file = File::open(&dir).unwrap();
        match fsxattr(&file) {
            Ok((_, projid)) => {
                let error = XfsMemoryQuota::new().validate_project(&dir, i64::from(projid) + 600000).unwrap_err();
                assert_eq!(error.to_string(), "memory directory lost project-quota ownership");
            }
            Err(error) => assert_eq!(error.raw_os_error(), Some(libc::ENOTTY)),
        }
    }
}
