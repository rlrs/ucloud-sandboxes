//! File discipline shared with the Python agent: private directories, owner
//! checks, flock'd lock files and atomic, fsynced replacement.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub fn euid() -> u32 {
    // SAFETY: geteuid has no preconditions.
    unsafe { libc::geteuid() }
}

/// mkdir -p with mode 0700 for created levels, then require the leaf to be a
/// real directory owned by us and not group or world writable.
pub fn ensure_private_dir(path: &Path) -> io::Result<()> {
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(path)?;
    let meta = std::fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.uid() != euid() || meta.mode() & 0o022 != 0 {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, format!("{} is not a private directory", path.display())));
    }
    Ok(())
}

/// An exclusive flock on a private lock file, released on drop.
pub struct FileLock {
    file: File,
}

impl FileLock {
    /// Python: open(O_RDWR|O_CREAT|O_CLOEXEC|O_NOFOLLOW, 0o600) + flock(LOCK_EX).
    pub fn acquire(path: &Path, require_private: bool) -> io::Result<FileLock> {
        Self::flocked(path, require_private, libc::LOCK_EX).map(|lock| lock.expect("blocking flock returns a lock"))
    }

    /// `acquire` without waiting: `None` when another holder has it.
    pub fn try_acquire(path: &Path, require_private: bool) -> io::Result<Option<FileLock>> {
        Self::flocked(path, require_private, libc::LOCK_EX | libc::LOCK_NB)
    }

    fn flocked(path: &Path, require_private: bool, operation: libc::c_int) -> io::Result<Option<FileLock>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)?;
        if require_private {
            let meta = file.metadata()?;
            if !meta.is_file() || meta.uid() != euid() || meta.mode() & 0o077 != 0 {
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, format!("{} is not a private lock file", path.display())));
            }
        }
        // SAFETY: the descriptor is valid for the life of `file`.
        if unsafe { libc::flock(file.as_raw_fd(), operation) } != 0 {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(libc::EWOULDBLOCK) { Ok(None) } else { Err(error) };
        }
        Ok(Some(FileLock { file }))
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        // SAFETY: as above; closing the file would release the lock anyway.
        unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

pub fn fsync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn temp_name(dir: &Path, name: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos();
    dir.join(format!(".{name}.{}-{nanos:x}-{}.tmp", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)))
}

/// Python's `_save_unlocked`: a private temp file in the same directory (0600),
/// write, fsync, rename over `path`, fsync the directory; the temp file never survives.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = path.parent().ok_or_else(|| io::Error::other("path has no parent"))?;
    let name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| io::Error::other("invalid file name"))?;
    let temp = temp_name(dir, name);
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&temp)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temp, path)?;
        fsync_dir(dir)
    })();
    let _ = std::fs::remove_file(&temp);
    result
}

/// Read a regular file we own that nobody else may write, at most `limit` bytes.
/// `Ok(None)` when it does not exist.
pub fn read_owned_file(path: &Path, limit: u64) -> io::Result<Option<Vec<u8>>> {
    let file = match OpenOptions::new().read(true).custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let meta = file.metadata()?;
    if !meta.is_file() || meta.uid() != euid() || meta.mode() & 0o022 != 0 || meta.len() > limit {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, format!("cannot safely open {}", path.display())));
    }
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(io::Error::other(format!("{} is too large", path.display())));
    }
    Ok(Some(bytes))
}
