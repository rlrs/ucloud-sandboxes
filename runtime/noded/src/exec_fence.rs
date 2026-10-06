//! The exec fence shared with the Python agent through the kernel (phase 2a;
//! ucloud_sandboxes/exec_fence.py). Per sandbox id, in the Warden's lock
//! directory:
//!
//! - `.<id>.transition` (T): Python holds it exclusively for a lifecycle
//!   transition. The daemon takes it shared and non-blocking only while it
//!   takes A; busy means "send the exec to the agent".
//! - `.<id>.activity` (A): the daemon holds it shared for a whole exec session,
//!   from before its running check until the session is reaped. Park and pause
//!   need it exclusively and fail fast; delete and wake never take it.
//!
//! Lock order is T, then A. A's mtime is the activity clock. Delete unlinks A,
//! then T, under T, so every locker checks after locking that the path still
//! names its descriptor's inode and opens again otherwise.

use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

/// A held shared A lock; dropping it releases the fence.
#[derive(Debug)]
pub struct ActivityLease {
    file: File,
}

impl ActivityLease {
    /// Advance the activity clock (mtime := now). Never fails the exec.
    pub fn touch(&self) {
        // SAFETY: futimens with a NULL times array sets both stamps to now.
        unsafe { libc::futimens(self.file.as_raw_fd(), std::ptr::null()) };
    }
}

#[derive(Debug)]
pub enum Fenced {
    Held(ActivityLease),
    /// A transition or a holder of A is in the way: forward to the agent.
    Busy,
}

#[derive(Clone, Debug)]
pub struct ExecFence {
    directory: PathBuf,
}

/// Python's SANDBOX_ID_RE: `[A-Za-z0-9][A-Za-z0-9_.-]{0,63}`.
fn safe_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_alphanumeric()
        && bytes.iter().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

fn open(path: &Path) -> std::io::Result<File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
}

/// Open and flock without blocking; `None` when busy. Retries while the lock
/// landed on an inode the path no longer names.
fn locked(path: &Path, operation: libc::c_int) -> std::io::Result<Option<File>> {
    loop {
        let file = open(path)?;
        // SAFETY: a valid descriptor.
        if unsafe { libc::flock(file.as_raw_fd(), operation | libc::LOCK_NB) } != 0 {
            let error = std::io::Error::last_os_error();
            return match error.raw_os_error() {
                Some(libc::EWOULDBLOCK) => Ok(None),
                _ => Err(error),
            };
        }
        let held = file.metadata()?;
        match std::fs::symlink_metadata(path) {
            Ok(current) if (current.dev(), current.ino()) == (held.dev(), held.ino()) => return Ok(Some(file)),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
}

impl ExecFence {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        ExecFence { directory: directory.into() }
    }

    fn path(&self, sandbox_id: &str, kind: &str) -> Option<PathBuf> {
        safe_id(sandbox_id).then(|| self.directory.join(format!(".{sandbox_id}.{kind}")))
    }

    /// T shared, then A shared, then release T. Non-blocking throughout.
    pub fn acquire(&self, sandbox_id: &str) -> std::io::Result<Fenced> {
        let (Some(transition), Some(activity)) = (self.path(sandbox_id, "transition"), self.path(sandbox_id, "activity"))
        else {
            return Ok(Fenced::Busy);
        };
        let Some(_transition) = locked(&transition, libc::LOCK_SH)? else { return Ok(Fenced::Busy) };
        let Some(file) = locked(&activity, libc::LOCK_SH)? else { return Ok(Fenced::Busy) };
        Ok(Fenced::Held(ActivityLease { file }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp() -> PathBuf {
        let path = std::env::temp_dir().join(format!("noded-fence-{}-{:?}", std::process::id(), std::thread::current().id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn transitions_and_activity_exclude_each_other() {
        let dir = temp();
        let fence = ExecFence::new(&dir);
        let Fenced::Held(lease) = fence.acquire("box-1").unwrap() else { panic!("free fence is busy") };
        // A second exec shares A.
        assert!(matches!(fence.acquire("box-1").unwrap(), Fenced::Held(_)));
        // Park (A exclusive) fails while an exec holds A.
        assert!(locked(&dir.join(".box-1.activity"), libc::LOCK_EX).unwrap().is_none());
        lease.touch();
        drop(lease);
        // A transition holding T exclusively sends execs away.
        let transition = locked(&dir.join(".box-1.transition"), libc::LOCK_EX).unwrap().unwrap();
        assert!(matches!(fence.acquire("box-1").unwrap(), Fenced::Busy));
        drop(transition);
        assert!(matches!(fence.acquire("box-1").unwrap(), Fenced::Held(_)));
        assert!(matches!(fence.acquire("../x").unwrap(), Fenced::Busy));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_lock_on_an_unlinked_inode_is_retaken() {
        let dir = temp();
        let fence = ExecFence::new(&dir);
        let Fenced::Held(old) = fence.acquire("box-2").unwrap() else { panic!() };
        // Delete unlinks A while an old exec still holds the orphan.
        std::fs::remove_file(dir.join(".box-2.activity")).unwrap();
        let Fenced::Held(new) = fence.acquire("box-2").unwrap() else { panic!() };
        assert_ne!(old.file.metadata().unwrap().ino(), new.file.metadata().unwrap().ino());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
