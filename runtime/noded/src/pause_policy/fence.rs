//! The exclusive side of the phase-2a exec fence (`exec_fence.py`
//! `transition` without `allow_shared`), for the daemon's own pauses (spec
//! §6.2 item 1), and the activity clock (A's mtime).
//!
//! Files per sandbox id in the Warden's lock directory: `.<id>.transition` (T)
//! and `.<id>.activity` (A). A pause takes T, then A, both `LOCK_EX|LOCK_NB`;
//! busy is "the next tick decides" (Python's `SandboxBusyError`). Every locker
//! checks after locking that the path still names its descriptor's inode,
//! because a delete unlinks A, then T, under T.

use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

/// T and A held exclusively; dropping it releases both (A first). Without an
/// A file there is nothing to hold: no exec has made one, and any new exec or
/// agent operation needs T shared first, which this hold excludes.
#[derive(Debug)]
pub struct ExclusiveHold {
    activity: Option<File>,
    transition: File,
}

impl ExclusiveHold {
    /// Release in the reverse of the lock order.
    pub fn release(self) {
        let ExclusiveHold { activity, transition } = self;
        drop(activity);
        drop(transition);
    }
}

/// The fence files of every sandbox on this node; `directory` is the one
/// `ExecFence` uses (`<runtime_root>/warden-locks`).
#[derive(Clone, Debug)]
pub struct PauseFence {
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

/// Open (creating, 0600, when `create`) and flock `LOCK_EX|LOCK_NB`; `None`
/// when busy. Retries while the lock landed on an inode the path no longer
/// names.
fn exclusive(path: &Path, create: bool) -> std::io::Result<Option<File>> {
    loop {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)?;
        // SAFETY: a valid descriptor for the life of `file`.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
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

impl PauseFence {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        PauseFence { directory: directory.into() }
    }

    fn path(&self, sandbox_id: &str, kind: &str) -> Option<PathBuf> {
        safe_id(sandbox_id).then(|| self.directory.join(format!(".{sandbox_id}.{kind}")))
    }

    /// T, then A, both exclusive and non-blocking: `None` when a transition
    /// or any activity holds either (or the id cannot name the files).
    pub fn try_exclusive(&self, sandbox_id: &str) -> std::io::Result<Option<ExclusiveHold>> {
        let (Some(transition), Some(activity)) = (self.path(sandbox_id, "transition"), self.path(sandbox_id, "activity")) else {
            return Ok(None);
        };
        let transition = match exclusive(&transition, true) {
            // The Warden makes its lock directory on first use; so may we.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                crate::fsutil::ensure_private_dir(&self.directory)?;
                exclusive(&transition, true)?
            }
            other => other?,
        };
        let Some(transition) = transition else { return Ok(None) };
        // A is never created here: its mtime is the activity clock, and a new
        // file would read as activity just now.
        let activity = match exclusive(&activity, false) {
            Ok(Some(activity)) => Some(activity),
            Ok(None) => return Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        Ok(Some(ExclusiveHold { activity, transition }))
    }

    /// A's last touch in wall-clock seconds; `None` without the file (no
    /// exec made it, and the agent creates it on its first activity mark).
    pub fn activity_mtime(&self, sandbox_id: &str) -> Option<f64> {
        let meta = std::fs::symlink_metadata(self.path(sandbox_id, "activity")?).ok()?;
        Some(meta.mtime() as f64 + meta.mtime_nsec() as f64 / 1e9)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec_fence::{ExecFence, Fenced};
    use crate::pause::tests::{TempDir, eventually};

    #[test]
    fn a_pause_excludes_execs_and_any_activity_excludes_a_pause() {
        let dir = TempDir::new("fence");
        let (pauses, execs) = (PauseFence::new(&dir.0), ExecFence::new(&dir.0));
        let Fenced::Held(lease) = execs.acquire("box-1").unwrap() else { panic!("free fence is busy") };
        assert!(pauses.try_exclusive("box-1").unwrap().is_none()); // An exec holds A.
        lease.touch();
        let touched = pauses.activity_mtime("box-1").unwrap();
        assert!((touched - std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64()).abs() < 5.0);
        drop(lease);
        // A lock just released can outlive its close while another test
        // thread forks (the child holds every descriptor until its exec).
        let hold = std::cell::RefCell::new(None);
        assert!(eventually(|| {
            let mut slot = hold.borrow_mut();
            if slot.is_none() {
                *slot = pauses.try_exclusive("box-1").unwrap();
            }
            slot.is_some()
        }));
        let hold = hold.into_inner().unwrap();
        assert!(matches!(execs.acquire("box-1").unwrap(), Fenced::Busy)); // T is held.
        assert!(pauses.try_exclusive("box-1").unwrap().is_none());
        hold.release();
        assert!(eventually(|| matches!(execs.acquire("box-1").unwrap(), Fenced::Held(_))));
        // A transition holding T (Python's exclusive) sends the pause away.
        let transition = exclusive(&dir.0.join(".box-1.transition"), true).unwrap().unwrap();
        assert!(pauses.try_exclusive("box-1").unwrap().is_none());
        drop(transition);
        assert!(pauses.try_exclusive("../x").unwrap().is_none());
        // Without A only T is held, and the activity clock is left alone.
        let hold = pauses.try_exclusive("box-2").unwrap().expect("a free fence");
        assert_eq!(pauses.activity_mtime("box-2"), None);
        assert!(matches!(execs.acquire("box-2").unwrap(), Fenced::Busy));
        hold.release();
        // The lock directory is made on first use.
        let fresh = PauseFence::new(dir.0.join("made/locks"));
        assert!(fresh.try_exclusive("box-3").unwrap().is_some());
        let mode = std::fs::metadata(dir.0.join(".box-1.activity")).unwrap().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
