//! Pause markers (`direct_warden.py` `_pause_marker`, `is_paused`,
//! `paused_keys`) and the cross-process "thaw in progress" signal.
//!
//! `<runtime_root>/warden-paused/<id>.sandbox-<generation>` holds the container
//! id, written atomically through a dot temporary. It is durable before
//! `runsc pause` and removed only after `runsc resume` succeeds, so a paused
//! runtime always has one; one on a running runtime (a crash between the two
//! steps) is harmless.
//!
//! A thaw holds an exclusive flock on the marker from just after it finds it
//! until after it unlinks it: through the prefetch and `runsc resume`. Any
//! process can see "a thaw is working" as that lock (Python's in-process
//! `_prefetches` could not). The lock is on an inode the thaw then unlinks:
//! - a later pause renames a new file into place, so a stale holder of the old
//!   inode never blocks it or makes it look thawed;
//! - a probe therefore re-opens the marker by path every time and, once it
//!   holds its shared lock, checks that the path still names that inode.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

pub const PAUSED_DIRECTORY: &str = "warden-paused";

pub fn marker_path(runtime_root: &Path, sandbox_id: &str, generation: u64) -> PathBuf {
    runtime_root.join(PAUSED_DIRECTORY).join(format!("{sandbox_id}.sandbox-{generation}"))
}

/// Python `is_paused`: `os.path.lexists(marker)`. Observation only: a thaw
/// acts under the warden flock and never trusts it.
pub fn exists(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok()
}

/// Python `paused_keys`: `(sandbox_id, generation)` of every marker; dot
/// names are atomic-write temporaries, not markers.
pub fn paused_keys(runtime_root: &Path) -> io::Result<Vec<(String, u64)>> {
    let entries = match std::fs::read_dir(runtime_root.join(PAUSED_DIRECTORY)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut keys = Vec::new();
    for entry in entries {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.starts_with('.') {
            continue;
        }
        let Some((sandbox_id, generation)) = name.rsplit_once(".sandbox-") else { continue };
        if sandbox_id.is_empty() || generation.is_empty() || !generation.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        if let Ok(generation) = generation.parse() {
            keys.push((sandbox_id.to_string(), generation));
        }
    }
    Ok(keys)
}

fn open(path: &Path) -> io::Result<Option<File>> {
    match OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC).open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// `Ok(true)` when locked, `Ok(false)` when another description holds it.
fn flock(file: &File, operation: libc::c_int) -> io::Result<bool> {
    loop {
        // SAFETY: a valid descriptor for the life of `file`.
        if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EWOULDBLOCK) => return Ok(false),
            Some(libc::EINTR) => continue,
            _ => return Err(error),
        }
    }
}

/// Whether `path` names the inode `file` refers to: `None` if the path is gone.
fn names(path: &Path, file: &File) -> io::Result<Option<bool>> {
    let held = file.metadata()?;
    match std::fs::symlink_metadata(path) {
        Ok(current) => Ok(Some((current.dev(), current.ino()) == (held.dev(), held.ino()))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Python `thawing`: a thaw (in any process) holds this marker's flock.
pub fn thawing(path: &Path) -> bool {
    let Ok(Some(file)) = open(path) else { return false };
    // Busy means a thaw holds it; closing `file` releases a probe's own lock.
    matches!(flock(&file, libc::LOCK_SH | libc::LOCK_NB), Ok(false))
}

/// Paused and no thaw in progress: the marker exists, its shared flock is free,
/// and the path still names the locked inode (spec §6.2 item 5). A paused
/// reclaim probes this before every window; it re-opens by path each time.
pub fn settled(path: &Path) -> bool {
    loop {
        let Ok(Some(file)) = open(path) else { return false };
        if !matches!(flock(&file, libc::LOCK_SH | libc::LOCK_NB), Ok(true)) {
            return false; // A thaw is working (or the lock failed): not settled.
        }
        match names(path, &file) {
            Ok(Some(true)) => return true,
            Ok(Some(false)) => continue, // Replaced by a newer pause: probe that one.
            Ok(None) | Err(_) => return false, // A thaw finished meanwhile.
        }
    }
}

/// A thaw's exclusive hold on the marker. Dropping it closes the descriptor
/// (never `LOCK_UN`), so a duplicate the prefetch still holds keeps the lock
/// until that reader exits too.
#[derive(Debug)]
pub struct ThawHold {
    file: File,
}

impl ThawHold {
    /// Lock the marker that `path` names, or `None` when there is none (not
    /// paused). Retries while the lock landed on an inode the path no longer
    /// names. Blocking: a probe holds the shared lock for microseconds, and
    /// thaws exclude each other with the warden flock first.
    pub fn acquire(path: &Path) -> io::Result<Option<ThawHold>> {
        loop {
            let Some(file) = open(path)? else { return Ok(None) };
            flock(&file, libc::LOCK_EX)?;
            match names(path, &file)? {
                Some(true) => return Ok(Some(ThawHold { file })),
                Some(false) => continue,
                None => return Ok(None),
            }
        }
    }

    /// Like `acquire`, but `Ok(Err(()))` instead of waiting when the lock is busy.
    pub fn try_acquire(path: &Path) -> io::Result<Result<Option<ThawHold>, ()>> {
        loop {
            let Some(file) = open(path)? else { return Ok(Ok(None)) };
            if !flock(&file, libc::LOCK_EX | libc::LOCK_NB)? {
                return Ok(Err(()));
            }
            match names(path, &file)? {
                Some(true) => return Ok(Ok(Some(ThawHold { file }))),
                Some(false) => continue,
                None => return Ok(Ok(None)),
            }
        }
    }

    /// A descriptor on the same open file description: it shares the lock.
    pub fn share(&self) -> io::Result<File> {
        self.file.try_clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fsutil::atomic_write;
    use crate::pause::tests::{TempDir, eventually};

    fn marker(dir: &TempDir) -> PathBuf {
        let path = marker_path(&dir.0, "box-1", 3);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        atomic_write(&path, b"container-1").unwrap();
        path
    }

    #[test]
    fn markers_hold_the_container_id_and_dot_temporaries_are_not_keys() {
        let dir = TempDir::new("marker");
        let path = marker(&dir);
        assert_eq!(path, dir.0.join("warden-paused/box-1.sandbox-3"));
        assert_eq!(std::fs::read(&path).unwrap(), b"container-1");
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        std::fs::write(dir.0.join("warden-paused/.box-1.sandbox-3.tmp"), b"").unwrap();
        std::fs::write(dir.0.join("warden-paused/odd"), b"").unwrap();
        std::fs::write(dir.0.join("warden-paused/x.sandbox-y"), b"").unwrap();
        std::fs::write(dir.0.join("warden-paused/.sandbox-4"), b"").unwrap();
        assert_eq!(paused_keys(&dir.0).unwrap(), vec![("box-1".to_string(), 3)]);
        assert!(exists(&path) && !thawing(&path) && settled(&path));
        assert_eq!(paused_keys(&dir.0.join("absent")).unwrap(), vec![]);
    }

    #[test]
    fn a_thaw_hold_is_visible_to_every_other_open_file_description() {
        let dir = TempDir::new("marker");
        let path = marker(&dir);
        let hold = ThawHold::acquire(&path).unwrap().unwrap();
        assert!(thawing(&path) && !settled(&path) && exists(&path));
        assert!(ThawHold::try_acquire(&path).unwrap().is_err());
        // A duplicate (the prefetch's) keeps the lock after the hold drops.
        let shared = hold.share().unwrap();
        drop(hold);
        assert!(thawing(&path));
        drop(shared);
        assert!(eventually(|| !thawing(&path) && settled(&path)));
        assert!(ThawHold::acquire(&dir.0.join("warden-paused/none.sandbox-1")).unwrap().is_none());
    }

    #[test]
    fn an_unlinked_marker_still_locked_never_blocks_or_hides_a_new_pause() {
        let dir = TempDir::new("marker");
        let path = marker(&dir);
        let stale = ThawHold::acquire(&path).unwrap().unwrap();
        let probe = open(&path).unwrap().unwrap(); // Opened before the unlink.
        std::fs::remove_file(&path).unwrap(); // The thaw resumed and unlinked.
        assert!(!exists(&path) && !thawing(&path) && !settled(&path));
        drop(stale);
        // The probe's lock now succeeds, but on an inode the path no longer names.
        assert!(eventually(|| flock(&probe, libc::LOCK_SH | libc::LOCK_NB).unwrap()));
        assert_eq!(names(&path, &probe).unwrap(), None);
        // While a stale thaw still holds the old inode, a new pause is settled.
        let path = marker(&dir);
        let old = ThawHold::acquire(&path).unwrap().unwrap();
        std::fs::remove_file(&path).unwrap();
        atomic_write(&path, b"container-1").unwrap();
        assert!(exists(&path) && !thawing(&path) && settled(&path));
        let new = ThawHold::try_acquire(&path).unwrap().unwrap().unwrap();
        drop((old, new));
    }

    #[test]
    fn another_process_holding_the_marker_lock_is_a_thaw_in_progress() {
        let Ok(status) = std::process::Command::new("flock").arg("--version").output() else {
            eprintln!("skipped: no flock(1)");
            return;
        };
        if !status.status.success() {
            eprintln!("skipped: no usable flock(1)");
            return;
        }
        let dir = TempDir::new("marker");
        let path = marker(&dir);
        let ready = dir.0.join("ready");
        let done = dir.0.join("done");
        let mut child = std::process::Command::new("flock")
            .args(["-x", "-o"])
            .arg(&path)
            .arg("-c")
            .arg(format!("touch '{}'; while [ ! -e '{}' ]; do sleep 0.01; done", ready.display(), done.display()))
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !ready.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let observed = (thawing(&path), settled(&path));
        std::fs::write(&done, b"").unwrap();
        let _ = child.wait();
        assert_eq!(observed, (true, false));
        assert!(eventually(|| !thawing(&path) && settled(&path)));
    }
}
