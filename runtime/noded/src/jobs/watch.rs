//! One inotify instance for the daemon's held job reads
//! (docs/managed-process-reads.md, "Waits"). A state directory is watched
//! only while a read waits on it; every event for it wakes all its waiters,
//! which re-read the files; its watch goes when its last waiter does.
//!
//! A watch holds the directory's inode, not its mount, so a held read never
//! keeps a park or delete from unmounting the rootfs: the kernel then drops
//! the watch (`IN_UNMOUNT`, `IN_IGNORED`) and the waiters see the directory
//! gone.

use std::collections::HashMap;
use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::unix::AsyncFd;
use tokio::sync::watch;

/// The events that can change what a read returns, and the directory's end.
pub const WATCH_MASK: u32 = libc::IN_MODIFY
    | libc::IN_CLOSE_WRITE
    | libc::IN_MOVED_TO
    | libc::IN_CREATE
    | libc::IN_DELETE_SELF
    | libc::IN_IGNORED
    | libc::IN_UNMOUNT;
const GONE: u32 = libc::IN_DELETE_SELF | libc::IN_IGNORED | libc::IN_UNMOUNT;
const EVENT_HEADER: usize = std::mem::size_of::<libc::inotify_event>();

#[derive(Debug)]
pub enum WatchError {
    /// The daemon already holds its bound of waiting reads.
    Full,
    Io(io::Error),
}

struct Dir {
    events: watch::Sender<u64>,
    gone: AtomicBool,
}

#[derive(Default)]
struct Dirs {
    /// Watch descriptor -> its directory and how many waiters it has.
    by_wd: HashMap<i32, (Arc<Dir>, usize)>,
    waiters: usize,
}

struct Inner {
    fd: AsyncFd<OwnedFd>,
    dirs: Mutex<Dirs>,
    max_waiters: usize,
}

/// The daemon's inotify instance.
pub struct Watcher {
    inner: Arc<Inner>,
}

/// One waiting read's registration; dropping it (the read answered, or its
/// client went away) releases the watch when it was the last.
pub struct Waiter {
    inner: Arc<Inner>,
    wd: i32,
    dir: Arc<Dir>,
    events: watch::Receiver<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Change {
    /// Something in the directory changed: re-read.
    Changed,
    /// The directory was deleted or unmounted.
    Gone,
}

impl Watcher {
    /// Inside a tokio runtime: the reader task runs as long as it does.
    pub fn new(max_waiters: usize) -> io::Result<Watcher> {
        // SAFETY: no pointers; the descriptor is owned below.
        let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a fresh descriptor.
        let fd = AsyncFd::new(unsafe { OwnedFd::from_raw_fd(fd) })?;
        let inner = Arc::new(Inner { fd, dirs: Mutex::new(Dirs::default()), max_waiters });
        tokio::spawn(read_events(inner.clone()));
        Ok(Watcher { inner })
    }

    /// Wait on the directory `directory` names. Register before reading the
    /// files, so no change between the read and the wait is missed.
    pub fn watch(&self, directory: &OwnedFd) -> Result<Waiter, WatchError> {
        let mut dirs = self.inner.dirs.lock().expect("not poisoned");
        if dirs.waiters >= self.inner.max_waiters {
            return Err(WatchError::Full);
        }
        // The descriptor's own directory, not a path someone could swap.
        let path = CString::new(format!("/proc/self/fd/{}", directory.as_raw_fd())).expect("no NUL");
        // SAFETY: valid descriptors and C string.
        let wd = unsafe { libc::inotify_add_watch(self.inner.fd.as_raw_fd(), path.as_ptr(), WATCH_MASK | libc::IN_ONLYDIR) };
        if wd < 0 {
            return Err(WatchError::Io(io::Error::last_os_error()));
        }
        // The kernel returns the existing descriptor for a watched inode.
        let entry = dirs
            .by_wd
            .entry(wd)
            .or_insert_with(|| (Arc::new(Dir { events: watch::channel(0).0, gone: AtomicBool::new(false) }), 0));
        entry.1 += 1;
        let dir = entry.0.clone();
        dirs.waiters += 1;
        let events = dir.events.subscribe();
        Ok(Waiter { inner: self.inner.clone(), wd, dir, events })
    }

    /// Reads waiting now.
    pub fn waiters(&self) -> usize {
        self.inner.dirs.lock().expect("not poisoned").waiters
    }

    /// Directories watched now.
    pub fn watches(&self) -> usize {
        self.inner.dirs.lock().expect("not poisoned").by_wd.len()
    }
}

impl Waiter {
    /// The next event for this directory after registration or the last call.
    pub async fn changed(&mut self) -> Change {
        if self.dir.gone.load(Ordering::Acquire) {
            return Change::Gone;
        }
        let changed = self.events.changed().await;
        if changed.is_err() || self.dir.gone.load(Ordering::Acquire) { Change::Gone } else { Change::Changed }
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        let mut dirs = self.inner.dirs.lock().expect("not poisoned");
        dirs.waiters -= 1;
        // The kernel may have dropped this watch already (IN_IGNORED).
        let Some((dir, count)) = dirs.by_wd.get_mut(&self.wd) else { return };
        if !Arc::ptr_eq(dir, &self.dir) {
            return;
        }
        *count -= 1;
        if *count == 0 {
            dirs.by_wd.remove(&self.wd);
            // SAFETY: valid descriptors; an already-removed watch is EINVAL.
            unsafe { libc::inotify_rm_watch(self.inner.fd.as_raw_fd(), self.wd) };
        }
    }
}

/// Wake each directory's waiters per batch of events.
async fn read_events(inner: Arc<Inner>) {
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let Ok(mut ready) = inner.fd.readable().await else { return };
        let read = ready.try_io(|fd| {
            // SAFETY: the buffer is valid for its length.
            let read = unsafe { libc::read(fd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
            if read < 0 { Err(io::Error::last_os_error()) } else { Ok(read as usize) }
        });
        let length = match read {
            Ok(Ok(length)) => length,
            Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => continue,
            Ok(Err(error)) => {
                eprintln!("ucloud-noded: job read watches failed: {error}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }
            Err(_would_block) => continue,
        };
        let mut woken: HashMap<i32, bool> = HashMap::new();
        let mut overflow = false;
        let mut offset = 0;
        while offset + EVENT_HEADER <= length {
            // SAFETY: a whole header lies within the bytes read.
            let event: libc::inotify_event = unsafe { std::ptr::read_unaligned(buffer[offset..].as_ptr().cast()) };
            offset += EVENT_HEADER + event.len as usize;
            if event.mask & libc::IN_Q_OVERFLOW != 0 {
                overflow = true;
                continue;
            }
            *woken.entry(event.wd).or_default() |= event.mask & GONE != 0;
        }
        let mut dirs = inner.dirs.lock().expect("not poisoned");
        if overflow {
            // Events were lost: every waiter re-reads.
            for (dir, _) in dirs.by_wd.values() {
                dir.events.send_modify(|count| *count += 1);
            }
        }
        for (wd, gone) in woken {
            let Some((dir, _)) = dirs.by_wd.get(&wd) else { continue };
            if gone {
                dir.gone.store(true, Ordering::Release);
            }
            dir.events.send_modify(|count| *count += 1);
            if gone {
                // The kernel dropped the watch, or will; its waiters answer now.
                dirs.by_wd.remove(&wd);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn scratch(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("noded-watch-{name}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn open(path: &std::path::Path) -> OwnedFd {
        crate::guest::open_directory(path).unwrap()
    }

    #[tokio::test]
    async fn waiters_share_a_watch_wake_on_changes_and_release_it() {
        let root = scratch("share");
        let watcher = Watcher::new(4).unwrap();
        let mut first = watcher.watch(&open(&root)).unwrap();
        let mut second = watcher.watch(&open(&root)).unwrap();
        assert_eq!((watcher.waiters(), watcher.watches()), (2, 1));
        // Nothing happened yet.
        assert!(tokio::time::timeout(Duration::from_millis(50), first.changed()).await.is_err());
        std::fs::write(root.join("state.json"), b"{}").unwrap();
        assert_eq!(tokio::time::timeout(Duration::from_secs(5), first.changed()).await.unwrap(), Change::Changed);
        assert_eq!(tokio::time::timeout(Duration::from_secs(5), second.changed()).await.unwrap(), Change::Changed);
        drop(first);
        assert_eq!((watcher.waiters(), watcher.watches()), (1, 1));
        drop(second);
        assert_eq!((watcher.waiters(), watcher.watches()), (0, 0));
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[tokio::test]
    async fn the_bound_holds_and_a_removed_directory_is_gone() {
        let root = scratch("gone");
        let watcher = Watcher::new(1).unwrap();
        let mut waiter = watcher.watch(&open(&root)).unwrap();
        assert!(matches!(watcher.watch(&open(&root)), Err(WatchError::Full)));
        std::fs::remove_dir_all(&root).unwrap();
        let change = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if waiter.changed().await == Change::Gone {
                    return Change::Gone;
                }
            }
        });
        assert_eq!(change.await.unwrap(), Change::Gone);
        assert_eq!(waiter.changed().await, Change::Gone);
        drop(waiter);
        assert_eq!((watcher.waiters(), watcher.watches()), (0, 0));
    }
}
