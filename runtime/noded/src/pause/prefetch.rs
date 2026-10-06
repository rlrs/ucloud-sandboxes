//! Thaw prefetch (`pause_tier.memory_pieces`, `pause_tier.prefetch`): read a
//! paused sandbox's swapped application memory back in parallel before
//! `runsc resume`, so the guest does not fault it in page by page.
//!
//! Qualification: 4 MiB pieces over 8 threads swapped 645 MiB back in
//! 0.81-0.95 s, where the guest's own faults took 3.6-5.0 s; eight thaws with
//! 64 readers reached 1.5 GiB/s together. A thaw waits for at most 1 GiB and
//! 2 s; demand faults bring back the rest.

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

pub const PREFETCH_THREADS: usize = 8;
pub const PREFETCH_NODE_THREADS: usize = 64;
pub const PREFETCH_PIECE_BYTES: u64 = 4 * 1024 * 1024;
pub const PREFETCH_READ_BYTES: usize = 1024 * 1024;
pub const PREFETCH_MAX_BYTES: u64 = 1024 * 1024 * 1024;
pub const PREFETCH_SECONDS: Duration = Duration::from_secs(2);
/// Less swap than this is about the Sentry's and gofer's own heaps (15 MiB,
/// which no memory-file read restores): nothing worth a prefetch moved out.
pub const PREFETCH_MIN_SWAP_BYTES: u64 = 32 * 1024 * 1024;

/// How long a thaw waits for its first reader slot between cancel checks.
const FIRST_SLOT_STEP: Duration = Duration::from_millis(50);

/// The node's reader slots (Python: a `BoundedSemaphore`). However many thaws
/// run, the node has at most this many readers and read buffers.
#[derive(Debug)]
pub struct Slots {
    free: Mutex<usize>,
    freed: Condvar,
}

impl Slots {
    pub fn new(count: usize) -> Self {
        Slots { free: Mutex::new(count), freed: Condvar::new() }
    }

    pub fn available(&self) -> usize {
        *self.free.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn try_acquire(&self) -> Option<Slot<'_>> {
        let mut free = self.free.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        (*free > 0).then(|| {
            *free -= 1;
            Slot(self)
        })
    }

    fn acquire_timeout(&self, timeout: Duration) -> Option<Slot<'_>> {
        let free = self.free.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let (mut free, _) = self
            .freed
            .wait_timeout_while(free, timeout, |free| *free == 0)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (*free > 0).then(|| {
            *free -= 1;
            Slot(self)
        })
    }
}

/// One held slot; dropping it (a reader's exit, by any path) returns it.
struct Slot<'a>(&'a Slots);

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        *self.0.free.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
        self.0.freed.notify_one();
    }
}

fn lseek(file: &File, offset: u64, whence: libc::c_int) -> io::Result<u64> {
    // SAFETY: lseek on a valid descriptor; it only moves the file offset.
    let at = unsafe { libc::lseek(file.as_raw_fd(), offset as libc::off_t, whence) };
    if at < 0 { Err(io::Error::last_os_error()) } else { Ok(at as u64) }
}

/// `(start, end)` pieces of the file's data extents in file order, up to a
/// byte budget. On tmpfs a swapped-out page is still data; a hole holds
/// nothing to read.
pub fn memory_pieces(file: &File, budget_bytes: u64, piece_bytes: u64) -> io::Result<Vec<(u64, u64)>> {
    let (mut pieces, mut offset, mut budget) = (Vec::new(), 0u64, budget_bytes);
    let size = file.metadata()?.len();
    while offset < size && budget > 0 {
        let start = match lseek(file, offset, libc::SEEK_DATA) {
            Ok(start) => start,
            Err(error) if error.raw_os_error() == Some(libc::ENXIO) => break, // Only a hole remains.
            Err(error) => return Err(error),
        };
        offset = lseek(file, start, libc::SEEK_HOLE)?.min(start + budget);
        let mut at = start;
        while at < offset {
            pieces.push((at, (at + piece_bytes).min(offset)));
            at += piece_bytes;
        }
        budget -= offset - start;
    }
    Ok(pieces)
}

/// A positional read (`preadv` in Python); injectable for tests.
pub(crate) type ReadAt = dyn Fn(&File, &mut [u8], u64) -> io::Result<usize> + Send + Sync;

pub(crate) fn pread(file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buffer, offset)
}

/// Read the pieces back in parallel; the bytes read.
///
/// Each reader holds one of the node's `slots` for its whole life. A thaw
/// waits for its first slot and then takes only free ones, up to `threads`.
/// Readers stop between reads once `cancelled()` is true or one has failed.
/// This returns only after every reader has exited; a reader's error is
/// returned after that.
pub fn prefetch(file: &File, pieces: &[(u64, u64)], slots: &Slots, cancelled: &(dyn Fn() -> bool + Sync), threads: usize) -> io::Result<u64> {
    prefetch_with(file, pieces, slots, cancelled, threads, &pread, &|_| true)
}

/// `prefetch` with its reads and thread starts injectable (`may_start(n)`
/// false models the node refusing the n-th reader thread).
pub(crate) fn prefetch_with(
    file: &File,
    pieces: &[(u64, u64)],
    slots: &Slots,
    cancelled: &(dyn Fn() -> bool + Sync),
    threads: usize,
    read_at: &ReadAt,
    may_start: &(dyn Fn(usize) -> bool + Sync),
) -> io::Result<u64> {
    let next = AtomicUsize::new(0);
    let total = AtomicU64::new(0);
    let failed = AtomicBool::new(false);
    let error: Mutex<Option<io::Error>> = Mutex::new(None);
    let reader = |slot: Slot<'_>| {
        let _slot = slot;
        let mut buffer = vec![0u8; PREFETCH_READ_BYTES];
        let mut count = 0u64;
        'pieces: while !failed.load(Ordering::SeqCst) && !cancelled() {
            let Some(&(mut at, end)) = pieces.get(next.fetch_add(1, Ordering::SeqCst)) else { break };
            while at < end && !cancelled() && !failed.load(Ordering::SeqCst) {
                let want = (end - at).min(PREFETCH_READ_BYTES as u64) as usize;
                match read_at(file, &mut buffer[..want], at) {
                    Ok(0) => break, // The file shrank: nothing more here.
                    Ok(read) => {
                        at += read as u64;
                        count += read as u64;
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => {
                        failed.store(true, Ordering::SeqCst);
                        error.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).get_or_insert(e);
                        break 'pieces;
                    }
                }
            }
        }
        total.fetch_add(count, Ordering::SeqCst);
    };
    std::thread::scope(|scope| {
        let mut started = 0usize;
        while started < threads.min(pieces.len()) && !cancelled() {
            let slot = if started == 0 {
                match slots.acquire_timeout(FIRST_SLOT_STEP) {
                    Some(slot) => slot,
                    None => continue,
                }
            } else {
                match slots.try_acquire() {
                    Some(slot) => slot,
                    None => break,
                }
            };
            // No thread to spare: fewer readers, never a failed thaw.
            if !may_start(started) {
                break; // The slot drops here.
            }
            let reader = &reader;
            let spawned = std::thread::Builder::new().name("thaw-prefetch".into()).spawn_scoped(scope, move || reader(slot));
            if spawned.is_err() {
                break;
            }
            started += 1;
        }
    }); // Joins every reader.
    match error.into_inner().unwrap_or_else(|poisoned| poisoned.into_inner()) {
        Some(error) => Err(error),
        None => Ok(total.into_inner()),
    }
}

/// `cancelled` for one thaw: an explicit cancel (a delete) or its time budget.
pub(crate) fn deadline(cancel: &AtomicBool, started: Instant, budget: Duration) -> impl Fn() -> bool + Sync + '_ {
    move || cancel.load(Ordering::SeqCst) || started.elapsed() >= budget
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pause::tests::TempDir;
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    const MIB: u64 = 1024 * 1024;

    /// Data, an 8 MiB hole (never written), data, then a trailing hole.
    fn memory() -> (TempDir, File) {
        let dir = TempDir::new("prefetch");
        let path = dir.0.join("application_memory.active");
        let mut file = File::create(&path).unwrap();
        let data: Vec<u8> = (0..8 * MIB).map(|i| (i * 7 + 3) as u8).collect();
        file.write_all(&data).unwrap();
        file.seek(SeekFrom::Start(16 * MIB)).unwrap();
        file.write_all(&data).unwrap();
        file.set_len(32 * MIB).unwrap();
        drop(file);
        let file = File::open(&path).unwrap();
        (dir, file)
    }

    /// A swap device: each read blocks for `delay`; counts calls, reads in
    /// flight (and their maximum), and fails the `fail_at`-th call.
    struct SlowReads {
        delay: Duration,
        fail_at: Option<usize>,
        calls: Mutex<Vec<u64>>,
        active: Mutex<(usize, usize)>,
    }

    impl SlowReads {
        fn new(delay: Duration, fail_at: Option<usize>) -> Arc<Self> {
            Arc::new(SlowReads { delay, fail_at, calls: Mutex::new(Vec::new()), active: Mutex::new((0, 0)) })
        }

        fn read(&self, file: &File, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
            {
                let mut calls = self.calls.lock().unwrap();
                calls.push(offset);
                if self.fail_at == Some(calls.len()) {
                    return Err(io::Error::from_raw_os_error(libc::EIO));
                }
                let mut active = self.active.lock().unwrap();
                active.0 += 1;
                active.1 = active.1.max(active.0);
            }
            std::thread::sleep(self.delay);
            let result = pread(file, buffer, offset);
            self.active.lock().unwrap().0 -= 1;
            result
        }

        fn calls(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    fn run(file: &File, slots: &Slots, reads: &Arc<SlowReads>, cancelled: &(dyn Fn() -> bool + Sync), threads: usize) -> io::Result<u64> {
        let pieces = memory_pieces(file, PREFETCH_MAX_BYTES, PREFETCH_PIECE_BYTES).unwrap();
        let reads = reads.clone();
        prefetch_with(file, &pieces, slots, cancelled, threads, &move |f, b, o| reads.read(f, b, o), &|_| true)
    }

    #[test]
    fn pieces_cover_only_data_extents_within_the_byte_budget() {
        let (dir, file) = memory();
        assert_eq!(memory_pieces(&file, PREFETCH_MAX_BYTES, PREFETCH_PIECE_BYTES).unwrap(),
                   vec![(0, 4 * MIB), (4 * MIB, 8 * MIB), (16 * MIB, 20 * MIB), (20 * MIB, 24 * MIB)]);
        assert_eq!(memory_pieces(&file, 10 * MIB, PREFETCH_PIECE_BYTES).unwrap(),
                   vec![(0, 4 * MIB), (4 * MIB, 8 * MIB), (16 * MIB, 18 * MIB)]);
        let hole = dir.0.join("hole");
        File::create(&hole).unwrap().set_len(8 * MIB).unwrap();
        assert_eq!(memory_pieces(&File::open(&hole).unwrap(), PREFETCH_MAX_BYTES, PREFETCH_PIECE_BYTES).unwrap(), vec![]);
    }

    #[test]
    fn parallel_readers_beat_one_reader_and_read_every_data_byte() {
        let (_dir, file) = memory();
        let slots = Slots::new(PREFETCH_NODE_THREADS);
        let mut timings = Vec::new();
        for threads in [1, 8] {
            let reads = SlowReads::new(Duration::from_millis(10), None);
            let started = Instant::now();
            let read = run(&file, &slots, &reads, &|| false, threads).unwrap();
            timings.push(started.elapsed());
            assert_eq!((read, reads.calls()), (16 * MIB, 16));
        }
        // 16 reads of 10 ms: about 160 ms alone and 40 ms over four pieces.
        assert!(timings[1] < timings[0] / 2, "{timings:?}");
        assert_eq!(slots.available(), PREFETCH_NODE_THREADS);
    }

    #[test]
    fn node_slots_bound_readers_across_concurrent_thaws() {
        let (_dir, file) = memory();
        let reads = SlowReads::new(Duration::from_millis(5), None);
        let slots = Slots::new(2);
        std::thread::scope(|scope| {
            for _ in 0..6 {
                scope.spawn(|| run(&file, &slots, &reads, &|| false, PREFETCH_THREADS).unwrap());
            }
        });
        // Waiting thaws hold neither a reader nor a buffer; every thaw reads.
        assert_eq!(reads.active.lock().unwrap().1, 2);
        assert_eq!(reads.calls(), 6 * 16);
        assert_eq!(slots.available(), 2);
    }

    #[test]
    fn cancellation_stops_between_reads_and_waits_for_every_reader() {
        let (_dir, file) = memory();
        let reads = SlowReads::new(Duration::from_millis(10), None);
        let slots = Slots::new(PREFETCH_NODE_THREADS);
        let probe = reads.clone();
        let read = run(&file, &slots, &reads, &move || probe.calls() >= 3, PREFETCH_THREADS).unwrap();
        assert!(read < 16 * MIB);
        assert!(reads.calls() <= 3 + PREFETCH_THREADS);
        assert_eq!(read, reads.calls() as u64 * MIB); // No reader outlives the call.
        assert_eq!(reads.active.lock().unwrap().0, 0);
        assert_eq!(slots.available(), PREFETCH_NODE_THREADS);
    }

    #[test]
    fn a_failed_read_stops_the_readers_and_returns_after_they_exit() {
        let (_dir, file) = memory();
        let reads = SlowReads::new(Duration::from_millis(5), Some(3));
        let slots = Slots::new(PREFETCH_NODE_THREADS);
        let error = run(&file, &slots, &reads, &|| false, PREFETCH_THREADS).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        assert!(reads.calls() < 16);
        assert_eq!(reads.active.lock().unwrap().0, 0);
        assert_eq!(slots.available(), PREFETCH_NODE_THREADS); // Every slot returned.
    }

    #[test]
    fn readers_the_node_cannot_start_are_skipped() {
        let (_dir, file) = memory();
        let slots = Slots::new(PREFETCH_NODE_THREADS);
        let pieces = memory_pieces(&file, PREFETCH_MAX_BYTES, PREFETCH_PIECE_BYTES).unwrap();
        let read = prefetch_with(&file, &pieces, &slots, &|| false, PREFETCH_THREADS, &pread, &|n| n < 2).unwrap();
        assert_eq!(read, 16 * MIB);
        assert_eq!(prefetch_with(&file, &pieces, &slots, &|| false, PREFETCH_THREADS, &pread, &|_| false).unwrap(), 0);
        assert_eq!(slots.available(), PREFETCH_NODE_THREADS);
    }

    #[test]
    fn a_thaw_waits_for_its_first_slot_until_cancelled() {
        let (_dir, file) = memory();
        let slots = Slots::new(1);
        let held = slots.try_acquire().unwrap();
        let (cancel, started) = (AtomicBool::new(false), Instant::now());
        let read = std::thread::scope(|scope| {
            let waiting = scope.spawn(|| prefetch(&file, &[(0, MIB)], &slots, &deadline(&cancel, started, Duration::from_secs(60)), 8));
            std::thread::sleep(Duration::from_millis(120));
            cancel.store(true, Ordering::SeqCst);
            waiting.join().unwrap().unwrap()
        });
        assert_eq!(read, 0);
        drop(held);
        assert_eq!(prefetch(&file, &[(0, MIB)], &slots, &|| false, 8).unwrap(), MIB);
    }
}
