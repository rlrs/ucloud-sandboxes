//! `background_io.PressureSampler` and the `resource_evidence` reads under it:
//! `/proc/meminfo`, memory and IO PSI, and the RAM backing's capacity
//! (`sample_memory_backing`: statfs of a verified tmpfs mount). One sample
//! serves every caller for 100 ms.

use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::Clock;
use super::decision::{MemoryBackingCapacity, Pressure};

/// A sample younger than this is served again.
pub const PRESSURE_CACHE_SECONDS: f64 = 0.1;

/// `read_proc_meminfo`: kB values by key, non-negative only; MemAvailable
/// falls back to MemFree.
pub fn read_meminfo(path: &Path) -> Vec<(String, u64)> {
    let Ok(text) = std::fs::read_to_string(path) else { return Vec::new() };
    let mut values: Vec<(String, u64)> = Vec::new();
    for line in text.lines() {
        let Some((key, value)) = line.split_once(':') else { continue };
        let Some(first) = value.split_whitespace().next() else { continue };
        // Python int(): an optional sign, digits (and underscores, never seen here).
        let Ok(parsed) = first.parse::<i64>() else { continue };
        if let Ok(parsed) = u64::try_from(parsed) {
            values.retain(|(existing, _)| existing != key);
            values.push((key.to_string(), parsed));
        }
    }
    if !values.iter().any(|(key, _)| key == "MemAvailable")
        && let Some(free) = values.iter().find(|(key, _)| key == "MemFree").map(|(_, value)| *value)
    {
        values.push(("MemAvailable".into(), free));
    }
    values
}

/// `read_proc_pressure`: the avg10 of each line ("some", "full") within 0-100.
pub fn read_psi(path: &Path) -> Vec<(String, f64)> {
    let Ok(text) = std::fs::read_to_string(path) else { return Vec::new() };
    let mut values = Vec::new();
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        let Some(kind) = fields.next() else { continue };
        for field in fields {
            let Some((key, value)) = field.split_once('=') else { continue };
            if key != "avg10" {
                continue;
            }
            if let Ok(parsed) = value.parse::<f64>()
                && parsed.is_finite()
                && (0.0..=100.0).contains(&parsed)
            {
                values.push((kind.to_string(), parsed));
            }
            break;
        }
    }
    values
}

fn get<T: Copy>(values: &[(String, T)], key: &str) -> Option<T> {
    values.iter().rev().find(|(name, _)| name == key).map(|(_, value)| *value)
}

/// Linux `dev_t` major and minor (glibc's `gnu_dev_major`/`minor`), as
/// mountinfo prints them.
fn device_name(dev: u64) -> String {
    let major = ((dev >> 32) & 0xffff_f000) | ((dev >> 8) & 0xfff);
    let minor = ((dev >> 12) & 0xffff_ff00) | (dev & 0xff);
    format!("{major}:{minor}")
}

/// mountinfo's octal escapes for space, tab and backslash.
fn unescape(path: &str) -> String {
    path.replace("\\040", " ").replace("\\011", "\t").replace("\\134", "\\")
}

/// Whether mount `mount_id` is a tmpfs at `root` on `device`.
fn verified_mount(proc_root: &Path, root: &Path, mount_id: &str, device: &str) -> bool {
    let Ok(table) = std::fs::read_to_string(proc_root.join("self/mountinfo")) else { return false };
    for line in table.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.first() != Some(&mount_id) {
            continue;
        }
        let Some(separator) = parts.iter().position(|part| *part == "-") else { return false };
        return parts.len() > 4
            && unescape(parts[4]) == root.to_string_lossy()
            && parts.get(separator + 1) == Some(&"tmpfs")
            && parts[2] == device;
    }
    false
}

/// `fstatfs` through the raw syscall: (blocks, available blocks, fragment size).
fn statfs(file: &File) -> std::io::Result<(u64, u64, u64)> {
    // SAFETY: a zeroed statfs is a valid out-buffer for the kernel's struct
    // statfs, which libc::statfs mirrors on x86_64 (glibc and musl alike).
    let mut buffer: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: a valid descriptor and a buffer of the kernel's statfs size.
    if unsafe { libc::syscall(libc::SYS_fstatfs, file.as_raw_fd(), &mut buffer as *mut libc::statfs) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok((buffer.f_blocks as u64, buffer.f_bavail as u64, buffer.f_frsize as u64))
}

/// The verified-mount cache key (Python's lru_cache on the same tuple).
type MountKey = (String, String, u64);

/// `sample_memory_backing`: `None` when no RAM backing is configured; a
/// configured but missing, replaced or unverified mount is explicit unknown.
pub fn sample_memory_backing(root: Option<&Path>, proc_root: &Path, verified: &Mutex<Option<MountKey>>) -> Option<MemoryBackingCapacity> {
    let root = root?;
    if !root.is_absolute() {
        return Some(MemoryBackingCapacity::default());
    }
    let measure = || -> Option<MemoryBackingCapacity> {
        let directory = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(root)
            .ok()?;
        let info = directory.metadata().ok()?;
        let device = device_name(info.dev());
        let fdinfo = std::fs::read_to_string(proc_root.join(format!("self/fdinfo/{}", directory.as_raw_fd()))).ok()?;
        let mount_id = fdinfo.lines().find_map(|line| line.strip_prefix("mnt_id:"))?.split_whitespace().next()?.to_string();
        let key = (mount_id.clone(), device.clone(), info.ino());
        let known = verified.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).as_ref() == Some(&key);
        if !known {
            if !verified_mount(proc_root, root, &mount_id, &device) {
                return None;
            }
            *verified.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(key);
        }
        let (blocks, available, fragment) = statfs(&directory).ok()?;
        // A replaced mount is another filesystem's capacity, not ours.
        let current = std::fs::metadata(root).ok()?;
        if (info.dev(), info.ino()) != (current.dev(), current.ino()) {
            return None;
        }
        MemoryBackingCapacity::measured(blocks.saturating_mul(fragment), available.saturating_mul(fragment), format!("{mount_id}:{device}:{}", info.ino()))
    };
    Some(measure().unwrap_or_default())
}

/// What the reclaim tick asks for each tick.
pub trait PressureSource: Send + Sync {
    fn sample(&self) -> Pressure;
}

/// The node's pressure sampler, cached for `PRESSURE_CACHE_SECONDS`.
pub struct PressureSampler {
    proc_root: PathBuf,
    memory_backing_root: Option<PathBuf>,
    clock: Arc<dyn Clock>,
    cache: Mutex<Option<(f64, Pressure)>>,
    verified: Mutex<Option<MountKey>>,
}

impl PressureSampler {
    pub fn new(proc_root: PathBuf, memory_backing_root: Option<PathBuf>, clock: Arc<dyn Clock>) -> Self {
        PressureSampler { proc_root, memory_backing_root, clock, cache: Mutex::new(None), verified: Mutex::new(None) }
    }

    fn measure(&self) -> Pressure {
        let memory = read_meminfo(&self.proc_root.join("meminfo"));
        let memory_psi = read_psi(&self.proc_root.join("pressure/memory"));
        let io_psi = read_psi(&self.proc_root.join("pressure/io"));
        let (total, available) = (get(&memory, "MemTotal"), get(&memory, "MemAvailable"));
        Pressure {
            memory_fraction: match (total, available) {
                (Some(total), Some(available)) if total != 0 => available as f64 / total as f64,
                _ => 0.0,
            },
            memory_stall: get(&memory_psi, "some").unwrap_or(100.0),
            io_stall: get(&io_psi, "some").unwrap_or(0.0),
            memory_available_bytes: available.map_or(0, |kib| kib.saturating_mul(1024)),
            memory_backing: sample_memory_backing(self.memory_backing_root.as_deref(), &self.proc_root, &self.verified),
            swap_total_bytes: get(&memory, "SwapTotal").map(|kib| kib.saturating_mul(1024)),
            swap_free_bytes: get(&memory, "SwapFree").map(|kib| kib.saturating_mul(1024)),
        }
    }
}

impl PressureSource for PressureSampler {
    fn sample(&self) -> Pressure {
        let mut cache = self.cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = self.clock.monotonic();
        if let Some((at, value)) = cache.as_ref()
            && now - at < PRESSURE_CACHE_SECONDS
        {
            return value.clone();
        }
        let value = self.measure();
        *cache = Some((now, value.clone()));
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pause::tests::TempDir;
    use crate::pause_policy::tests::FakeClock;

    const MIB: u64 = 1 << 20;

    #[test]
    fn meminfo_psi_and_swap_are_read_as_python_reads_them() {
        let dir = TempDir::new("pressure");
        let proc = dir.0.clone();
        std::fs::create_dir_all(proc.join("pressure")).unwrap();
        std::fs::write(proc.join("meminfo"), "MemTotal: 1048576 kB\nMemAvailable: 524288 kB\nSwapTotal: 2048 kB\nSwapFree: 1024 kB\nOdd line\nBad: x kB\n").unwrap();
        std::fs::write(proc.join("pressure/memory"), "some avg10=12.50 avg60=1.00 avg300=0.00 total=1\nfull avg10=3.00 avg60=0 avg300=0 total=0\n").unwrap();
        std::fs::write(proc.join("pressure/io"), "some avg10=150.00 avg60=0 avg300=0 total=0\n").unwrap();
        let clock = FakeClock::new();
        let sampler = PressureSampler::new(proc.clone(), None, clock.clone());
        let sample = sampler.sample();
        assert_eq!((sample.memory_fraction, sample.memory_available_bytes), (0.5, 512 * MIB));
        assert_eq!((sample.swap_total_bytes, sample.swap_free_bytes), (Some(2 * MIB), Some(MIB)));
        // io avg10 out of range is unreadable: 0; memory reads 12.5.
        assert_eq!((sample.memory_stall, sample.io_stall, &sample.memory_backing), (12.5, 0.0, &None));

        // Cached for 100 ms, then re-read.
        std::fs::write(proc.join("meminfo"), "MemTotal: 1048576 kB\nMemFree: 262144 kB\n").unwrap();
        std::fs::remove_file(proc.join("pressure/memory")).unwrap();
        clock.advance(0.05);
        assert_eq!(sampler.sample(), sample);
        clock.advance(0.05);
        let fresh = sampler.sample();
        assert_eq!((fresh.memory_fraction, fresh.memory_stall), (0.25, 100.0)); // MemFree stands in.
        assert_eq!((fresh.swap_total_bytes, fresh.swap_free_bytes), (None, None));

        std::fs::remove_file(proc.join("meminfo")).unwrap();
        clock.advance(1.0);
        assert_eq!((sampler.sample().memory_fraction, sampler.sample().memory_available_bytes), (0.0, 0));
    }

    #[test]
    fn a_configured_backing_that_is_not_a_verified_tmpfs_is_unknown() {
        let dir = TempDir::new("backing");
        let clock = FakeClock::new();
        // A plain directory is not the tmpfs mount it claims to be.
        let sampler = PressureSampler::new(PathBuf::from("/proc"), Some(dir.0.clone()), clock.clone());
        assert_eq!(sampler.sample().memory_backing, Some(MemoryBackingCapacity::default()));
        let relative = PressureSampler::new(PathBuf::from("/proc"), Some(PathBuf::from("relative")), clock);
        assert_eq!(relative.sample().memory_backing, Some(MemoryBackingCapacity::default()));
    }

    #[test]
    fn a_real_tmpfs_mount_is_measured_when_one_exists() {
        // /dev/shm is a tmpfs on most Linux hosts; skip quietly elsewhere.
        let root = Path::new("/dev/shm");
        let Ok(mounts) = std::fs::read_to_string("/proc/self/mountinfo") else { return };
        if !mounts.lines().any(|line| line.split_whitespace().nth(4) == Some("/dev/shm") && line.contains(" - tmpfs ")) {
            eprintln!("skipped: no tmpfs at /dev/shm");
            return;
        }
        let verified = Mutex::new(None);
        let measured = sample_memory_backing(Some(root), Path::new("/proc"), &verified).unwrap();
        assert!(measured.total_bytes.is_some_and(|total| total > 0), "{measured:?}");
        assert!(measured.available_bytes <= measured.total_bytes);
        assert!(verified.lock().unwrap().is_some());
        // The second sample takes the cached proof.
        assert!(sample_memory_backing(Some(root), Path::new("/proc"), &verified).unwrap().total_bytes.is_some());
    }

    #[test]
    fn devices_are_named_as_mountinfo_names_them() {
        assert_eq!(device_name(0), "0:0");
        assert_eq!(device_name((8 << 8) | 1), "8:1");
        assert_eq!(device_name((259u64 << 8) | 3 | (0x100 << 12)), "259:259");
        assert_eq!(unescape("/a\\040b\\011c\\134d"), "/a b\tc\\d");
    }
}
