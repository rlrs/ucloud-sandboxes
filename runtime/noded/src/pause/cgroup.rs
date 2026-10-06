//! The two cgroup reads a pause and a thaw make (`pause_tier.cap_zswap`,
//! `pause_tier.cgroup_swap_bytes`).

use std::path::{Path, PathBuf};

/// zswap keeps incompressible pages nearly 1:1 in a pool charged to the same
/// cgroup, so a paused cgroup may hold at most this share of its bound there.
pub const ZSWAP_SHARE_OF_BOUND: f64 = 0.25;

/// The process's unified (v2) cgroup directory: exactly one `0::/` line,
/// without `.` or `..` components.
fn unified_cgroup(pid: u32, proc_root: &Path, cgroup_root: &Path) -> Option<PathBuf> {
    let membership = std::fs::read_to_string(proc_root.join(pid.to_string()).join("cgroup")).ok()?;
    let unified: Vec<&str> = membership.lines().filter_map(|line| line.strip_prefix("0::/")).collect();
    let [path] = unified[..] else { return None };
    if path.split('/').any(|part| part == "." || part == "..") {
        return None;
    }
    Some(cgroup_root.join(path))
}

/// `memory.swap.current` of a process's unified cgroup, or `None`.
pub fn cgroup_swap_bytes(pid: u32, proc_root: &Path, cgroup_root: &Path) -> Option<u64> {
    let cgroup = unified_cgroup(pid, proc_root, cgroup_root)?;
    std::fs::read_to_string(cgroup.join("memory.swap.current")).ok()?.trim().parse().ok()
}

/// Bound a process's cgroup's zswap to `ZSWAP_SHARE_OF_BOUND` of its memory
/// bound, once; the bytes set, or `None` (zswap off, no bound, already set,
/// or anything unreadable: without the cap a reclaim only frees less).
pub fn cap_zswap(pid: u32, proc_root: &Path, cgroup_root: &Path, zswap_enabled: &Path) -> Option<u64> {
    let enabled = std::fs::read_to_string(zswap_enabled).ok()?;
    if !matches!(enabled.trim(), "Y" | "1") {
        return None;
    }
    let cgroup = unified_cgroup(pid, proc_root, cgroup_root)?;
    let bound = std::fs::read_to_string(cgroup.join("memory.max")).ok()?;
    let bound = bound.trim();
    if bound == "max" || std::fs::read_to_string(cgroup.join("memory.zswap.max")).ok()?.trim() != "max" {
        return None;
    }
    // Python: int(int(bound) * ZSWAP_SHARE_OF_BOUND), the same float product.
    let cap = (bound.parse::<u64>().ok()? as f64 * ZSWAP_SHARE_OF_BOUND) as u64;
    std::fs::write(cgroup.join("memory.zswap.max"), cap.to_string()).ok()?;
    Some(cap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pause::tests::TempDir;

    const GIB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn zswap_holds_at_most_a_fixed_share_of_a_paused_cgroups_bound() {
        let root = TempDir::new("zswap");
        std::fs::create_dir_all(root.0.join("proc/7")).unwrap();
        std::fs::write(root.0.join("proc/7/cgroup"), "0::/ucloud-sandboxes/abc\n").unwrap();
        let cgroup = root.0.join("cgroup/ucloud-sandboxes/abc");
        std::fs::create_dir_all(&cgroup).unwrap();
        std::fs::write(cgroup.join("memory.max"), format!("{}\n", 2 * GIB)).unwrap();
        std::fs::write(cgroup.join("memory.zswap.max"), "max\n").unwrap();
        let enabled = root.0.join("enabled");
        let cap = || cap_zswap(7, &root.0.join("proc"), &root.0.join("cgroup"), &enabled);

        std::fs::write(&enabled, "N\n").unwrap();
        assert_eq!(cap(), None); // zswap off: swap only, nothing to bound.
        std::fs::write(&enabled, "Y\n").unwrap();
        assert_eq!(cap(), Some(GIB / 2));
        assert_eq!(std::fs::read_to_string(cgroup.join("memory.zswap.max")).unwrap(), (GIB / 2).to_string());
        assert_eq!(cap(), None); // Set once; a later pause leaves it.
        std::fs::write(cgroup.join("memory.zswap.max"), "max\n").unwrap();
        std::fs::write(cgroup.join("memory.max"), "max\n").unwrap();
        assert_eq!(cap(), None); // No bound to take a share of.
        std::fs::write(&enabled, "1\n").unwrap();
        std::fs::write(cgroup.join("memory.max"), "1000\n").unwrap();
        assert_eq!(cap(), Some(250));
    }

    #[test]
    fn swap_is_read_from_the_process_unified_cgroup() {
        let root = TempDir::new("swap");
        std::fs::create_dir_all(root.0.join("proc/7")).unwrap();
        std::fs::create_dir_all(root.0.join("cg/sandboxes/a")).unwrap();
        std::fs::write(root.0.join("cg/sandboxes/a/memory.swap.current"), "123\n").unwrap();
        let swap = |membership: &str| {
            std::fs::write(root.0.join("proc/7/cgroup"), membership).unwrap();
            cgroup_swap_bytes(7, &root.0.join("proc"), &root.0.join("cg"))
        };
        assert_eq!(swap("1:name=systemd:/x\n0::/sandboxes/a\n"), Some(123));
        assert_eq!(swap("0::/sandboxes/../a\n"), None);
        assert_eq!(swap("0::/sandboxes/b\n"), None);
        assert_eq!(swap("0::/sandboxes/a\n0::/sandboxes/a\n"), None);
        assert_eq!(cgroup_swap_bytes(8, &root.0.join("proc"), &root.0.join("cg")), None);
    }
}
