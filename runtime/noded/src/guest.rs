//! Guest files written into a prepared (merged overlay) rootfs before runsc
//! starts, as ucloud_sandboxes/direct_oci.py does them: S9 (`prepare_workspace`,
//! `prepare_working_directory`, `prepare_network_files`) and S10
//! (`install_init`, `install_managed_init`) of the create-pipeline spec.
//!
//! Every walk below the rootfs uses `openat` with `O_NOFOLLOW`: the image
//! controls those names and must never redirect a host write.

use std::collections::BTreeMap;
use std::ffi::CString;
use std::fs::File;
use std::io::{self, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

use crate::oci::{MANAGED_STATE_NAME, OciError, SandboxSpec, parse_ipv4, py_repr, validate_guest_path, validate_init_binary, validate_init_metadata};

pub const INIT_NAME: &str = ".ucloud-init";
pub const MANAGED_INIT_NAME: &str = ".ucloud-job-init";

fn c_string(bytes: &[u8]) -> io::Result<CString> {
    CString::new(bytes).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))
}

fn check(result: libc::c_int) -> io::Result<libc::c_int> {
    if result < 0 { Err(io::Error::last_os_error()) } else { Ok(result) }
}

/// `os.open(path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW)`.
pub(crate) fn open_directory(path: &Path) -> io::Result<OwnedFd> {
    let path = c_string(path.as_os_str().as_bytes())?;
    // SAFETY: a valid C string; the descriptor is owned by the result.
    let fd = check(unsafe { libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC) })?;
    // SAFETY: a fresh descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// `os.open(name, flags, mode, dir_fd=directory)`, close-on-exec as in Python.
pub(crate) fn open_at(directory: &OwnedFd, name: &str, flags: libc::c_int, mode: libc::mode_t) -> io::Result<OwnedFd> {
    let name = c_string(name.as_bytes())?;
    // SAFETY: a valid directory descriptor and C string.
    let fd = check(unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags | libc::O_CLOEXEC, mode as libc::c_uint) })?;
    // SAFETY: a fresh descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn mkdir_at(directory: &OwnedFd, name: &str, mode: libc::mode_t) -> io::Result<()> {
    let name = c_string(name.as_bytes())?;
    // SAFETY: as above.
    check(unsafe { libc::mkdirat(directory.as_raw_fd(), name.as_ptr(), mode) }).map(drop)
}

fn rename_at(directory: &OwnedFd, from: &str, to: &str) -> io::Result<()> {
    let (from, to) = (c_string(from.as_bytes())?, c_string(to.as_bytes())?);
    // SAFETY: as above.
    check(unsafe { libc::renameat(directory.as_raw_fd(), from.as_ptr(), directory.as_raw_fd(), to.as_ptr()) }).map(drop)
}

fn unlink_at(directory: &OwnedFd, name: &str) -> io::Result<()> {
    let name = c_string(name.as_bytes())?;
    // SAFETY: as above.
    check(unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) }).map(drop)
}

fn fsync(fd: &OwnedFd) -> io::Result<()> {
    // SAFETY: a valid descriptor.
    check(unsafe { libc::fsync(fd.as_raw_fd()) }).map(drop)
}

fn fchmod(fd: &OwnedFd, mode: libc::mode_t) -> io::Result<()> {
    // SAFETY: a valid descriptor.
    check(unsafe { libc::fchmod(fd.as_raw_fd(), mode) }).map(drop)
}

/// `os.urandom(n)`.
pub(crate) fn random_bytes<const N: usize>() -> io::Result<[u8; N]> {
    let mut bytes = [0u8; N];
    let mut filled = 0;
    while filled < N {
        // The raw syscall: a static musl link resolved libc::getrandom, a weak
        // import, to address 0 (the 0.9.44 canary's SIGSEGV).
        // SAFETY: the buffer is valid for N - filled bytes.
        let read = unsafe { libc::syscall(libc::SYS_getrandom, bytes[filled..].as_mut_ptr(), N - filled, 0) };
        if read < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        filled += read as usize;
    }
    Ok(bytes)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `rootfs.is_absolute() and rootfs.is_dir() and not rootfs.is_symlink()`.
fn is_rootfs_directory(rootfs: &Path) -> bool {
    rootfs.is_absolute()
        && rootfs.is_dir()
        && !std::fs::symlink_metadata(rootfs).map(|meta| meta.file_type().is_symlink()).unwrap_or(false)
}

fn failed(message: &str) -> OciError {
    OciError::Config(message.to_string())
}

/// `_prepare_directory`: walk `directory` below `rootfs` one component at a
/// time without following symlinks, creating missing components (mode 0777
/// before umask). A newly created final workspace becomes 01777. The final
/// directory is fsynced.
fn prepare_directory(rootfs: &Path, directory: &str, workspace: bool) -> Result<(), OciError> {
    if !is_rootfs_directory(rootfs) {
        return Err(failed("direct-runtime workspace target must be an absolute rootfs directory"));
    }
    let components: Vec<&str> = directory.split('/').filter(|component| !component.is_empty() && *component != ".").collect();
    // An absolute component would make openat ignore its directory.
    if components.is_empty() && !workspace {
        return Ok(());
    }
    if components.is_empty() || components.contains(&"..") {
        return Err(failed("direct-runtime workspace must name a directory below rootfs"));
    }
    let walk = || -> io::Result<()> {
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW;
        let mut current = open_directory(rootfs)?;
        let mut created = false;
        for component in &components {
            created = false;
            let child = match open_at(&current, component, flags, 0) {
                Ok(child) => child,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    mkdir_at(&current, component, 0o777)?;
                    created = true;
                    open_at(&current, component, flags, 0)?
                }
                Err(error) => return Err(error),
            };
            current = child;
        }
        // Preserve image ownership and modes; only a new workspace is shared,
        // with the sticky bit protecting entries from other guest users.
        if created && workspace {
            fchmod(&current, 0o1777)?;
        }
        fsync(&current)
    };
    walk().map_err(|_| failed("failed to prepare direct-runtime sandbox workspace"))
}

/// `prepare_workspace`: the SDK's workspace inside the quota-owned overlay.
pub fn prepare_workspace(rootfs: &Path, spec: &SandboxSpec) -> Result<(), OciError> {
    spec.filesystem.validate()?;
    prepare_directory(rootfs, &spec.filesystem.workspace_path, true)
}

/// `prepare_working_directory`: create a missing OCI cwd; never rewrite
/// existing image permissions.
pub fn prepare_working_directory(rootfs: &Path, directory: &str) -> Result<(), OciError> {
    validate_guest_path("working_dir", directory)?;
    if directory == "/" {
        return Ok(());
    }
    prepare_directory(rootfs, directory, false)
}

/// The direct-egress `/etc/resolv.conf`.
pub fn direct_resolv_conf(spec: &SandboxSpec) -> Result<String, OciError> {
    let servers: Vec<&str> = if spec.dns_servers.is_empty() {
        crate::oci::DEFAULT_DNS_SERVERS.to_vec()
    } else {
        spec.dns_servers.iter().map(String::as_str).collect()
    };
    let mut content = String::new();
    for server in servers {
        content.push_str(&format!("nameserver {}\n", parse_ipv4(server)?));
    }
    content.push_str("options timeout:2 attempts:2\n");
    Ok(content)
}

/// The direct-egress `/etc/hosts` naming loopback and `relay_hosts` (name ->
/// virtual address), as Python writes it; `None` without relay names.
pub fn direct_hosts(relay_hosts: &BTreeMap<String, String>) -> Result<Option<String>, OciError> {
    if relay_hosts.is_empty() {
        return Ok(None);
    }
    let mut content = String::from("127.0.0.1 localhost\n::1 localhost\n");
    for (host, address) in relay_hosts {
        let valid = !host.is_empty() && host.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-');
        if !valid {
            return Err(OciError::Config(format!("relay host {} is not a DNS name", py_repr(host))));
        }
        content.push_str(&format!("{} {host}\n", parse_ipv4(address)?));
    }
    Ok(Some(content))
}

/// `prepare_network_files` for direct egress: `/etc/resolv.conf`, and with
/// relay names `/etc/hosts` (as Docker replaces it), replaced by `openat` +
/// `renameat`; without relay names the image's `/etc/hosts` stays. Nothing for
/// `network=none`. Relay egress (its `/etc/hosts` names only its own relay)
/// stays in Python and is `OciError::Unsupported`.
pub fn prepare_network_files(rootfs: &Path, spec: &SandboxSpec, relay_hosts: &BTreeMap<String, String>) -> Result<(), OciError> {
    if spec.network == "none" {
        return Ok(());
    }
    if spec.network_policy.egress == "relay" {
        return Err(OciError::Unsupported("relay egress network files are prepared by the Python agent".into()));
    }
    let mut contents = vec![("resolv.conf", direct_resolv_conf(spec)?)];
    if let Some(hosts) = direct_hosts(relay_hosts)? {
        contents.push(("hosts", hosts));
    }
    if !is_rootfs_directory(rootfs) {
        return Err(failed("direct-runtime network rootfs must be an absolute directory"));
    }
    let write = || -> io::Result<()> {
        let root = open_directory(rootfs)?;
        match mkdir_at(&root, "etc", 0o755) {
            Err(error) if error.kind() != io::ErrorKind::AlreadyExists => return Err(error),
            _ => {}
        }
        let etc = open_at(&root, "etc", libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW, 0)?;
        for (name, content) in &contents {
            let temporary = format!(".ucloud-network-{}", hex(&random_bytes::<12>()?));
            let fd = open_at(&etc, &temporary, libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW, 0o644)?;
            let result = (|| {
                let mut file = File::from(fd);
                file.write_all(content.as_bytes())?;
                file.sync_all()?;
                drop(file);
                rename_at(&etc, &temporary, name)
            })();
            match unlink_at(&etc, &temporary) {
                Err(error) if error.kind() != io::ErrorKind::NotFound && result.is_ok() => return Err(error),
                _ => {}
            }
            result?;
        }
        fsync(&etc)
    };
    write().map_err(|_| failed("failed to prepare direct-runtime network configuration"))
}

/// `install_init`: the trusted init at `/.ucloud-init` when `enabled`
/// (`security.init and not managed_process`).
pub fn install_init(rootfs: &Path, init_binary: Option<&Path>, enabled: bool) -> Result<(), OciError> {
    if !enabled {
        return Ok(());
    }
    let binary = init_binary.ok_or_else(|| failed("security.init requires a configured direct-runtime init binary"))?;
    install_binary(rootfs, binary, INIT_NAME)
}

/// `install_managed_init`: the checkpoint-owned supervisor at
/// `/.ucloud-job-init` when `enabled` (`managed_process` or the static helper).
pub fn install_managed_init(rootfs: &Path, managed_init_binary: Option<&Path>, enabled: bool) -> Result<(), OciError> {
    if !enabled {
        return Ok(());
    }
    let binary =
        managed_init_binary.ok_or_else(|| failed("managed_process requires a configured managed-process init binary"))?;
    install_binary(rootfs, binary, MANAGED_INIT_NAME)
}

/// The managed supervisor's state directory, the source of its bind mount
/// (`oci::managed_state_mount`), which must exist before `runsc create`: a
/// real directory below the rootfs (never a symlink the image placed),
/// owned by the daemon (root) with mode 0700. A replay re-secures an existing
/// one. Only `managed_process` sandboxes get the mount, so only they get this.
pub fn prepare_managed_state(rootfs: &Path, enabled: bool) -> Result<(), OciError> {
    if !enabled {
        return Ok(());
    }
    if !is_rootfs_directory(rootfs) {
        return Err(failed("managed process state target must be an absolute rootfs directory"));
    }
    let prepare = || -> io::Result<()> {
        let root = open_directory(rootfs)?;
        match mkdir_at(&root, MANAGED_STATE_NAME, 0o700) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        let directory = open_at(&root, MANAGED_STATE_NAME, libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW, 0)?;
        // SAFETY: a valid descriptor; our own ids.
        check(unsafe { libc::fchown(directory.as_raw_fd(), libc::geteuid(), libc::getegid()) })?;
        fchmod(&directory, 0o700)?;
        fsync(&directory)?;
        fsync(&root)
    };
    prepare().map_err(|_| failed("failed to prepare the managed process state directory"))
}

/// `tempfile.mkstemp(prefix=prefix, dir=directory)`: O_EXCL, mode 0600.
fn mkstemp(directory: &Path, prefix: &str) -> io::Result<(File, std::path::PathBuf)> {
    const LETTERS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789_";
    loop {
        let random = random_bytes::<8>()?;
        let suffix: String = random.iter().map(|b| LETTERS[*b as usize % LETTERS.len()] as char).collect();
        let path = directory.join(format!("{prefix}{suffix}"));
        let opened = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .mode(0o600)
            .open(&path);
        match opened {
            Ok(file) => return Ok((file, path)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
}

/// `_install_binary`: validate the trusted source, copy it to a private temp
/// file in the rootfs, chmod 0755, fsync, rename onto `target_name`, fsync the
/// rootfs directory. A bind mount is not executable under gVisor.
fn install_binary(rootfs: &Path, binary: &Path, target_name: &str) -> Result<(), OciError> {
    if !is_rootfs_directory(rootfs) {
        return Err(failed("direct-runtime init target must be an absolute rootfs directory"));
    }
    validate_init_binary(binary)?;
    let install_failed = || failed("failed to install direct-runtime init into sandbox rootfs");
    let mut source = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(binary)
        .map_err(|_| install_failed())?;
    validate_init_metadata(&source.metadata().map_err(|_| install_failed())?)?;
    let (mut target, temporary) = mkstemp(rootfs, &format!(".{target_name}.")).map_err(|_| install_failed())?;
    let result = (|| -> io::Result<()> {
        io::copy(&mut source, &mut target)?;
        target.flush()?;
        target.set_permissions(std::fs::Permissions::from_mode(0o755))?;
        target.sync_all()?;
        drop(target);
        std::fs::rename(&temporary, rootfs.join(target_name))?;
        File::open(rootfs)?.sync_all()
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result.map_err(|_| install_failed())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;
    use std::path::PathBuf;

    use serde_json::json;

    fn scratch(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("noded-guest-{name}-{}-{}", std::process::id(), hex(&random_bytes::<4>().unwrap())));
        std::fs::create_dir_all(root.join("rootfs")).unwrap();
        root
    }

    fn spec(extra: serde_json::Value) -> SandboxSpec {
        let mut raw = json!({"id": "s1", "image": "registry.example/x:1", "memory_mb": 512, "disk_mb": 512});
        raw.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        SandboxSpec::from_value(&raw).unwrap()
    }

    /// A root-owned, executable host binary nobody else may write (not a symlink).
    pub(crate) fn trusted_binary() -> PathBuf {
        ["/bin/true", "/usr/bin/true", "/bin/sh", "/usr/bin/env"]
            .iter()
            .map(PathBuf::from)
            .find(|path| validate_init_binary(path).is_ok())
            .expect("a root-owned host binary for the init fixtures")
    }

    fn mode(path: &Path) -> u32 {
        std::fs::symlink_metadata(path).unwrap().mode() & 0o7777
    }

    #[test]
    fn workspace_is_created_shared_and_existing_modes_are_kept() {
        let root = scratch("workspace");
        let rootfs = root.join("rootfs");
        prepare_workspace(&rootfs, &spec(json!({"filesystem": {"workspace_path": "/srv/work"}}))).unwrap();
        assert!(rootfs.join("srv/work").is_dir());
        assert_eq!(mode(&rootfs.join("srv/work")), 0o1777);
        // Intermediate components keep their mkdir mode; only the last is shared.
        assert_eq!(mode(&rootfs.join("srv")) & 0o1000, 0);
        // An existing workspace keeps the image's mode.
        std::fs::create_dir(rootfs.join("workspace")).unwrap();
        std::fs::set_permissions(rootfs.join("workspace"), std::fs::Permissions::from_mode(0o750)).unwrap();
        prepare_workspace(&rootfs, &spec(json!({}))).unwrap();
        assert_eq!(mode(&rootfs.join("workspace")), 0o750);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn walks_never_follow_image_symlinks() {
        let root = scratch("symlink");
        let rootfs = root.join("rootfs");
        let outside = root.join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, rootfs.join("workspace")).unwrap();
        let error = prepare_workspace(&rootfs, &spec(json!({}))).unwrap_err();
        assert_eq!(error.to_string(), "failed to prepare direct-runtime sandbox workspace");
        std::os::unix::fs::symlink(&outside, rootfs.join("srv")).unwrap();
        assert!(prepare_working_directory(&rootfs, "/srv/app").is_err());
        assert!(!outside.join("app").exists());
        std::os::unix::fs::symlink(&outside, rootfs.join("etc")).unwrap();
        assert!(prepare_network_files(&rootfs, &spec(json!({})), &BTreeMap::new()).is_err());
        assert!(!outside.join("resolv.conf").exists());
        // The rootfs itself must be a real directory.
        let link = root.join("link");
        std::os::unix::fs::symlink(&rootfs, &link).unwrap();
        assert!(prepare_working_directory(&link, "/x").is_err());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn working_directory_is_created_without_chmod_and_root_is_skipped() {
        let root = scratch("cwd");
        let rootfs = root.join("rootfs");
        prepare_working_directory(&rootfs, "/").unwrap();
        prepare_working_directory(&rootfs, "/srv/./app/").unwrap();
        assert!(rootfs.join("srv/app").is_dir());
        assert_eq!(mode(&rootfs.join("srv/app")) & 0o1000, 0);
        assert!(prepare_working_directory(&rootfs, "relative").is_err());
        assert!(prepare_working_directory(&rootfs, "/a/../b").is_err());
        // "//" names no component below the rootfs and is a no-op.
        prepare_working_directory(&rootfs, "//").unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn direct_network_files_replace_resolv_conf_only() {
        let root = scratch("network");
        let rootfs = root.join("rootfs");
        let none = BTreeMap::new();
        prepare_network_files(&rootfs, &spec(json!({})), &none).unwrap();
        assert_eq!(
            std::fs::read_to_string(rootfs.join("etc/resolv.conf")).unwrap(),
            "nameserver 1.1.1.1\nnameserver 8.8.8.8\noptions timeout:2 attempts:2\n"
        );
        assert!(!rootfs.join("etc/hosts").exists());
        std::fs::write(rootfs.join("etc/hosts"), "image hosts\n").unwrap();
        prepare_network_files(&rootfs, &spec(json!({"dns_servers": ["9.9.9.9"]})), &none).unwrap();
        assert_eq!(
            std::fs::read_to_string(rootfs.join("etc/resolv.conf")).unwrap(),
            "nameserver 9.9.9.9\noptions timeout:2 attempts:2\n"
        );
        assert_eq!(std::fs::read_to_string(rootfs.join("etc/hosts")).unwrap(), "image hosts\n");
        let names: Vec<_> = std::fs::read_dir(rootfs.join("etc")).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(names.len(), 2, "no temporary files remain: {names:?}");
        // network=none writes nothing; relay egress belongs to Python.
        let other = scratch("network-none");
        prepare_network_files(&other.join("rootfs"), &spec(json!({"network": "none"})), &none).unwrap();
        assert!(!other.join("rootfs/etc").exists());
        let relay = spec(json!({"network_policy": {"egress": "relay", "relay": "default"}}));
        assert!(matches!(prepare_network_files(&other.join("rootfs"), &relay, &none), Err(OciError::Unsupported(_))));
        std::fs::remove_dir_all(&root).unwrap();
        std::fs::remove_dir_all(&other).unwrap();
    }

    #[test]
    fn direct_network_files_name_relays_as_python_does() {
        let root = scratch("network-hosts");
        let rootfs = root.join("rootfs");
        std::fs::create_dir_all(rootfs.join("etc")).unwrap();
        std::fs::write(rootfs.join("etc/hosts"), "10.0.0.9 baked\n").unwrap();
        let names = BTreeMap::from([
            ("sandboxes-store-1".to_string(), "198.18.95.3".to_string()),
            ("sandbox-gateway-gateway-1".to_string(), "198.19.38.106".to_string()),
        ]);
        prepare_network_files(&rootfs, &spec(json!({})), &names).unwrap();
        // Sorted by name, like Python's sorted(relay_hosts.items()).
        assert_eq!(
            std::fs::read_to_string(rootfs.join("etc/hosts")).unwrap(),
            "127.0.0.1 localhost\n::1 localhost\n198.19.38.106 sandbox-gateway-gateway-1\n198.18.95.3 sandboxes-store-1\n"
        );
        assert!(std::fs::read_to_string(rootfs.join("etc/resolv.conf")).unwrap().starts_with("nameserver 1.1.1.1\n"));
        for (host, address) in [("bad host", "198.18.0.1"), ("name\n", "198.18.0.1"), ("name", "not-an-ip")] {
            let bad = BTreeMap::from([(host.to_string(), address.to_string())]);
            assert!(prepare_network_files(&rootfs, &spec(json!({})), &bad).is_err());
        }
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn the_managed_state_directory_is_private_and_never_a_symlink() {
        let root = scratch("managed-state");
        let rootfs = root.join("rootfs");
        prepare_managed_state(&rootfs, false).unwrap();
        assert!(!rootfs.join(MANAGED_STATE_NAME).exists());
        prepare_managed_state(&rootfs, true).unwrap();
        let state = rootfs.join(MANAGED_STATE_NAME);
        assert_eq!(mode(&state), 0o700);
        assert_eq!(std::fs::metadata(&state).unwrap().uid(), crate::fsutil::euid());
        // A replay re-secures an existing directory.
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o777)).unwrap();
        prepare_managed_state(&rootfs, true).unwrap();
        assert_eq!(mode(&state), 0o700);
        // An image's symlink there is refused, and its target untouched.
        std::fs::remove_dir(&state).unwrap();
        let outside = root.join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::os::unix::fs::symlink(&outside, &state).unwrap();
        assert!(prepare_managed_state(&rootfs, true).is_err());
        assert_eq!(mode(&outside), 0o755);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn init_is_copied_atomically_from_a_trusted_root_owned_binary() {
        let root = scratch("init");
        let rootfs = root.join("rootfs");
        let trusted = trusted_binary();
        let trusted = trusted.as_path();
        install_init(&rootfs, None, false).unwrap();
        assert!(!rootfs.join(INIT_NAME).exists());
        assert!(install_init(&rootfs, None, true).is_err());
        install_init(&rootfs, Some(trusted), true).unwrap();
        install_managed_init(&rootfs, Some(trusted), true).unwrap();
        for name in [INIT_NAME, MANAGED_INIT_NAME] {
            assert_eq!(mode(&rootfs.join(name)), 0o755);
            assert_eq!(std::fs::read(rootfs.join(name)).unwrap(), std::fs::read(trusted).unwrap());
        }
        // A replay replaces the file; no temporary survives.
        install_init(&rootfs, Some(trusted), true).unwrap();
        let names: Vec<_> = std::fs::read_dir(&rootfs).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(names.len(), 2, "{names:?}");
        // An untrusted (non-root-owned) source and a symlink are refused.
        let untrusted = root.join("init");
        std::fs::write(&untrusted, b"#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&untrusted, std::fs::Permissions::from_mode(0o755)).unwrap();
        let error = install_init(&rootfs, Some(&untrusted), true).unwrap_err();
        assert_eq!(error.to_string(), "direct-runtime init binary must be root-owned, executable, and immutable");
        let link = root.join("link");
        std::os::unix::fs::symlink(trusted, &link).unwrap();
        assert!(install_init(&rootfs, Some(&link), true).is_err());
        assert!(install_init(&rootfs, Some(&root.join("missing")), true).is_err());
        std::fs::remove_dir_all(&root).unwrap();
    }
}
