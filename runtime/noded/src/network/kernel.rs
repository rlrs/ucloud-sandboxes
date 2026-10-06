//! The kernel objects of a direct-egress pair: a named network namespace and
//! a veth pair, `us<slot>h` on the host and `eth0` inside, created and
//! configured over rtnetlink with no `ip` processes.
//!
//! Namespaces are named as iproute2 names them, so `ip netns` and the Python
//! agent see the same thing: a bind mount of the creating thread's
//! `ns/net` on `<root>/<name>`, with `<root>` a shared mount point.

use std::ffi::CString;
use std::fs::File;
use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use super::netlink::{self, RouteSocket};
use super::{Lease, MTU, NetworkError, fail};

/// The guest end of every pair.
pub const GUEST_INTERFACE: &str = "eth0";
const PREFIX: u8 = 31;
const MS_BIND: libc::c_ulong = 4096;
const MS_REC: libc::c_ulong = 16384;
const MS_SHARED: libc::c_ulong = 1 << 20;
const MNT_DETACH: libc::c_int = 2;
const CLONE_NEWNET: libc::c_int = 0x4000_0000;

/// What the network manager needs from the kernel. The pool and lease logic
/// sit above it; tests substitute a fake.
pub trait Kernel: Send + Sync {
    /// A new namespace at `lease.namespace_path` (which must not exist) with
    /// the pair, configured. Partial objects are left for the caller's cleanup.
    fn create_pair(&self, lease: &Lease) -> Result<(), NetworkError>;
    /// Configure an existing pair; fails when either end is missing.
    fn configure_pair(&self, lease: &Lease) -> Result<(), NetworkError>;
    /// Delete a host link and so its peer; absent is fine.
    fn delete_link(&self, name: &str) -> Result<(), NetworkError>;
    /// Give the namespace named `source` a second name, `target` (new).
    fn attach_namespace(&self, source: &Path, target: &Path) -> io::Result<()>;
    /// Drop one name; the namespace lives while it has one. Absent is fine.
    fn detach_namespace(&self, path: &Path) -> io::Result<()>;
    /// Whether a host interface exists: sysfs, never if_nametoindex.
    fn interface_present(&self, name: &str) -> bool;
}

/// Python tests presence with sysfs, never if_nametoindex: for a missing name
/// the kernel's SIOCGIFINDEX runs request_module("netdev-<name>") and
/// request_module("<name>") as root, two modprobe execs per check.
pub fn interface_present(name: &str) -> bool {
    !name.contains('/') && std::fs::symlink_metadata(format!("/sys/class/net/{name}")).is_ok()
}

fn c_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))
}

fn syscall_result(result: libc::c_long) -> io::Result<()> {
    if result < 0 { Err(io::Error::last_os_error()) } else { Ok(()) }
}

/// mount(2) through syscall(2): musl's wrapper is not relied upon.
fn mount(source: &CString, target: &CString, flags: libc::c_ulong) -> io::Result<()> {
    let none = c"none";
    // SAFETY: NUL-terminated strings that outlive the call; no data argument.
    syscall_result(unsafe {
        libc::syscall(libc::SYS_mount, source.as_ptr(), target.as_ptr(), none.as_ptr(), flags, std::ptr::null::<libc::c_void>())
    })
}

/// `mount --make-rshared ROOT`, first bind-mounting it on itself if it is
/// not a mount point: iproute2's `netns_add` setup, under the same flock on
/// the directory so concurrent `ip netns add` runs never stack binds. Mounts
/// under it then propagate to the mount namespaces runsc copies, and so do
/// their unmounts, which lets a deleted namespace go.
pub fn make_namespace_root_shared(root: &Path) -> io::Result<()> {
    let directory = File::open(root)?;
    // SAFETY: a valid descriptor for the life of `directory`.
    if unsafe { libc::flock(directory.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let target = c_path(root)?;
    let mut made_mount_point = false;
    loop {
        match mount(&CString::default(), &target, MS_SHARED | MS_REC) {
            Ok(()) => return Ok(()),
            Err(error) if error.raw_os_error() == Some(libc::EINVAL) && !made_mount_point => {
                mount(&target, &target, MS_BIND | MS_REC)?;
                made_mount_point = true;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Run `inside` in a fresh network namespace named `path`, on a thread of
/// its own: `unshare(CLONE_NEWNET)`, then a bind mount of that thread's
/// `/proc/self/task/<tid>/ns/net` on `path`, as `ip netns add` does. Any
/// failure leaves no name behind.
pub fn create_namespace<T: Send>(path: &Path, inside: impl FnOnce() -> io::Result<T> + Send) -> io::Result<T> {
    let target = c_path(path)?;
    std::fs::OpenOptions::new().read(true).write(true).create_new(true).mode(0).open(path)?;
    let result = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                // SAFETY: affects only this thread, which ends with the scope.
                syscall_result(unsafe { libc::syscall(libc::SYS_unshare, CLONE_NEWNET) })?;
                // SAFETY: no arguments.
                let tid = unsafe { libc::syscall(libc::SYS_gettid) };
                let source = c_path(Path::new(&format!("/proc/self/task/{tid}/ns/net")))?;
                mount(&source, &target, MS_BIND)?;
                inside()
            })
            .join()
            .unwrap_or_else(|_| Err(io::Error::other("namespace thread panicked")))
    });
    if result.is_err() {
        let _ = detach_namespace(path);
    }
    result
}

/// Run `inside` on a thread that joined the network namespace named `path`.
pub fn enter_namespace<T: Send>(path: &Path, inside: impl FnOnce() -> io::Result<T> + Send) -> io::Result<T> {
    let namespace = File::open(path)?;
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                // SAFETY: a valid namespace descriptor; affects only this thread.
                syscall_result(unsafe { libc::syscall(libc::SYS_setns, namespace.as_raw_fd(), CLONE_NEWNET) })?;
                inside()
            })
            .join()
            .unwrap_or_else(|_| Err(io::Error::other("namespace thread panicked")))
    })
}

/// Bind `source`'s namespace at `target`, as `ip netns attach` does.
pub fn attach_namespace(source: &Path, target: &Path) -> io::Result<()> {
    std::fs::OpenOptions::new().read(true).write(true).create_new(true).mode(0).open(target)?;
    let result = mount(&c_path(source)?, &c_path(target)?, MS_BIND);
    if result.is_err() {
        let _ = std::fs::remove_file(target);
    }
    result
}

/// Drop one name, as `ip netns delete`: a lazy unmount, then the file.
pub fn detach_namespace(path: &Path) -> io::Result<()> {
    let target = c_path(path)?;
    // SAFETY: a NUL-terminated path that outlives the call.
    if let Err(error) = syscall_result(unsafe { libc::syscall(libc::SYS_umount2, target.as_ptr(), MNT_DETACH) }) {
        // Not mounted, or gone.
        if !matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOENT)) {
            return Err(error);
        }
    }
    match std::fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

/// `sysctl -w net.ipv6.conf.<device>.disable_ipv6=1` for the calling
/// thread's network namespace. Without IPv6 in the kernel there is no file,
/// and nothing to turn off.
pub fn disable_ipv6(device: &str) -> io::Result<()> {
    let path = format!("/proc/sys/net/ipv6/conf/{device}/disable_ipv6");
    match std::fs::OpenOptions::new().write(true).open(&path) {
        Ok(mut file) => file.write_all(b"1\n"),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// The production kernel: netlink, namespaces and mounts.
pub struct NetlinkKernel {
    namespace_root: PathBuf,
    /// Whether this process has made the namespace root a shared mount point.
    root_shared: Mutex<bool>,
    /// Pairs are created one at a time. Concurrent namespace and veth
    /// creation contends in the kernel worse than it queues: 32 cold creates
    /// at once took p50 80-100 ms each, against 28 ms one at a time (1.3 ms
    /// per pair alone).
    create_turn: Mutex<()>,
}

impl NetlinkKernel {
    pub fn new(namespace_root: PathBuf) -> NetlinkKernel {
        NetlinkKernel { namespace_root, root_shared: Mutex::new(false), create_turn: Mutex::new(()) }
    }

    fn prepare_root(&self) -> io::Result<()> {
        let mut shared = self.root_shared.lock().expect("not poisoned");
        if !*shared {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new().recursive(true).mode(0o755).create(&self.namespace_root)?;
            make_namespace_root_shared(&self.namespace_root)?;
            *shared = true;
        }
        Ok(())
    }

    /// Python's `ip -batch` lines, in order: the host end (IPv6 off before it
    /// comes up), then lo, eth0 and the default route inside.
    fn configure(lease: &Lease, guest: &mut RouteSocket) -> Result<(), NetworkError> {
        let step = |what: &str, name: &str| {
            let what = what.to_string();
            let name = name.to_string();
            move |error: io::Error| fail(format!("direct network {what} {name} failed: {error}"))
        };
        let host_name = lease.host_interface.as_str();
        disable_ipv6(host_name).map_err(step("disabling IPv6 on", host_name))?;
        let mut host = RouteSocket::open().map_err(step("netlink socket for", host_name))?;
        let host_index = host.link_index(host_name).map_err(step("finding", host_name))?;
        host.acknowledged(netlink::set_link_up(host_index, Some(MTU))).map_err(step("setting up", host_name))?;
        host.acknowledged(netlink::replace_address(host_index, lease.host_ip, PREFIX))
            .map_err(step("addressing", host_name))?;
        guest.acknowledged(netlink::set_named_link_up("lo")).map_err(step("setting up lo in", &lease.namespace))?;
        let guest_index = guest.link_index(GUEST_INTERFACE).map_err(step("finding eth0 in", &lease.namespace))?;
        guest.acknowledged(netlink::set_link_up(guest_index, Some(MTU))).map_err(step("setting up eth0 in", &lease.namespace))?;
        guest.acknowledged(netlink::replace_address(guest_index, lease.guest_ip, PREFIX))
            .map_err(step("addressing eth0 in", &lease.namespace))?;
        guest.acknowledged(netlink::replace_default_route(guest_index, lease.host_ip))
            .map_err(step("routing eth0 in", &lease.namespace))?;
        Ok(())
    }
}

impl Kernel for NetlinkKernel {
    fn create_pair(&self, lease: &Lease) -> Result<(), NetworkError> {
        let _turn = self.create_turn.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        self.prepare_root().map_err(|error| fail(format!("direct network namespace root: {error}")))?;
        // Interfaces registered in the new namespace inherit its default, so
        // eth0 never has IPv6; lo, already there, keeps ::1.
        let mut guest = create_namespace(&lease.namespace_path, || {
            disable_ipv6("default")?;
            RouteSocket::open()
        })
        .map_err(|error| fail(format!("direct network namespace {} failed: {error}", lease.namespace)))?;
        let namespace = File::open(&lease.namespace_path)?;
        RouteSocket::open()
            .and_then(|mut host| {
                host.acknowledged(netlink::new_veth(&lease.host_interface, GUEST_INTERFACE, MTU, namespace.as_raw_fd()))
            })
            .map_err(|error| fail(format!("direct network veth {} failed: {error}", lease.host_interface)))?;
        Self::configure(lease, &mut guest)
    }

    fn configure_pair(&self, lease: &Lease) -> Result<(), NetworkError> {
        // A pair from an older release may still have IPv6 on eth0.
        let mut guest = enter_namespace(&lease.namespace_path, || {
            disable_ipv6(GUEST_INTERFACE)?;
            RouteSocket::open()
        })
        .map_err(|error| fail(format!("direct network namespace {} is unusable: {error}", lease.namespace)))?;
        Self::configure(lease, &mut guest)
    }

    fn delete_link(&self, name: &str) -> Result<(), NetworkError> {
        let result = RouteSocket::open().and_then(|mut socket| socket.acknowledged(netlink::delete_link(name)));
        match result {
            Err(error) if error.raw_os_error() != Some(libc::ENODEV) => {
                Err(fail(format!("direct network link delete {name} failed: {error}")))
            }
            _ => Ok(()),
        }
    }

    fn attach_namespace(&self, source: &Path, target: &Path) -> io::Result<()> {
        attach_namespace(source, target)
    }

    fn detach_namespace(&self, path: &Path) -> io::Result<()> {
        detach_namespace(path)
    }

    fn interface_present(&self, name: &str) -> bool {
        interface_present(name)
    }
}

/// Root-only: real namespaces and veth pairs on this host, at slots far above
/// what a developer box uses, removed again. Skipped when not root.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::NetworkManager;
    use std::process::Command;

    fn root() -> bool {
        // SAFETY: no preconditions.
        let root = unsafe { libc::geteuid() } == 0;
        if !root {
            eprintln!("skipped: needs root");
        }
        root
    }

    fn ip(args: &[&str]) -> String {
        let output = Command::new("ip").args(args).output().expect("ip runs");
        assert!(output.status.success(), "ip {args:?}: {}", String::from_utf8_lossy(&output.stderr));
        String::from_utf8(output.stdout).unwrap()
    }

    fn sysctl_in(namespace: Option<&str>, device: &str) -> String {
        let path = format!("/proc/sys/net/ipv6/conf/{device}/disable_ipv6");
        let mut argv = vec!["cat", path.as_str()];
        if let Some(namespace) = namespace {
            argv.splice(0..0, ["ip", "netns", "exec", namespace]);
        }
        let output = Command::new(argv[0]).args(&argv[1..]).output().unwrap();
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    struct Pair(Lease, NetlinkKernel);

    impl Drop for Pair {
        fn drop(&mut self) {
            let _ = self.1.delete_link(&self.0.host_interface);
            let _ = detach_namespace(&self.0.namespace_path);
        }
    }

    fn lease(sandbox: &str, slot: u32) -> Lease {
        let manager = NetworkManager::new(PathBuf::from("/nonexistent/slots.json"), PathBuf::from("/run/netns"), vec![], false);
        manager.lease(&format!("noded-test-{}-{sandbox}", std::process::id()), 1, slot).unwrap()
    }

    #[test]
    fn creates_configures_and_removes_a_real_pair() {
        if !root() {
            return;
        }
        let pair = Pair(lease("create", 32741), NetlinkKernel::new(PathBuf::from("/run/netns")));
        let (lease, kernel) = (&pair.0, &pair.1);
        kernel.create_pair(lease).unwrap();
        assert!(kernel.interface_present(&lease.host_interface));
        // iproute2 sees the namespace by name, and both ends as configured.
        assert!(ip(&["netns", "list"]).contains(&lease.namespace));
        let host = ip(&["-d", "-o", "link", "show", "dev", &lease.host_interface]);
        assert!(host.contains("mtu 1420") && host.contains("veth") && host.contains(",UP"), "{host}");
        assert!(host.contains("numtxqueues 1 numrxqueues 1"), "{host}");
        let host_address = ip(&["-o", "address", "show", "dev", &lease.host_interface]);
        assert!(host_address.contains(&format!("inet {}/31", lease.host_ip)), "{host_address}");
        assert!(!host_address.contains("inet6"), "{host_address}");
        let guest = ip(&["-n", &lease.namespace, "-d", "-o", "link", "show", "dev", "eth0"]);
        assert!(guest.contains("mtu 1420") && guest.contains(",UP") && guest.contains("numtxqueues 1 numrxqueues 1"), "{guest}");
        let guest_address = ip(&["-n", &lease.namespace, "-o", "address", "show"]);
        assert!(guest_address.contains(&format!("inet {}/31", lease.guest_ip)), "{guest_address}");
        assert!(guest_address.contains("inet 127.0.0.1/8") && guest_address.contains("inet6 ::1/128"), "{guest_address}");
        assert!(!guest_address.contains("inet6 fe80"), "{guest_address}");
        let route = ip(&["-n", &lease.namespace, "route", "show", "default"]);
        assert_eq!(route.trim(), format!("default via {} dev eth0", lease.host_ip)); // proto boot is not shown
        assert_eq!((sysctl_in(None, &lease.host_interface).as_str(), sysctl_in(Some(&lease.namespace), "eth0").as_str()), ("1", "1"));
        assert_eq!(sysctl_in(Some(&lease.namespace), "lo"), "0");
        // The pair carries traffic.
        let ping = Command::new("ip")
            .args(["netns", "exec", &lease.namespace, "ping", "-c1", "-W2", &lease.host_ip.to_string()])
            .output()
            .unwrap();
        assert!(ping.status.success(), "{}", String::from_utf8_lossy(&ping.stdout));

        // Reconfiguring is idempotent, and fails once the guest end is gone.
        kernel.configure_pair(lease).unwrap();
        ip(&["-n", &lease.namespace, "link", "delete", "eth0"]);
        assert!(kernel.configure_pair(lease).is_err());
        assert!(!kernel.interface_present(&lease.host_interface));
        kernel.delete_link(&lease.host_interface).unwrap(); // absent is fine
        kernel.detach_namespace(&lease.namespace_path).unwrap();
        assert!(!lease.namespace_path.exists());
        assert!(!ip(&["netns", "list"]).contains(&lease.namespace));
        kernel.detach_namespace(&lease.namespace_path).unwrap(); // absent is fine
    }

    #[test]
    fn reconfigures_a_pair_iproute2_made_and_turns_its_ipv6_off() {
        if !root() {
            return;
        }
        let pair = Pair(lease("iproute2", 32743), NetlinkKernel::new(PathBuf::from("/run/netns")));
        let (lease, kernel) = (&pair.0, &pair.1);
        let name = lease.namespace.as_str();
        ip(&["netns", "add", name]);
        ip(&["link", "add", &lease.host_interface, "type", "veth", "peer", "name", "eth0", "netns", name]);
        ip(&["link", "set", "dev", &lease.host_interface, "up"]);
        ip(&["-n", name, "link", "set", "dev", "eth0", "up"]);
        kernel.configure_pair(lease).unwrap();
        assert_eq!((sysctl_in(None, &lease.host_interface).as_str(), sysctl_in(Some(name), "eth0").as_str()), ("1", "1"));
        let guest_address = ip(&["-n", name, "-o", "address", "show", "dev", "eth0"]);
        assert!(guest_address.contains(&format!("inet {}/31", lease.guest_ip)) && !guest_address.contains("inet6"), "{guest_address}");
    }

    #[test]
    fn a_namespace_is_attached_under_a_second_name_and_detached() {
        if !root() {
            return;
        }
        let pair = Pair(lease("attach", 32745), NetlinkKernel::new(PathBuf::from("/run/netns")));
        let (lease, kernel) = (&pair.0, &pair.1);
        kernel.create_pair(lease).unwrap();
        let manager = NetworkManager::new(PathBuf::from("/nonexistent/slots.json"), PathBuf::from("/run/netns"), vec![], false);
        let second = Pair(manager.pool_lease(32745), NetlinkKernel::new(PathBuf::from("/run/netns")));
        let _ = detach_namespace(&second.0.namespace_path);
        kernel.attach_namespace(&lease.namespace_path, &second.0.namespace_path).unwrap();
        assert!(kernel.attach_namespace(&lease.namespace_path, &second.0.namespace_path).is_err());
        let same = |a: &Path, b: &Path| {
            use std::os::unix::fs::MetadataExt;
            let (a, b) = (std::fs::metadata(a).unwrap(), std::fs::metadata(b).unwrap());
            (a.dev(), a.ino()) == (b.dev(), b.ino())
        };
        assert!(same(&lease.namespace_path, &second.0.namespace_path));
        kernel.detach_namespace(&lease.namespace_path).unwrap();
        // The pair lives on under the other name.
        let guest = ip(&["-n", &second.0.namespace, "-o", "address", "show", "dev", "eth0"]);
        assert!(guest.contains(&format!("inet {}/31", lease.guest_ip)), "{guest}");
        assert!(kernel.interface_present(&lease.host_interface));
    }
}
