use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use super::*;
use crate::fsutil::FileLock;

fn manager(root: &Path) -> NetworkManager {
    NetworkManager::new(root.join("network-slots.json"), root.join("netns"), vec![], true)
}

fn temp_root(name: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!("noded-net-{name}-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

#[test]
fn names_and_addresses_match_python() {
    let m = manager(Path::new("/tmp/x"));
    let lease = m.lease("sb-1", 1, 1).unwrap();
    assert_eq!((lease.host_ip.to_string(), lease.guest_ip.to_string()), ("100.96.0.2".into(), "100.96.0.3".into()));
    assert_eq!(lease.namespace, "ucloud-3019d619ced61cc29ced");
    assert_eq!(lease.host_interface, "us1h");
    let last = m.lease("sb", 1, MAX_SLOTS).unwrap();
    assert_eq!((last.host_ip.to_string(), last.guest_ip.to_string()), ("100.96.255.254".into(), "100.96.255.255".into()));
    assert!(m.lease("sb", 1, 0).is_err());
    let pooled = m.pool_lease(7);
    assert_eq!((pooled.namespace.as_str(), pooled.host_interface.as_str()), ("ucloud-pool-7", "us7h"));
    assert_eq!(pooled.namespace_path, Path::new("/tmp/x/netns/ucloud-pool-7"));
    assert_eq!((pooled.host_ip, pooled.guest_ip), (m.lease("any", 3, 7).unwrap().host_ip, m.lease("any", 3, 7).unwrap().guest_ip));
}

#[test]
fn state_round_trips_in_python_format() {
    let root = temp_root("state");
    let m = manager(&root);
    // Python wrote this: a leased slot in the pool is dropped on load; policies are kept.
    std::fs::write(root.join("network-slots.json"), "{\"leases\":{\"a\\u00001\":3},\"policies\":{},\"pool\":[3,5],\"version\":1}\n").unwrap();
    let mut state = m.load().unwrap();
    assert_eq!(state["pool"], json!([5]));
    state["leases"].as_object_mut().unwrap().insert("b\u{0}2".into(), json!(1));
    m.store(&state, false).unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("network-slots.json")).unwrap(),
        "{\"leases\":{\"a\\u00001\":3,\"b\\u00002\":1},\"policies\":{},\"pool\":[5],\"version\":1}\n"
    );
    // A pool-only write is the same bytes.
    m.store(&state, true).unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join("network-slots.json")).unwrap(),
        "{\"leases\":{\"a\\u00001\":3,\"b\\u00002\":1},\"policies\":{},\"pool\":[5],\"version\":1}\n"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn iptables_save_keys_match_install_checks() {
    let saved = "*filter\n:INPUT ACCEPT [0:0]\n-A INPUT -s 100.96.0.0/16 -j DROP\n\
        -A FORWARD -s 100.96.0.0/16 -d 10.36.101.16/32 -p tcp -m tcp --dport 8092 -j ACCEPT\nCOMMIT\n\
        *nat\n-A POSTROUTING -s 100.96.0.0/16 -j MASQUERADE\nCOMMIT\n";
    let rules = parse_iptables_save(saved).unwrap();
    assert!(rules.contains(&rule_key(&["iptables", "-C", "INPUT", "-s", CIDR, "-j", "DROP"])));
    let egress = tcp_egress_rule("10.36.101.16", 8092);
    let check: Vec<&str> = ["iptables", "-C", "FORWARD"].into_iter().chain(egress.iter().map(String::as_str)).collect();
    assert!(rules.contains(&rule_key(&check)));
    assert!(rules.contains(&rule_key(&["iptables", "-t", "nat", "-C", "POSTROUTING", "-s", CIDR, "-j", "MASQUERADE"])));
    assert!(parse_iptables_save("*filter\n-A INPUT -j DROP\n").is_none());
}

#[test]
fn egress_endpoints_are_literal_ipv4_only() {
    assert_eq!(TcpEgress::parse("10.36.101.16:8092"), Some(TcpEgress { address: Ipv4Addr::new(10, 36, 101, 16), port: 8092 }));
    assert_eq!(TcpEgress::parse("relay.example:8092"), None);
}

/// A gate a fake kernel operation waits on until the test opens it.
#[derive(Clone, Default)]
struct Gate(Arc<(Mutex<bool>, Condvar)>);

impl Gate {
    fn wait(&self) {
        let (open, changed) = &*self.0;
        let guard = open.lock().unwrap();
        let (guard, timeout) = changed.wait_timeout_while(guard, Duration::from_secs(10), |open| !*open).unwrap();
        assert!(*guard && !timeout.timed_out(), "the test never opened the gate");
    }

    fn open(&self) {
        *self.0.0.lock().unwrap() = true;
        self.0.1.notify_all();
    }
}

#[derive(Default)]
struct FakeState {
    /// Host interface → the namespace holding its eth0 peer.
    links: BTreeMap<String, u64>,
    configured: BTreeSet<String>,
    /// (thread name, operation, object), in order.
    calls: Vec<(String, &'static str, String)>,
    fail_namespace: Option<String>,
    fail_attach: bool,
    block_pool_creates: Option<Gate>,
    block_sandbox_creates: Option<(Gate, Gate)>,
}

/// Python's FakeKernel: namespaces are files naming an id, as bind mounts name
/// one nsfs inode. A namespace lives while a name refers to it; dropping its
/// last name destroys it with its veth pair.
struct FakeKernel {
    root: PathBuf,
    state: Mutex<FakeState>,
    next: AtomicU64,
}

impl FakeKernel {
    fn new(root: PathBuf) -> Arc<FakeKernel> {
        Arc::new(FakeKernel { root, state: Mutex::new(FakeState::default()), next: AtomicU64::new(1) })
    }

    fn record(&self, state: &mut FakeState, operation: &'static str, object: &str) {
        let thread = std::thread::current().name().unwrap_or("").to_string();
        state.calls.push((thread, operation, object.to_string()));
    }

    fn namespace(&self, name: &str) -> Option<u64> {
        std::fs::read_to_string(self.root.join(name)).ok()?.parse().ok()
    }

    fn names(&self) -> BTreeSet<String> {
        std::fs::read_dir(&self.root)
            .map(|entries| entries.map(|entry| entry.unwrap().file_name().into_string().unwrap()).collect())
            .unwrap_or_default()
    }

    fn links(&self) -> BTreeMap<String, u64> {
        self.state.lock().unwrap().links.clone()
    }

    /// Operations a thread ran, as (operation, object).
    fn calls_by(&self, thread: &str) -> Vec<(&'static str, String)> {
        let state = self.state.lock().unwrap();
        state.calls.iter().filter(|(name, _, _)| name == thread).map(|(_, op, object)| (*op, object.clone())).collect()
    }

    fn calls(&self) -> Vec<(&'static str, String)> {
        self.state.lock().unwrap().calls.iter().map(|(_, op, object)| (*op, object.clone())).collect()
    }

    fn drop_name(&self, state: &mut FakeState, path: &Path) {
        let identity: u64 = std::fs::read_to_string(path).unwrap().parse().unwrap();
        std::fs::remove_file(path).unwrap();
        let alive: BTreeSet<u64> = self.names().iter().filter_map(|name| self.namespace(name)).collect();
        if !alive.contains(&identity) {
            let gone: Vec<String> = state.links.iter().filter(|(_, peer)| **peer == identity).map(|(link, _)| link.clone()).collect();
            for link in gone {
                state.links.remove(&link);
                state.configured.remove(&link);
            }
        }
    }
}

impl Kernel for FakeKernel {
    fn create_pair(&self, lease: &Lease) -> Result<(), NetworkError> {
        let (pool_gate, sandbox_gate) = {
            let mut state = self.state.lock().unwrap();
            self.record(&mut state, "create", &lease.namespace);
            (state.block_pool_creates.clone(), state.block_sandbox_creates.clone())
        };
        if lease.namespace.starts_with("ucloud-pool-") {
            if let Some(gate) = pool_gate {
                gate.wait();
            }
        } else if let Some((entered, release)) = sandbox_gate {
            entered.open();
            release.wait();
        }
        let mut state = self.state.lock().unwrap();
        if state.fail_namespace.as_deref() == Some(lease.namespace.as_str()) {
            return Err(fail("injected netns failure"));
        }
        std::fs::create_dir_all(&self.root)?;
        let identity = self.next.fetch_add(1, Ordering::SeqCst);
        std::fs::OpenOptions::new().write(true).create_new(true).open(&lease.namespace_path)?;
        std::fs::write(&lease.namespace_path, identity.to_string())?;
        if state.links.contains_key(&lease.host_interface) {
            return Err(fail(format!("{} exists", lease.host_interface)));
        }
        state.links.insert(lease.host_interface.clone(), identity);
        state.configured.insert(lease.host_interface.clone());
        Ok(())
    }

    fn configure_pair(&self, lease: &Lease) -> Result<(), NetworkError> {
        let mut state = self.state.lock().unwrap();
        self.record(&mut state, "configure", &lease.namespace);
        let identity = self.namespace(&lease.namespace);
        if identity.is_none() || state.links.get(&lease.host_interface) != identity.as_ref() {
            return Err(fail("guest eth0 is absent"));
        }
        state.configured.insert(lease.host_interface.clone());
        Ok(())
    }

    fn delete_link(&self, name: &str) -> Result<(), NetworkError> {
        let mut state = self.state.lock().unwrap();
        self.record(&mut state, "delete_link", name);
        state.links.remove(name);
        state.configured.remove(name);
        Ok(())
    }

    fn attach_namespace(&self, source: &Path, target: &Path) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap();
        self.record(&mut state, "attach", &target.file_name().unwrap().to_string_lossy());
        if state.fail_attach {
            return Err(std::io::Error::other("bind refused"));
        }
        std::fs::hard_link(source, target) // One inode, as two binds name one nsfs inode.
    }

    fn detach_namespace(&self, path: &Path) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap();
        self.record(&mut state, "detach", &path.file_name().unwrap().to_string_lossy());
        if path.exists() {
            self.drop_name(&mut state, path);
        }
        Ok(())
    }

    fn interface_present(&self, name: &str) -> bool {
        self.state.lock().unwrap().links.contains_key(name)
    }
}

fn wait_for(what: &str, mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !predicate() {
        assert!(Instant::now() < deadline, "{what} was not reached");
        std::thread::sleep(Duration::from_millis(2));
    }
}

struct PoolTest {
    root: PathBuf,
    kernel: Arc<FakeKernel>,
    managers: Mutex<Vec<Arc<NetworkManager>>>,
}

impl PoolTest {
    fn new(name: &str) -> PoolTest {
        let root = temp_root(name);
        PoolTest { kernel: FakeKernel::new(root.join("netns")), root, managers: Mutex::new(Vec::new()) }
    }

    fn manager(&self, size: usize) -> Arc<NetworkManager> {
        let mut manager = NetworkManager::with_kernel(
            self.root.join("network-slots.json"),
            self.root.join("netns"),
            vec![],
            false,
            self.kernel.clone(),
        )
        .with_pool_size(size)
        .unwrap();
        manager.pool.retry = Duration::from_millis(10);
        manager.assume_host_rules_current();
        let manager = Arc::new(manager);
        self.managers.lock().unwrap().push(manager.clone());
        manager
    }

    fn state(&self) -> Value {
        serde_json::from_str(&std::fs::read_to_string(self.root.join("network-slots.json")).unwrap()).unwrap()
    }

    fn pool(&self) -> Vec<u64> {
        self.state()["pool"].as_array().unwrap().iter().map(|slot| slot.as_u64().unwrap()).collect()
    }

    fn write_state(&self, leases: Value, pool: Value) {
        let state = json!({"version": 1, "leases": leases, "pool": pool});
        std::fs::write(self.root.join("network-slots.json"), state.to_string()).unwrap();
    }

    fn assert_pair_owned(&self, lease: &Lease) {
        let links = self.kernel.links();
        assert_eq!(links.get(&lease.host_interface).copied(), self.kernel.namespace(&lease.namespace), "{lease:?}");
        assert!(self.kernel.state.lock().unwrap().configured.contains(&lease.host_interface));
        assert!(!self.root.join("netns").join(format!("ucloud-pool-{}", lease.slot)).exists());
    }

    fn here() -> String {
        std::thread::current().name().unwrap().to_string()
    }
}

impl Drop for PoolTest {
    fn drop(&mut self) {
        for manager in self.managers.lock().unwrap().iter() {
            manager.stop_pool();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn refill_fills_the_pool_and_a_create_takes_a_pair_without_kernel_work() {
    let test = PoolTest::new("refill");
    let manager = test.manager(3);
    manager.start_pool();
    wait_for("three ready pairs", || manager.pool_ready().len() == 3);
    assert_eq!(test.pool(), [1, 2, 3]);

    let lease = manager.ensure_direct("sandbox-a", 1).unwrap();

    // Slot 1 moved to the lease; only its name changed hands.
    assert_eq!(lease.slot, 1);
    let ops: Vec<&str> = test.kernel.calls_by(&PoolTest::here()).into_iter().map(|(op, _)| op).collect();
    assert_eq!(ops, ["attach", "detach"]);
    test.assert_pair_owned(&lease);
    let state = test.state();
    assert_eq!(state["leases"], json!({"sandbox-a\u{0}1": 1}));
    assert!(!test.pool().contains(&1));
    wait_for("the refill", || manager.pool_ready().len() == 3);
    assert_eq!(test.pool(), [2, 3, 4]);
    // An existing lease is reused, its pair rechecked.
    assert_eq!(manager.ensure_direct("sandbox-a", 1).unwrap(), lease);
    test.assert_pair_owned(&lease);
}

#[test]
fn an_exhausted_pool_falls_back_to_creating_the_pair() {
    let test = PoolTest::new("exhausted");
    let manager = test.manager(2);
    manager.start_pool();
    wait_for("two ready pairs", || manager.pool_ready().len() == 2);
    let gate = Gate::default();
    test.kernel.state.lock().unwrap().block_pool_creates = Some(gate.clone());
    let first = manager.ensure_direct("a", 1).unwrap();
    let second = manager.ensure_direct("b", 1).unwrap();
    wait_for("slot 3 reserved, unfilled", || test.pool() == [3]);

    let third = manager.ensure_direct("c", 1).unwrap();

    assert_eq!((first.slot, second.slot), (1, 2));
    // A reserved but unfilled pool slot is never leased.
    assert_eq!(third.slot, 4);
    assert!(test.kernel.calls_by(&PoolTest::here()).contains(&("create", third.namespace.clone())));
    for lease in [&first, &second, &third] {
        test.assert_pair_owned(lease);
    }
    gate.open();
    wait_for("the refill", || manager.pool_ready().len() == 2);
    assert_eq!(test.pool(), [3, 5]);
}

#[test]
fn restart_rechecks_a_partial_pool_and_finishes_interrupted_hand_offs() {
    let test = PoolTest::new("restart");
    let setup = test.manager(0);
    let (complete, partial) = (setup.pool_lease(1), setup.pool_lease(2));
    setup.ensure_kernel_lease(&complete).unwrap();
    std::fs::write(&partial.namespace_path, "999").unwrap(); // crashed before its veth
    // Crash after the durable hand-off of slot 3 to "x", before attach; and
    // after slot 4's attach to "y", before the pooled name was dropped.
    let (handed, attached) = (setup.pool_lease(3), setup.pool_lease(4));
    setup.ensure_kernel_lease(&handed).unwrap();
    setup.ensure_kernel_lease(&attached).unwrap();
    let y = setup.lease("y", 1, 4).unwrap();
    std::fs::hard_link(&attached.namespace_path, &y.namespace_path).unwrap();
    let handed_namespace = test.kernel.namespace(&handed.namespace);
    test.write_state(json!({"x\u{0}1": 3, "y\u{0}1": 4}), json!([1, 2]));

    let manager = test.manager(3);
    manager.start_pool();
    wait_for("three ready pairs", || manager.pool_ready().len() == 3);
    let before = test.kernel.calls_by(&PoolTest::here()).len();
    let x = manager.ensure_direct("x", 1).unwrap();
    let y = manager.ensure_direct("y", 1).unwrap();

    assert_eq!(test.pool(), [1, 2, 5]);
    for slot in [1, 2, 5] {
        let pooled = manager.pool_lease(slot);
        assert_eq!(test.kernel.links().get(&pooled.host_interface).copied(), test.kernel.namespace(&pooled.namespace));
    }
    assert_eq!((x.slot, y.slot), (3, 4));
    assert_eq!(test.kernel.namespace(&x.namespace), handed_namespace);
    for lease in [&x, &y] {
        test.assert_pair_owned(lease);
    }
    let ops: Vec<&str> = test.kernel.calls_by(&PoolTest::here())[before..].iter().map(|(op, _)| *op).collect();
    assert!(!ops.contains(&"create"), "{ops:?}");
}

#[test]
fn an_unadoptable_hand_off_deletes_the_pooled_link_before_its_name() {
    // A stale name at the lease's path, or a failed bind. Dropping the pooled
    // name first would free its veth asynchronously.
    let test = PoolTest::new("unadoptable");
    let manager = test.manager(2);
    manager.start_pool();
    wait_for("two ready pairs", || manager.pool_ready().len() == 2);
    let stale_path = manager.lease("stale", 1, 1).unwrap().namespace_path;
    std::fs::write(&stale_path, "998").unwrap();
    let stale = manager.ensure_direct("stale", 1).unwrap();
    test.kernel.state.lock().unwrap().fail_attach = true;
    let refused = manager.ensure_direct("refused", 1).unwrap();

    assert_eq!((stale.slot, refused.slot), (1, 2));
    let calls = test.kernel.calls_by(&PoolTest::here());
    for slot in [1, 2] {
        let link = calls.iter().position(|call| *call == ("delete_link", format!("us{slot}h"))).unwrap();
        let name = calls.iter().position(|call| *call == ("detach", format!("ucloud-pool-{slot}"))).unwrap();
        assert!(link < name, "{calls:?}");
    }
    for lease in [&stale, &refused] {
        test.assert_pair_owned(lease);
    }
}

#[test]
fn concurrent_creates_never_share_a_slot_or_pair() {
    let test = PoolTest::new("concurrent");
    let manager = test.manager(4);
    manager.start_pool();
    wait_for("four ready pairs", || manager.pool_ready().len() == 4);
    let leases: Vec<Lease> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..16).map(|n| {
            let manager = manager.clone();
            scope.spawn(move || manager.ensure_direct(&format!("s{n}"), 1).unwrap())
        }).collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    let slots: BTreeSet<u32> = leases.iter().map(|lease| lease.slot).collect();
    assert_eq!(slots.len(), 16);
    let namespaces: BTreeSet<Option<u64>> = leases.iter().map(|lease| test.kernel.namespace(&lease.namespace)).collect();
    assert_eq!(namespaces.len(), 16);
    for lease in &leases {
        test.assert_pair_owned(lease);
    }
    wait_for("the refill", || manager.pool_ready().len() == 4);
    let state = test.state();
    let leased: BTreeSet<u64> = state["leases"].as_object().unwrap().values().map(|slot| slot.as_u64().unwrap()).collect();
    assert_eq!(leased.len(), 16);
    assert!(test.pool().iter().all(|slot| !leased.contains(slot)));
}

#[test]
fn a_lease_from_an_older_release_owns_a_pooled_slot() {
    let test = PoolTest::new("older");
    test.write_state(json!({"old\u{0}1": 2}), json!([1, 2]));
    let manager = test.manager(2);
    assert_eq!(manager.load().unwrap()["pool"], json!([1]));
    manager.start_pool();
    wait_for("two ready pairs", || manager.pool_ready().len() == 2);
    assert_eq!(test.pool(), [1, 3]);
    assert!(!test.kernel.links().contains_key("us2h"));
}

#[test]
fn a_disabled_pool_is_trimmed_and_its_pairs_deleted() {
    let test = PoolTest::new("disabled");
    let setup = test.manager(0);
    for slot in [1, 2] {
        setup.ensure_kernel_lease(&setup.pool_lease(slot)).unwrap();
    }
    test.write_state(json!({}), json!([1, 2]));
    let manager = test.manager(0);
    manager.start_pool();
    wait_for("an empty pool", || test.pool().is_empty());
    wait_for("the thread to end", || manager.pool.thread.lock().unwrap().as_ref().unwrap().is_finished());
    assert!(test.kernel.names().is_empty());
    assert!(test.kernel.links().is_empty());
}

#[test]
fn stray_pool_names_join_the_pool_unless_their_slot_is_leased() {
    let test = PoolTest::new("stray");
    let setup = test.manager(0);
    // Slot 5: a hand-off to "z" a crash interrupted. Slot 7: nobody's.
    for slot in [5, 7, 9] {
        setup.ensure_kernel_lease(&setup.pool_lease(slot)).unwrap();
    }
    test.write_state(json!({"z\u{0}1": 5}), json!([]));
    let manager = test.manager(1);
    manager.start_pool();
    wait_for("one ready pair", || manager.pool_ready().len() == 1);
    // Both strays were pooled and one trimmed; neither was handed out unchecked.
    wait_for("the trim", || test.pool().len() == 1);
    assert_eq!(test.pool(), [7]);
    assert_eq!(manager.pool_ready(), [7]);
    assert!(!test.kernel.names().contains("ucloud-pool-9"));
    assert!(test.kernel.names().contains("ucloud-pool-5"));
    let z = manager.ensure_direct("z", 1).unwrap();
    assert_eq!(z.slot, 5);
    test.assert_pair_owned(&z);
}

#[test]
fn one_process_owns_the_pool() {
    let test = PoolTest::new("owner");
    let (owner, other) = (test.manager(2), test.manager(2));
    owner.start_pool();
    wait_for("two ready pairs", || owner.pool_ready().len() == 2);
    other.start_pool();
    std::thread::sleep(Duration::from_millis(50));
    assert!(other.pool_ready().is_empty());
    assert_eq!(other.ensure_direct("a", 1).unwrap().slot, 3);
    assert_eq!(owner.ensure_direct("b", 1).unwrap().slot, 1);
    // Once the owner stops, the other takes over and rechecks before handing out.
    owner.stop_pool();
    wait_for("the other's pool", || other.pool_ready().len() == 2);
    assert_eq!(other.ensure_direct("c", 1).unwrap().slot, 2);
}

#[test]
fn a_failed_refill_retries_without_handing_out_the_slot() {
    let test = PoolTest::new("failed");
    test.kernel.state.lock().unwrap().fail_namespace = Some("ucloud-pool-1".into());
    let manager = test.manager(1);
    manager.start_pool();
    let attempts = || test.kernel.calls().iter().filter(|call| **call == ("create", "ucloud-pool-1".to_string())).count();
    wait_for("two attempts", || attempts() >= 2);
    assert!(manager.pool_ready().is_empty());
    assert_eq!(manager.ensure_direct("a", 1).unwrap().slot, 2);
    test.kernel.state.lock().unwrap().fail_namespace = None;
    wait_for("one ready pair", || manager.pool_ready().len() == 1);
    assert_eq!(test.pool(), [1]);
}

#[test]
fn the_refill_waits_for_ensures_in_flight() {
    let test = PoolTest::new("defer");
    let manager = test.manager(1);
    let (entered, release) = (Gate::default(), Gate::default());
    test.kernel.state.lock().unwrap().block_sandbox_creates = Some((entered.clone(), release.clone()));
    std::thread::scope(|scope| {
        let pending = scope.spawn(|| manager.ensure_direct("a", 1).unwrap());
        entered.wait(); // outside the state lock, inside the ensure
        manager.start_pool();
        std::thread::sleep(Duration::from_millis(200));
        assert!(test.kernel.calls_by("noded-network-pool").iter().all(|(op, _)| *op != "create"));
        release.open();
        pending.join().unwrap();
    });
    wait_for("one ready pair", || manager.pool_ready().len() == 1);
}

#[test]
fn queued_creates_share_one_durable_write_and_fail_alone() {
    let test = PoolTest::new("group");
    let state = json!({"version": 1, "leases": {"bad\u{0}1": 9}, "policies": {"bad\u{0}1": {"egress": "relay", "relay": "r"}}, "pool": []});
    std::fs::write(test.root.join("network-slots.json"), state.to_string()).unwrap();
    let manager = test.manager(0);
    // Another process (the agent) holds the state flock while creates queue.
    let held = FileLock::acquire(&test.root.join("network-slots.json.lock"), false).unwrap();
    let outcomes: Vec<Result<Lease, NetworkError>> = std::thread::scope(|scope| {
        let handles: Vec<_> = ["a", "b", "bad", "c", "d"]
            .into_iter()
            .map(|id| {
                let manager = manager.clone();
                scope.spawn(move || manager.ensure_direct(id, 1))
            })
            .collect();
        wait_for("five queued allocations", || manager.allocator.pending() == 5);
        drop(held);
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    assert_eq!(manager.lease_writes.load(std::sync::atomic::Ordering::Relaxed), 1);
    let mut slots = BTreeSet::new();
    for (id, outcome) in ["a", "b", "bad", "c", "d"].into_iter().zip(&outcomes) {
        match (id, outcome) {
            ("bad", Err(error)) => assert_eq!(error.to_string(), "network policy is immutable for a sandbox generation"),
            (_, Ok(lease)) => {
                test.assert_pair_owned(lease);
                slots.insert(lease.slot);
            }
            (id, outcome) => panic!("{id}: {outcome:?}"),
        }
    }
    assert_eq!(slots, BTreeSet::from([1, 2, 3, 4]));
    let leases = test.state()["leases"].clone();
    assert_eq!(leases.as_object().unwrap().len(), 5);
    // Existing leases are found without a write.
    for id in ["a", "b"] {
        manager.ensure_direct(id, 1).unwrap();
    }
    assert_eq!(manager.lease_writes.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(test.state()["leases"], leases);
}

#[test]
fn pool_sizes_are_bounded() {
    let root = temp_root("bounds");
    assert!(manager(&root).with_pool_size(1024).is_ok());
    assert!(manager(&root).with_pool_size(1025).is_err());
    let _ = std::fs::remove_dir_all(&root);
}

/// Root-only: the manager over the real kernel, with its pool, at low slots
/// (so not on a node that runs sandboxes). Host rules are skipped: this
/// never touches the machine's firewall.
#[test]
fn real_pairs_from_the_pool_and_cold() {
    // SAFETY: no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("skipped: needs root");
        return;
    }
    let root = temp_root("real");
    let manager = Arc::new(
        NetworkManager::new(root.join("network-slots.json"), PathBuf::from("/run/netns"), vec![], false).with_pool_size(2).unwrap(),
    );
    manager.assume_host_rules_current();
    let id = format!("noded-test-{}", std::process::id());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        manager.start_pool();
        wait_for("two ready pairs", || manager.pool_ready().len() == 2);
        let pooled = manager.ensure_direct(&format!("{id}-a"), 1).unwrap();
        assert_eq!(pooled.slot, 1);
        assert!(pooled.namespace_path.exists() && !manager.pool_lease(1).namespace_path.exists());
        manager.stop_pool();
        let cold = manager.ensure_direct(&format!("{id}-b"), 1).unwrap();
        let again = manager.ensure_direct(&format!("{id}-b"), 1).unwrap();
        assert_eq!(cold, again);
        for lease in [&pooled, &cold] {
            let output = std::process::Command::new("ip")
                .args(["netns", "exec", &lease.namespace, "ping", "-c1", "-W2", &lease.host_ip.to_string()])
                .output()
                .unwrap();
            assert!(output.status.success(), "{lease:?}: {}", String::from_utf8_lossy(&output.stdout));
        }
    }));
    manager.stop_pool();
    let state = manager.load().unwrap();
    for (key, slot) in state["leases"].as_object().unwrap() {
        let (sandbox, generation) = key.split_once('\0').unwrap();
        manager.cleanup_kernel_lease(&manager.lease(sandbox, generation.parse().unwrap(), slot.as_u64().unwrap() as u32).unwrap());
    }
    for slot in pool_slots(&state) {
        manager.cleanup_kernel_lease(&manager.pool_lease(slot as u32));
    }
    let _ = std::fs::remove_dir_all(&root);
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
