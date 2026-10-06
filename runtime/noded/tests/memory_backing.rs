//! The memory-backing allocator against temporary journals, and against the
//! Python agent's own `MemoryBackingStore` on the same files.

use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use rusqlite::types::Value as SqlValue;
use serde_json::{Value, json};
use ucloud_noded::memory_backing::{
    ActiveMode, MemoryBackingConfig, MemoryBackingError, MemoryBackingRef, MemoryBackingStore, MemoryQuota, QuotaBackend,
    Result, XfsMemoryQuota, fsxattr, marker_bytes, FS_XFLAG_PROJINHERIT, MARKER,
};

struct TempDir(PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn temp_dir() -> TempDir {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "noded-memory-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&path);
    fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
    TempDir(path)
}

/// Python's tests' FakeQuota: projects by path; `strict` rejects a path it did
/// not provision (a Python-provisioned one is unknown to this process).
#[derive(Clone, Default)]
struct FakeQuota {
    projects: Arc<Mutex<HashMap<PathBuf, (i64, i64)>>>,
    provisions: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
    strict: bool,
}

impl MemoryQuota for FakeQuota {
    fn validate_root(&mut self, _root: &Path) -> Result<()> {
        Ok(())
    }

    fn validate_active_root(&self, _active_root: &Path, _ram_swappable: bool) -> Result<()> {
        Ok(())
    }

    fn provision(&self, path: &Path, project_id: i64, limit_bytes: i64) -> Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(MemoryBackingError::Command { argv: vec!["xfs_quota".into()], status: Some(1), stderr: "quota failed".into() });
        }
        self.provisions.fetch_add(1, Ordering::SeqCst);
        self.projects.lock().unwrap().insert(path.to_path_buf(), (project_id, limit_bytes));
        self.validate_project(path, project_id)
    }

    fn validate_project(&self, path: &Path, project_id: i64) -> Result<()> {
        match self.projects.lock().unwrap().get(path) {
            Some((actual, _)) if *actual != project_id => Err(MemoryBackingError::Backing("project changed".into())),
            None if self.strict => Err(MemoryBackingError::Backing("project changed".into())),
            _ => Ok(()),
        }
    }
}

struct Node {
    dir: TempDir,
    capacity: u64,
    ram: bool,
}

impl Node {
    fn new(capacity: u64, ram: bool) -> Node {
        let dir = temp_dir();
        if ram {
            fs::DirBuilder::new().mode(0o700).create(dir.0.join("ram")).unwrap();
        }
        Node { dir, capacity, ram }
    }

    fn root(&self) -> PathBuf {
        self.dir.0.join("disk")
    }

    fn journal(&self) -> PathBuf {
        self.dir.0.join("state").join("memory-backing.sqlite")
    }

    fn ram_root(&self) -> PathBuf {
        self.dir.0.join("ram")
    }

    fn config(&self) -> MemoryBackingConfig {
        MemoryBackingConfig {
            root: self.root(),
            journal: self.journal(),
            hard_capacity_bytes: self.capacity,
            active_root: self.ram.then(|| self.ram_root()),
            ram_swappable: false,
        }
    }

    fn open(&self, quota: &FakeQuota) -> Result<MemoryBackingStore> {
        MemoryBackingStore::open(self.config(), Box::new(quota.clone()))
    }

    fn sql(&self) -> Connection {
        Connection::open(self.journal()).unwrap()
    }

    fn python_args(&self) -> Value {
        json!({
            "root": self.root(),
            "journal": self.journal(),
            "capacity": self.capacity,
            "active": if self.ram { Some(self.ram_root()) } else { None },
        })
    }
}

fn reference(sandbox_id: &str, quota_bytes: u64) -> MemoryBackingRef {
    MemoryBackingRef::new(format!("{sandbox_id}.sandbox-1"), quota_bytes).unwrap()
}

fn row(node: &Node, allocation_id: &str) -> Vec<SqlValue> {
    node.sql()
        .query_row("SELECT * FROM allocations WHERE allocation_id=?", [allocation_id], |r| {
            (0..8).map(|i| r.get(i)).collect()
        })
        .unwrap()
}

fn counter(node: &Node) -> i64 {
    node.sql().query_row("SELECT value FROM counter", [], |r| r.get(0)).unwrap()
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path).unwrap().permissions().mode() & 0o7777
}

fn message<T>(result: Result<T>) -> String {
    match result {
        Ok(_) => panic!("expected an error"),
        Err(error) => error.to_string(),
    }
}

#[test]
fn prepare_claims_marks_and_provisions_once() {
    let node = Node::new(16384, false);
    let quota = FakeQuota::default();
    let store = node.open(&quota).unwrap();
    let r = reference("sb-1", 8192);
    let lease = store.prepare(&r, "sb-1", 1, Some(4096)).unwrap();
    assert_eq!((lease.project_id, lease.active_mode), (600000, ActiveMode::File));
    assert_eq!(lease.path, node.root().join("sb-1.sandbox-1"));
    assert_eq!(
        row(&node, "sb-1.sandbox-1"),
        vec![
            SqlValue::Text("sb-1.sandbox-1".into()),
            SqlValue::Text("sb-1".into()),
            SqlValue::Integer(1),
            SqlValue::Integer(600000),
            SqlValue::Integer(8192),
            SqlValue::Text("ready".into()),
            SqlValue::Text("file".into()),
            SqlValue::Integer(4096),
        ]
    );
    assert_eq!(counter(&node), 600001);
    let marker = lease.path.join(MARKER);
    assert_eq!(
        fs::read_to_string(&marker).unwrap(),
        r#"{"allocation_id": "sb-1.sandbox-1", "project_id": 600000, "quota_bytes": 8192, "sandbox_generation": 1, "sandbox_id": "sb-1", "version": 1}"#
    );
    assert_eq!((mode(&lease.path), mode(&marker)), (0o700, 0o600));
    assert_eq!(quota.projects.lock().unwrap()[&lease.path], (600000, 4096));
    assert!(mode(&store.lease_root().join("sb-1.sandbox-1.mutation")) & 0o077 == 0);
    assert_eq!(store.active_mode("sb-1", 1), Some(ActiveMode::File));

    // A ready allocation is validated, not re-provisioned; the claim is reused.
    assert_eq!(store.prepare(&r, "sb-1", 1, Some(8192)).unwrap(), lease);
    assert_eq!(store.require(&r, "sb-1", 1).unwrap(), lease);
    assert_eq!((quota.provisions.load(Ordering::SeqCst), counter(&node)), (1, 600001));

    let second = store.prepare(&reference("sb-2", 8192), "sb-2", 1, None).unwrap();
    assert_eq!(second.project_id, 600001);
}

#[test]
fn prepare_refuses_foreign_identities_limits_and_capacity() {
    let node = Node::new(16384, false);
    let store = node.open(&FakeQuota::default()).unwrap();
    let r = reference("sb-1", 8192);
    assert_eq!(message(store.prepare(&r, "sb-2", 1, None)), "memory allocation belongs to another incarnation");
    assert_eq!(message(store.prepare(&r, "sb-1", 2, None)), "memory allocation belongs to another incarnation");
    assert_eq!(message(store.prepare(&r, "sb-1", 1, Some(8193))), "memory allocation limit exceeds its ceiling");
    assert_eq!(message(store.prepare(&r, "sb-1", 1, Some(0))), "memory allocation limit exceeds its ceiling");
    store.prepare(&r, "sb-1", 1, None).unwrap();
    let error = store.prepare(&reference("sb-1", 4096), "sb-1", 1, None).unwrap_err();
    assert!(matches!(&error, MemoryBackingError::Backing(m) if m == "memory allocation identity/claim conflicts"));
    assert_eq!(error.python_class(), "MemoryBackingError");

    // The capacity check counts limits, not ceilings, and inserts nothing on failure.
    store.prepare(&reference("sb-2", 8192), "sb-2", 1, Some(4096)).unwrap();
    assert_eq!(message(store.prepare(&reference("sb-3", 8192), "sb-3", 1, Some(4097))), "memory backing hard capacity exhausted");
    assert_eq!(counter(&node), 600002);
    let rows: i64 = node.sql().query_row("SELECT COUNT(*) FROM allocations", [], |r| r.get(0)).unwrap();
    assert_eq!(rows, 2);
    store.prepare(&reference("sb-3", 8192), "sb-3", 1, Some(4096)).unwrap();
}

#[test]
fn an_interrupted_prepare_resumes_with_its_claim() {
    let node = Node::new(16384, true);
    let quota = FakeQuota::default();
    let store = node.open(&quota).unwrap();
    let r = reference("sb-1", 8192);
    quota.fail.store(true, Ordering::SeqCst);
    let error = store.prepare(&r, "sb-1", 1, Some(1024)).unwrap_err();
    assert_eq!(error.python_class(), "CalledProcessError");
    assert_eq!(row(&node, "sb-1.sandbox-1")[5], SqlValue::Text("preparing".into()));
    assert!(node.root().join("sb-1.sandbox-1").join(MARKER).exists());
    assert_eq!(store.active_mode("sb-1", 1), None);
    // Python's provisioner reaches `require` only after a ready prepare.
    assert_eq!(message(store.require(&r, "sb-1", 1)), "memory allocation is not retained by this incarnation");

    quota.fail.store(false, Ordering::SeqCst);
    let reopened = node.open(&quota).unwrap();
    let lease = reopened.prepare(&r, "sb-1", 1, None).unwrap();
    assert_eq!((lease.project_id, lease.active_mode), (600000, ActiveMode::Ram));
    // The resumed claim keeps its journalled limit, not the new argument.
    assert_eq!(quota.projects.lock().unwrap()[&lease.path], (600000, 1024));
    assert_eq!(counter(&node), 600001);
    assert_eq!(mode(&node.ram_root().join("sb-1.sandbox-1")), 0o700);
    assert_eq!(reopened.active_mode("sb-1", 1), Some(ActiveMode::Ram));
}

#[test]
fn foreign_directory_contents_and_markers_conflict() {
    let node = Node::new(1 << 20, false);
    let store = node.open(&FakeQuota::default()).unwrap();

    let stray = node.root().join("sb-1.sandbox-1");
    fs::DirBuilder::new().mode(0o700).create(&stray).unwrap();
    fs::write(stray.join("leftover"), b"x").unwrap();
    assert_eq!(message(store.prepare(&reference("sb-1", 8192), "sb-1", 1, None)), "unmarked memory allocation is not empty");

    let other = node.root().join("sb-2.sandbox-1");
    fs::DirBuilder::new().mode(0o700).create(&other).unwrap();
    fs::write(other.join(MARKER), b"{\"version\": 1}").unwrap();
    assert_eq!(message(store.prepare(&reference("sb-2", 8192), "sb-2", 1, None)), "memory allocation marker conflicts");

    let linked = node.root().join("sb-3.sandbox-1");
    fs::DirBuilder::new().mode(0o700).create(&linked).unwrap();
    std::os::unix::fs::symlink(other.join(MARKER), linked.join(MARKER)).unwrap();
    assert_eq!(message(store.prepare(&reference("sb-3", 8192), "sb-3", 1, None)), "memory allocation marker conflicts");

    let shared = node.root().join("sb-4.sandbox-1");
    fs::DirBuilder::new().mode(0o750).create(&shared).unwrap();
    fs::set_permissions(&shared, fs::Permissions::from_mode(0o750)).unwrap();
    assert_eq!(message(store.prepare(&reference("sb-4", 8192), "sb-4", 1, None)), "memory backing directory is not privately owned");
}

#[test]
fn deleting_and_deleted_rows() {
    let node = Node::new(1 << 20, true);
    let quota = FakeQuota::default();
    let store = node.open(&quota).unwrap();
    let r = reference("sb-1", 8192);
    let lease = store.prepare(&r, "sb-1", 1, None).unwrap();
    node.sql().execute("UPDATE allocations SET state='deleting'", []).unwrap();
    assert_eq!(message(store.prepare(&r, "sb-1", 1, None)), "memory allocation is being deleted");

    // A deleted row is replaced by a fresh claim that keeps its placement.
    node.sql().execute("UPDATE allocations SET state='deleted', active_mode='file'", []).unwrap();
    assert_eq!(message(store.prepare(&r, "sb-1", 1, None)), "deleted memory allocation path still exists");
    assert_eq!(message(store.prepare(&reference("sb-1", 4096), "sb-1", 1, None)), "reimported memory identity/claim conflicts");
    fs::remove_dir_all(&lease.path).unwrap();
    fs::remove_dir(node.ram_root().join("sb-1.sandbox-1")).unwrap();
    let again = store.prepare(&r, "sb-1", 1, None).unwrap();
    assert_eq!((again.project_id, again.active_mode), (600001, ActiveMode::File));
    assert_eq!(row(&node, "sb-1.sandbox-1")[6], SqlValue::Text("file".into()));
    assert!(!node.ram_root().join("sb-1.sandbox-1").exists());
}

#[test]
fn require_revalidates_marker_project_and_ram_directory() {
    let node = Node::new(1 << 20, true);
    let quota = FakeQuota::default();
    let store = node.open(&quota).unwrap();
    let r = reference("sb-1", 8192);
    assert_eq!(message(store.require(&r, "sb-1", 1)), "memory allocation is not retained by this incarnation");
    let lease = store.prepare(&r, "sb-1", 1, None).unwrap();
    assert_eq!(message(store.require(&reference("sb-1", 4096), "sb-1", 1)), "memory allocation is not retained by this incarnation");

    // tmpfs empties on reboot; require re-creates the RAM directory.
    let ram = node.ram_root().join("sb-1.sandbox-1");
    fs::remove_dir(&ram).unwrap();
    assert_eq!(store.require(&r, "sb-1", 1).unwrap(), lease);
    assert_eq!(mode(&ram), 0o700);

    quota.projects.lock().unwrap().insert(lease.path.clone(), (7, 0));
    assert_eq!(message(store.require(&r, "sb-1", 1)), "project changed");
    quota.projects.lock().unwrap().insert(lease.path.clone(), (600000, 0));

    fs::write(lease.path.join(MARKER), b"not json").unwrap();
    assert_eq!(store.require(&r, "sb-1", 1).unwrap_err().python_class(), "ValueError");
    fs::remove_file(lease.path.join(MARKER)).unwrap();
    assert!(matches!(store.require(&r, "sb-1", 1), Err(MemoryBackingError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound));
}

#[test]
fn open_validates_configuration_and_journal() {
    let node = Node::new(1 << 20, false);
    let quota = FakeQuota::default();
    let mut config = node.config();
    config.root = node.dir.0.join("a b");
    assert_eq!(message(MemoryBackingStore::open(config, Box::new(quota.clone()))), "memory backing root cannot contain whitespace");
    let mut config = node.config();
    config.active_root = Some(node.root());
    assert_eq!(message(MemoryBackingStore::open(config, Box::new(quota.clone()))), "RAM memory root must be a distinct absolute path");
    let mut config = node.config();
    config.hard_capacity_bytes = 0;
    assert!(matches!(MemoryBackingStore::open(config, Box::new(quota.clone())), Err(MemoryBackingError::Invalid(_))));

    let store = node.open(&quota).unwrap();
    store.prepare(&reference("sb-1", 8192), "sb-1", 1, None).unwrap();
    drop(store);
    // A RAM row on a node without a RAM root cannot be placed.
    node.sql().execute("UPDATE allocations SET active_mode='ram'", []).unwrap();
    assert_eq!(message(node.open(&quota)), "memory allocation has an unsupported active backing mode");
    node.sql().execute("UPDATE allocations SET active_mode='file'", []).unwrap();
    node.sql().execute_batch("PRAGMA user_version=4").unwrap();
    assert_eq!(message(node.open(&quota)), "unsupported memory backing journal version");
    node.sql().execute_batch("PRAGMA user_version=3").unwrap();

    let store = node.open(&quota).unwrap();
    assert_eq!(store.active_mode("sb-1", 1), Some(ActiveMode::File));
    let copy = node.dir.0.join("copy.sqlite");
    fs::copy(node.journal(), &copy).unwrap();
    fs::rename(&copy, node.journal()).unwrap();
    assert_eq!(message(store.prepare(&reference("sb-2", 8192), "sb-2", 1, None)), "memory journal file was replaced");
    assert_eq!(message(store.require(&reference("sb-1", 8192), "sb-1", 1)), "memory journal file was replaced");
}

#[test]
fn concurrent_prepares_get_distinct_projects() {
    let node = Node::new(1 << 20, false);
    let store = Arc::new(node.open(&FakeQuota::default()).unwrap());
    let threads: Vec<_> = (0..8)
        .map(|i| {
            let store = store.clone();
            std::thread::spawn(move || {
                let id = format!("sb-{i}");
                // Two racers per allocation: the mutation flock makes one claim.
                let r = reference(&id, 4096);
                let other = {
                    let store = store.clone();
                    let (r, id) = (r.clone(), id.clone());
                    std::thread::spawn(move || store.prepare(&r, &id, 1, None).unwrap())
                };
                let lease = store.prepare(&r, &id, 1, None).unwrap();
                assert_eq!(other.join().unwrap(), lease);
                lease.project_id
            })
        })
        .collect();
    let mut projects: Vec<i64> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    projects.sort();
    assert_eq!(projects, (600000..600008).collect::<Vec<_>>());
    assert_eq!(counter(&node), 600008);
}

#[test]
fn a_burst_of_prepares_group_commits_and_stays_exact() {
    // 64 creates at once: every claim and every ready is durable before its
    // prepare returns, but they share COMMITs; capacity stays exact.
    let node = Node::new(64 * 4096, false);
    let quota = FakeQuota::default();
    let store = Arc::new(node.open(&quota).unwrap());
    let before = store.journal_commits();
    let barrier = Arc::new(std::sync::Barrier::new(65));
    let threads: Vec<_> = (0..65)
        .map(|i| {
            let (store, barrier) = (store.clone(), barrier.clone());
            std::thread::spawn(move || {
                let id = format!("sb-{i}");
                barrier.wait();
                store.prepare(&reference(&id, 4096), &id, 1, None).map(|lease| lease.project_id)
            })
        })
        .collect();
    let outcomes: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    let mut projects: Vec<i64> = outcomes.iter().filter_map(|o| o.as_ref().ok().copied()).collect();
    let refused: Vec<String> = outcomes.into_iter().filter_map(|o| o.err().map(|e| e.to_string())).collect();
    // One more than fits: exactly one refusal, and it consumed no project id.
    assert_eq!(refused, ["memory backing hard capacity exhausted"]);
    projects.sort();
    assert_eq!(projects, (600000..600064).collect::<Vec<_>>());
    assert_eq!(counter(&node), 600064);
    let ready: i64 = node.sql().query_row("SELECT COUNT(*) FROM allocations WHERE state='ready'", [], |r| r.get(0)).unwrap();
    assert_eq!(ready, 64);
    let commits = store.journal_commits() - before;
    eprintln!("64 prepares, {commits} journal commits");
    assert!(commits <= 128, "{commits} commits");
}

#[test]
fn real_findmnt_checks_reject_the_wrong_filesystems() {
    let dir = temp_dir();
    let fstype = Command::new("findmnt").args(["-n", "-o", "FSTYPE", "--target"]).arg(&dir.0).output().unwrap();
    if String::from_utf8_lossy(&fstype.stdout).trim() != "xfs" {
        assert_eq!(message(XfsMemoryQuota::new().validate_root(&dir.0)), "memory backing requires an XFS project-quota filesystem");
    }
    let shm = Path::new("/dev/shm");
    let options = Command::new("findmnt").args(["-n", "-o", "FSTYPE,OPTIONS", "--target"]).arg(shm).output().unwrap();
    let options = String::from_utf8_lossy(&options.stdout).to_string();
    if options.starts_with("tmpfs ") {
        let noswap = options.trim().split(',').any(|o| o == "noswap");
        let quota = XfsMemoryQuota::new();
        assert!(quota.validate_active_root(shm, !noswap).is_ok());
        assert_eq!(message(quota.validate_active_root(shm, noswap)), "active RAM backing requires tmpfs, swappable only on pause-tier nodes");
    }
    assert!(XfsMemoryQuota::new().validate_active_root(&dir.0, true).is_err());
}

// ---- Against the Python agent's MemoryBackingStore on the same files. ----

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}

const PYTHON_PRELUDE: &str = r#"
import json, sys
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ucloud_sandboxes.checkpoint_components import MemoryBackingRef
from ucloud_sandboxes.memory_backing import MemoryBackingStore

class Quota:
    def validate_root(self, root): pass
    def provision(self, root, path, project_id, quota_bytes): pass
    def validate_project(self, path, project_id): pass
    def release(self, root, project_id): pass

a = json.loads(sys.argv[1])

def store():
    with patch("ucloud_sandboxes.memory_backing.subprocess.run",
               return_value=SimpleNamespace(stdout="tmpfs rw,noswap\n")):
        return MemoryBackingStore(Path(a["root"]), Path(a["journal"]),
                                  hard_capacity_bytes=a["capacity"], quota=Quota(),
                                  active_root=Path(a["active"]) if a["active"] else None)

def lease(value):
    return {"project_id": value.project_id, "path": str(value.path), "active_mode": value.active_mode}
"#;

/// Runs `body` after the prelude; it prints one JSON value. `None` (and the
/// test passes vacuously) when the repo's virtualenv is absent.
fn python(node: &Node, body: &str) -> Option<Value> {
    let interpreter = repo().join(".venv/bin/python");
    if !interpreter.exists() {
        eprintln!("skipping: {} is absent", interpreter.display());
        return None;
    }
    let output = Command::new(interpreter)
        .current_dir(repo())
        .arg("-c")
        .arg(format!("{PYTHON_PRELUDE}\n{body}"))
        .arg(node.python_args().to_string())
        .output()
        .unwrap();
    assert!(output.status.success(), "python failed: {}", String::from_utf8_lossy(&output.stderr));
    Some(serde_json::from_slice(&output.stdout).unwrap())
}

/// Everything about the journal's shape that a reader depends on.
fn schema(journal: &Path) -> Value {
    let conn = Connection::open(journal).unwrap();
    let mut statement = conn.prepare("SELECT type,name,tbl_name,rootpage,sql FROM sqlite_master ORDER BY rowid").unwrap();
    let master: Vec<Value> = statement
        .query_map([], |r| {
            Ok(json!([r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?, r.get::<_, i64>(3)?, r.get::<_, Option<String>>(4)?]))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
    let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0)).unwrap();
    let mut statement = conn.prepare("SELECT * FROM allocations ORDER BY allocation_id").unwrap();
    let rows: Vec<Value> = statement
        .query_map([], |r| {
            (0..8).map(|i| r.get::<_, SqlValue>(i).map(|v| format!("{v:?}"))).collect::<rusqlite::Result<Vec<_>>>().map(Value::from)
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    let counter: Vec<i64> = conn.prepare("SELECT value FROM counter").unwrap().query_map([], |r| r.get(0)).unwrap().map(|v| v.unwrap()).collect();
    json!({"master": master, "user_version": version, "journal_mode": journal_mode, "allocations": rows, "counter": counter})
}

#[test]
fn a_fresh_journal_has_pythons_exact_schema() {
    let ours = Node::new(1 << 20, true);
    let theirs = Node::new(1 << 20, true);
    drop(ours.open(&FakeQuota::default()).unwrap());
    let Some(_) = python(&theirs, "store(); print('null')") else { return };
    let schema_ours = schema(&ours.journal());
    assert_eq!(schema_ours, schema(&theirs.journal()));
    assert_eq!(schema_ours["user_version"], 3);
    assert_eq!(schema_ours["counter"], json!([600000]));
    assert_eq!(schema_ours["journal_mode"], "wal");
}

#[test]
fn a_legacy_journal_migrates_as_python_migrates_it() {
    let ours = Node::new(1 << 20, true);
    let theirs = Node::new(1 << 20, true);
    for node in [&ours, &theirs] {
        fs::create_dir_all(node.journal().parent().unwrap()).unwrap();
        // The pre-placement six-column layout, with a ready row and a used counter.
        node.sql()
            .execute_batch(
                "CREATE TABLE allocations (allocation_id TEXT PRIMARY KEY, sandbox_id TEXT NOT NULL, \
                 generation INTEGER NOT NULL, project_id INTEGER UNIQUE NOT NULL, \
                 quota_bytes INTEGER NOT NULL, state TEXT NOT NULL);
                 CREATE TABLE counter (value INTEGER NOT NULL);
                 INSERT INTO counter VALUES (600001);
                 INSERT INTO allocations VALUES ('guest.sandbox-1','guest',1,600000,4096,'ready');",
            )
            .unwrap();
    }
    let store = ours.open(&FakeQuota::default()).unwrap();
    assert_eq!(store.active_mode("guest", 1), Some(ActiveMode::Ram));
    let Some(_) = python(&theirs, "store(); print('null')") else { return };
    let migrated = schema(&ours.journal());
    assert_eq!(migrated, schema(&theirs.journal()));
    assert_eq!(migrated["user_version"], 3);
    assert_eq!(migrated["counter"], json!([600001]));
    assert_eq!(
        migrated["allocations"],
        json!([["Text(\"guest.sandbox-1\")", "Text(\"guest\")", "Integer(1)", "Integer(600000)", "Integer(4096)", "Text(\"ready\")", "Text(\"ram\")", "Integer(4096)"]])
    );
}

#[test]
fn python_and_rust_share_allocations_counter_and_capacity() {
    let node = Node::new(16384, true);
    let quota = FakeQuota { strict: true, ..FakeQuota::default() };
    let store = node.open(&quota).unwrap();
    let ours = store.prepare(&reference("sb-1", 8192), "sb-1", 1, Some(4096)).unwrap();

    // Python reads and requires the allocation we made, and makes the next one.
    let Some(seen) = python(
        &node,
        r#"
s = store()
r1 = MemoryBackingRef("sb-1.sandbox-1", 8192)
required = s.require(r1, sandbox_id="sb-1", sandbox_generation=1)
cached = s.active_mode("sb-1", 1)
prepared = s.prepare(MemoryBackingRef("sb-2.sandbox-1", 8192), sandbox_id="sb-2", sandbox_generation=1, limit_bytes=8192)
print(json.dumps({"required": lease(required), "cached": cached, "limit": s.limit_bytes(r1),
                  "prepared": lease(prepared), "metrics": s.metrics()}))
"#,
    ) else {
        return;
    };
    assert_eq!(seen["required"], json!({"project_id": 600000, "path": ours.path, "active_mode": "ram"}));
    assert_eq!((seen["cached"].clone(), seen["limit"].clone()), (json!("ram"), json!(4096)));
    assert_eq!(seen["prepared"]["project_id"], 600001);
    assert_eq!(seen["metrics"], json!({"memory_backing_allocations": 2, "memory_backing_hard_reserved_bytes": 12288}));

    // We require Python's allocation (its projid is real to us only by trust here).
    let r2 = reference("sb-2", 8192);
    assert_eq!(store.active_mode("sb-2", 1), None);
    assert_eq!(message(store.require(&r2, "sb-2", 1)), "project changed");
    let trusting = node.open(&FakeQuota::default()).unwrap();
    let theirs = trusting.require(&r2, "sb-2", 1).unwrap();
    assert_eq!((theirs.project_id, theirs.active_mode), (600001, ActiveMode::Ram));
    assert_eq!(trusting.active_mode("sb-2", 1), Some(ActiveMode::Ram));
    assert_eq!(fs::read(theirs.path.join(MARKER)).unwrap(), marker_bytes(&theirs));
    // Python's capacity charge counts against ours: 4096 + 8192 + 4097 > 16384.
    assert_eq!(message(store.prepare(&reference("sb-3", 8192), "sb-3", 1, Some(4097))), "memory backing hard capacity exhausted");

    // Python deletes our allocation; our prepare reimports it with a new project.
    python(
        &node,
        r#"
store().delete(MemoryBackingRef("sb-1.sandbox-1", 8192), sandbox_id="sb-1", sandbox_generation=1)
print("null")
"#,
    );
    assert!(!ours.path.exists() && !node.ram_root().join("sb-1.sandbox-1").exists());
    let again = store.prepare(&reference("sb-1", 8192), "sb-1", 1, Some(4096)).unwrap();
    assert_eq!((again.project_id, again.active_mode), (600002, ActiveMode::Ram));
    assert_eq!(counter(&node), 600003);
}

#[test]
fn python_and_rust_group_commits_interleave_on_one_journal() {
    // The agent's DurableSqliteBatch and our JournalWriter commit groups into
    // the same file at once: SQLite's locks serialize them, and the shared
    // counter and capacity stay exact.
    let node = Node::new(1 << 30, false);
    let store = Arc::new(node.open(&FakeQuota::default()).unwrap());
    let ours = std::thread::spawn({
        let store = store.clone();
        move || {
            let threads: Vec<_> = (0..16)
                .map(|t| {
                    let store = store.clone();
                    std::thread::spawn(move || {
                        for i in 0..4 {
                            let id = format!("rs-{t}-{i}");
                            store.prepare(&reference(&id, 4096), &id, 1, None).unwrap();
                        }
                    })
                })
                .collect();
            threads.into_iter().for_each(|t| t.join().unwrap());
        }
    });
    let theirs = python(
        &node,
        r#"
from concurrent.futures import ThreadPoolExecutor
s = store()
def prepare(i):
    return s.prepare(MemoryBackingRef(f"py-{i}.sandbox-1", 4096), sandbox_id=f"py-{i}", sandbox_generation=1).project_id
with ThreadPoolExecutor(16) as pool:
    projects = list(pool.map(prepare, range(64)))
print(json.dumps({"projects": projects}))
"#,
    );
    ours.join().unwrap();
    let ready: i64 = node.sql().query_row("SELECT COUNT(*) FROM allocations WHERE state='ready'", [], |r| r.get(0)).unwrap();
    let distinct: i64 = node.sql().query_row("SELECT COUNT(DISTINCT project_id) FROM allocations", [], |r| r.get(0)).unwrap();
    let Some(theirs) = theirs else {
        assert_eq!((ready, distinct, counter(&node)), (64, 64, 600064));
        return;
    };
    assert_eq!(theirs["projects"].as_array().unwrap().len(), 64);
    assert_eq!((ready, distinct, counter(&node)), (128, 128, 600128));
    // Each side requires the other's allocations from the shared rows.
    let trusting = node.open(&FakeQuota::default()).unwrap();
    assert_eq!(trusting.require(&reference("py-7", 4096), "py-7", 1).unwrap().project_id, theirs["projects"][7].as_i64().unwrap());
    let required = python(
        &node,
        r#"
s = store()
print(json.dumps([lease(s.require(MemoryBackingRef(f"rs-{t}-3.sandbox-1", 4096), sandbox_id=f"rs-{t}-3", sandbox_generation=1)) for t in range(16)]))
"#,
    )
    .unwrap();
    assert_eq!(required.as_array().unwrap().len(), 16);
}

// ---- A real XFS prjquota filesystem (root only). ----

/// A sparse image on a direct-I/O loop device, mounted with prjquota, as the
/// node's mount root is; unmounted and detached on drop.
struct LoopXfs {
    dir: TempDir,
    device: String,
}

impl LoopXfs {
    /// `None` (the test skips) unless root with mkfs.xfs, losetup and xfs_quota.
    fn new() -> Option<LoopXfs> {
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } != 0 {
            eprintln!("skipping: not root");
            return None;
        }
        for tool in ["mkfs.xfs", "losetup", "xfs_quota", "mount", "umount"] {
            if !Command::new("sh").args(["-c", &format!("command -v {tool}")]).output().unwrap().status.success() {
                eprintln!("skipping: {tool} is absent");
                return None;
            }
        }
        let dir = temp_dir();
        let image = dir.0.join("xfs.img");
        fs::File::create(&image).unwrap().set_len(512 << 20).unwrap();
        run(Command::new("mkfs.xfs").arg("-q").arg(&image));
        let device = run(Command::new("losetup").args(["--direct-io=on", "-f", "--show"]).arg(&image)).trim().to_string();
        let mounted = LoopXfs { dir, device };
        fs::create_dir(mounted.mountpoint()).unwrap();
        run(Command::new("mount").args(["-o", "prjquota", &mounted.device]).arg(mounted.mountpoint()));
        Some(mounted)
    }

    fn mountpoint(&self) -> PathBuf {
        self.dir.0.join("mnt")
    }

    fn store(&self, name: &str, backend: QuotaBackend) -> MemoryBackingStore {
        let config = MemoryBackingConfig {
            root: self.mountpoint().join(name),
            journal: self.dir.0.join(format!("{name}.sqlite")),
            hard_capacity_bytes: 1 << 30,
            active_root: None,
            ram_swappable: false,
        };
        MemoryBackingStore::open(config, Box::new(XfsMemoryQuota::with_backend(backend))).unwrap()
    }

    /// `(soft, hard)` KiB block limits from `xfs_quota report`.
    fn limits(&self, project_id: i64) -> (u64, u64) {
        let report = run(Command::new("xfs_quota").args(["-x", "-c", "report -p -N -b -n"]).arg(self.mountpoint()));
        let line = report
            .lines()
            .find(|line| line.split_whitespace().next() == Some(&format!("#{project_id}")))
            .unwrap_or_else(|| panic!("project {project_id} not reported:\n{report}"));
        let fields: Vec<u64> = line.split_whitespace().skip(1).take(3).map(|f| f.parse().unwrap()).collect();
        (fields[1], fields[2])
    }
}

impl Drop for LoopXfs {
    fn drop(&mut self) {
        let _ = Command::new("umount").arg(self.mountpoint()).status();
        let _ = Command::new("losetup").args(["-d", &self.device]).status();
    }
}

fn run(command: &mut Command) -> String {
    let output = command.output().unwrap();
    assert!(output.status.success(), "{command:?}: {}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Project and PROJINHERIT of a path, as the kernel holds them.
fn project_of(path: &Path) -> (i64, bool) {
    let (flags, projid) = fsxattr(&fs::File::open(path).unwrap()).unwrap();
    (i64::from(projid), flags & FS_XFLAG_PROJINHERIT != 0)
}

#[test]
fn syscalls_provision_exactly_what_xfs_quota_does() {
    let Some(xfs) = LoopXfs::new() else { return };
    let mut probe = XfsMemoryQuota::new();
    fs::DirBuilder::new().mode(0o700).create(xfs.mountpoint().join("probe")).unwrap();
    probe.validate_root(&xfs.mountpoint().join("probe")).unwrap();
    assert_eq!(probe.block_device().unwrap().to_str().unwrap(), xfs.device, "quotactl must address the loop device");

    let syscalls = xfs.store("syscalls", QuotaBackend::Syscalls);
    let processes = xfs.store("processes", QuotaBackend::XfsQuota);
    // Projects are the filesystem's: keep the two journals' ids apart.
    Connection::open(xfs.dir.0.join("processes.sqlite")).unwrap().execute_batch("UPDATE counter SET value=700000").unwrap();
    let mut leases = Vec::new();
    for (store, id) in [(&syscalls, "sys"), (&processes, "proc")] {
        let r = MemoryBackingRef::new(format!("{id}.sandbox-1"), 64 << 20).unwrap();
        // A limit that is not a whole number of basic blocks rounds down.
        let lease = store.prepare(&r, id, 1, Some((16 << 20) + 1000)).unwrap();
        assert_eq!(store.require(&r, id, 1).unwrap(), lease);
        leases.push(lease);
    }
    let (ours, theirs) = (&leases[0], &leases[1]);
    assert_eq!((ours.project_id, theirs.project_id), (600000, 700000));
    // Same kernel state: directory and marker in the project, PROJINHERIT
    // on the directory, whatever XFS keeps on the marker.
    for lease in [ours, theirs] {
        assert_eq!(project_of(&lease.path), (lease.project_id, true));
        assert_eq!(project_of(&lease.path.join(MARKER)).0, lease.project_id);
    }
    assert_eq!(project_of(&ours.path.join(MARKER)).1, project_of(&theirs.path.join(MARKER)).1);
    // Same limits: bsoft = bhard = the limit in basic blocks, which XFS
    // rounds up to its 4 KiB blocks (16 MiB + 1000 B → 16388 KiB).
    assert_eq!(xfs.limits(ours.project_id), (16388, 16388));
    assert_eq!(xfs.limits(theirs.project_id), xfs.limits(ours.project_id));

    // The agent's own XfsMemoryQuota accepts the syscall-provisioned directory.
    let interpreter = repo().join(".venv/bin/python");
    if interpreter.exists() {
        let check = "import sys; from pathlib import Path; from ucloud_sandboxes.memory_backing import XfsMemoryQuota; \
                     XfsMemoryQuota().validate_project(Path(sys.argv[1]), int(sys.argv[2])); print('ok')";
        let output = run(Command::new(interpreter).current_dir(repo()).args(["-c", check]).arg(&ours.path).arg(ours.project_id.to_string()));
        assert_eq!(output.trim(), "ok");
    }

    // A second project, then enforcement: XFS fails a write past a project
    // limit with ENOSPC (EDQUOT is for user and group quotas).
    let r = MemoryBackingRef::new("big.sandbox-1", 64 << 20).unwrap();
    let lease = syscalls.prepare(&r, "big", 1, Some(4 << 20)).unwrap();
    assert_eq!(xfs.limits(lease.project_id), (4096, 4096));
    let mut file = fs::File::create(lease.path.join("memory")).unwrap();
    let chunk = vec![7u8; 1 << 20];
    let error = (0..8).try_for_each(|_| std::io::Write::write_all(&mut file, &chunk).and_then(|()| file.sync_all())).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::ENOSPC));
}

/// Per-prepare cost on a real XFS prjquota filesystem: as root, set
/// `UCLOUD_NODED_XFS_ROOT` to a directory on one and run with
/// `--ignored --nocapture`. `UCLOUD_NODED_BENCH_COUNT` prepares per
/// concurrency; `UCLOUD_NODED_QUOTA_BACKEND=xfs_quota` for the processes.
#[test]
#[ignore]
fn xfs_prepare_cost() {
    let base = PathBuf::from(std::env::var("UCLOUD_NODED_XFS_ROOT").expect("UCLOUD_NODED_XFS_ROOT"));
    let count: usize = std::env::var("UCLOUD_NODED_BENCH_COUNT").map(|v| v.parse().unwrap()).unwrap_or(256);
    let backend = match std::env::var("UCLOUD_NODED_QUOTA_BACKEND").as_deref() {
        Ok("xfs_quota") => QuotaBackend::XfsQuota,
        _ => QuotaBackend::Syscalls,
    };
    for concurrency in [1usize, 8, 32, 64] {
        let state = temp_dir();
        let config = MemoryBackingConfig {
            root: base.join(format!("bench-{}-{concurrency}", std::process::id())),
            journal: state.0.join("memory-backing.sqlite"),
            hard_capacity_bytes: 1 << 50,
            active_root: None,
            ram_swappable: false,
        };
        let store = Arc::new(MemoryBackingStore::open(config, Box::new(XfsMemoryQuota::with_backend(backend))).unwrap());
        let next = Arc::new(AtomicUsize::new(0));
        let started = std::time::Instant::now();
        let threads: Vec<_> = (0..concurrency)
            .map(|_| {
                let (store, next) = (store.clone(), next.clone());
                std::thread::spawn(move || {
                    let mut latencies = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::SeqCst);
                        if i >= count {
                            return latencies;
                        }
                        let id = format!("bench-{i}");
                        let begun = std::time::Instant::now();
                        store.prepare(&reference(&id, 64 << 20), &id, 1, None).unwrap();
                        latencies.push(begun.elapsed().as_secs_f64() * 1000.0);
                    }
                })
            })
            .collect();
        let mut latencies: Vec<f64> = threads.into_iter().flat_map(|t| t.join().unwrap()).collect();
        let wall = started.elapsed().as_secs_f64();
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let at = |q: f64| latencies[((latencies.len() as f64 * q) as usize).min(latencies.len() - 1)];
        println!(
            "{backend:?} concurrency {concurrency:>2}: {count} prepares in {wall:.2} s ({:.0}/s); mean {:.1} ms, p50 {:.1} ms, p95 {:.1} ms, max {:.1} ms; {} commits",
            count as f64 / wall,
            latencies.iter().sum::<f64>() / latencies.len() as f64,
            at(0.5),
            at(0.95),
            at(1.0),
            store.journal_commits(),
        );
    }
}
