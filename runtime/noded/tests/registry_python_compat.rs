//! Cross-language compatibility of the node registry: Rust and the
//! repository's Python (`ucloud_sandboxes.direct_registry`, the oracle) write,
//! read and take over the same `direct-registry.sqlite`, and must agree byte
//! for byte on every record. Skipped when the repository's virtualenv is
//! absent; `UCLOUD_SANDBOXES_PYTHON` overrides the interpreter.

use std::collections::BTreeMap;
use std::io::Write;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde_json::{Value, json};
use ucloud_noded::registry::{
    DiskClaim, DrainState, GrowthAction, Migration, Phase, PlanRequest, Quota, Registry, Rootfs, SandboxSpec,
};

const COMPAT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}

fn python() -> Option<PathBuf> {
    let interpreter = std::env::var_os("UCLOUD_SANDBOXES_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|| repository().join(".venv/bin/python"));
    if interpreter.exists() {
        Some(interpreter)
    } else {
        eprintln!("skipping: no Python at {}", interpreter.display());
        None
    }
}

/// Run `script` with `input` as JSON on stdin; it prints one JSON value.
fn run_python(interpreter: &Path, script: &str, input: Value) -> Value {
    let mut child = Command::new(interpreter)
        .arg("-c")
        .arg(format!("{PRELUDE}\n{script}"))
        .env("PYTHONPATH", repository())
        .current_dir(repository())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input.to_string().as_bytes()).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "python failed:\n{}", String::from_utf8_lossy(&output.stderr));
    serde_json::from_slice(&output.stdout).unwrap()
}

const PRELUDE: &str = r#"
import json, sys
from pathlib import Path
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry, DirectRegistryError, DiskClaim
from ucloud_sandboxes.direct_warden import DirectSandbox
from ucloud_sandboxes.sandbox import NodeDrainState, SandboxSpec, sandbox_spec_fingerprint

args = json.loads(sys.stdin.read())

def state(registry):
    snapshot = registry.snapshot()
    return {
        "records": {r.sandbox_id: DirectSandboxRegistry._encode(r) for r in snapshot.records},
        "spec_sha256": {r.sandbox_id: r.spec_sha256 for r in snapshot.records},
        "activity_revision": registry.activity_revision(),
        "claims": sorted([k[0], k[1], v] for k, v in registry.disk_claims_mb().items()),
        "dynamic": {r.sandbox_id: (lambda c: None if c is None else [c.workspace_mb, c.memory_mb])(
            registry.disk_claim(r.sandbox_id, r.sandbox_generation)) for r in snapshot.records},
        "drain": registry.load_drain().to_dict(),
        "growth": [list(vars(i).values()) for i in registry.growth_intents()],
        "overlaps": [list(vars(o).values()) for o in registry.list_reflink_overlaps()],
    }

def try_owner(path):
    try:
        DirectSandboxRegistry(path, owner=True).close()
        return "acquired"
    except DirectRegistryError as exc:
        return str(exc)
"#;

/// Everything both implementations must agree on, in the Python script's shape.
fn rust_state(registry: &Registry) -> Value {
    let snapshot = registry.snapshot().unwrap();
    let mut claims: Vec<(String, i64, i64)> =
        registry.disk_claims_mb().unwrap().into_iter().map(|((id, generation), mb)| (id, generation, mb)).collect();
    claims.sort();
    let drain = registry.load_drain().unwrap();
    json!({
        "records": snapshot.records.iter().map(|r| (r.sandbox_id().to_owned(), Value::String(r.encode()))).collect::<serde_json::Map<_, _>>(),
        "spec_sha256": snapshot.records.iter().map(|r| (r.sandbox_id().to_owned(), Value::String(r.spec_sha256()))).collect::<serde_json::Map<_, _>>(),
        "activity_revision": snapshot.activity_revision,
        "claims": claims.into_iter().map(|(id, generation, mb)| json!([id, generation, mb])).collect::<Vec<_>>(),
        "dynamic": snapshot.records.iter().map(|r| {
            let claim = registry.disk_claim(r.sandbox_id(), r.sandbox_generation).unwrap();
            (r.sandbox_id().to_owned(), claim.map_or(Value::Null, |c| json!([c.workspace_mb, c.memory_mb])))
        }).collect::<serde_json::Map<_, _>>(),
        "drain": drain.to_dict(),
        "growth": registry.growth_intents().unwrap().into_iter().map(|i| json!([i.sandbox_id, i.generation, i.job_id, i.launch_sha256, i.memory_bytes, i.phase, i.request_id])).collect::<Vec<_>>(),
        "overlaps": registry.list_reflink_overlaps(None).unwrap().into_iter().map(|o| json!([o.sandbox_id, o.sandbox_generation, o.hibernation_generation, o.allocated_bytes, o.manifest_sha256])).collect::<Vec<_>>(),
    })
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let path = std::env::temp_dir().join(format!("noded-registry-compat-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        TempDir(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn spec(raw: Value) -> SandboxSpec {
    let spec = SandboxSpec::from_dict(&raw).unwrap();
    spec.validate().unwrap();
    spec
}

fn image_spec(id: &str) -> Value {
    json!({"id": id, "image": format!("registry/image@sha256:{}", "a".repeat(64)), "memory_mb": 1024, "disk_mb": 2048})
}

fn request(spec: SandboxSpec, generation: i64) -> PlanRequest {
    PlanRequest {
        spec,
        sandbox_generation: generation,
        operation_id: format!("create:{generation}"),
        runtime_compatibility_sha256: COMPAT.into(),
        split_memory_backing: false,
        initial_claim: None,
    }
}

fn quota(root: &Path, id: &str, total_mb: i64) -> Quota {
    Quota { project_id: 200_001, total_mb, path: root.join("quota").join(id).display().to_string() }
}

fn rootfs(root: &Path, id: &str, generation: i64) -> Rootfs {
    Rootfs {
        rootfs_sha256: "d".repeat(64),
        container_id: format!("{generation:064x}"),
        bundle: root.join("bundles").join(id).display().to_string(),
        memory_directory: format!("{id}.{generation}"),
    }
}

fn image_id() -> String {
    format!("sha256:{}", "e".repeat(64))
}

#[test]
fn spec_canonical_form_matches_python() {
    let Some(interpreter) = python() else { return };
    let corpus = json!([
        {"id": "c1", "image": "img", "memory_mb": 512},
        {"id": "demo-1", "image": "registry.example/img:1", "memory_mb": 512, "cpus": 1, "disk_mb": 1024,
         "env": {"A": "ä"}, "parkable": true, "managed_process": true},
        {"id": "f", "image": "i", "cpus": 0.5},
        {"id": "f", "image": "i", "cpus": 1e-5},
        {"id": "f", "image": "i", "cpus": 1e16},
        {"id": "f", "image": "i", "cpus": 0.1, "ttl_seconds": 60, "working_dir": "/work"},
        {"id": "u", "image": "i", "memory_mb": 1,
         "env": {"Z": "é\u{7f}\t/\"\\ 😀", "a": "\u{0}", "_X": "ｦ"},
         "labels": {"é": "1", "😀": "2", "\u{ff00}": "3", "Z": "4", "a": "\u{2028}"},
         "command": ["sh", "-c", "echo 'ünï'"]},
        {"id": "r", "image": "i", "memory_mb": 1, "network_policy": {"egress": "relay", "relay": "default"},
         "required_features": ["gpu"], "environment_root": format!("sha256:{}", "c".repeat(64))},
        {"id": "t", "image": "i", "memory_mb": 1, "environment_root": format!("sha256:{}", "c".repeat(64)),
         "toolkits": [format!("vf-harness@sha256:{}", "d".repeat(64))]},
        {"id": "t", "image": "i", "memory_mb": 1, "toolkits": []},
        {"id": "d", "image": "i", "memory_mb": 1, "network_policy": {"egress": "direct"}, "dns_servers": ["1.1.1.1"],
         "required_features": [], "environment_root": null, "ssh": {"enabled": true, "host_port": 2222,
         "authorized_keys": ["ssh-ed25519 AAAA"]}},
        {"id": "s", "image": "i", "memory_mb": 1, "security": {"user": "", "cap_add": ["NET_ADMIN"],
         "supplementary_groups": ["44"], "pids_limit": null, "read_only_rootfs": true},
         "filesystem": {"shm_mb": 128, "management_helper": "static", "workspace_storage": "tmpfs", "tmpfs_mb": 32}},
        {"id": "s", "image": "i", "memory_mb": 1, "security": null, "filesystem": null, "linux_host": null, "ssh": null,
         "filesystem": {"shm_mb": 64, "management_helper": "shell", "workspace_storage": null}},
        {"id": "h", "image": "i", "memory_mb": 1, "profile": "linux_host", "security": {"pids_limit": 64},
         "filesystem": {"workspace_path": "/srv/work"}},
        {"id": "h", "image": "i", "memory_mb": 1, "profile": "linux_host", "security": null},
        {"id": "l", "image": "i", "memory_mb": 1, "profile": "linux_session", "linux_host": {"enable_sshd": true}},
        {"id": "l", "image": "i", "memory_mb": 1, "profile": "linux_session", "linux_host": {"writable_paths": ["/a"]}},
        {"id": "x\n", "image": "i", "memory_mb": 1},
        {"id": "x", "image": "i", "memory_mb": 1, "labels": {"UCLOUD-SANDBOXES.k": "v"}},
        {"id": "x", "image": "i", "memory_mb": 1, "env": {"1A": "v"}},
        {"id": "x", "image": "i", "memory_mb": 1, "dns_servers": ["1.2.03.4"]},
        {"id": "x", "image": "i", "memory_mb": 1, "dns_servers": ["1.2.3"]},
        {"id": "x", "image": "i", "memory_mb": 1, "filesystem": {"workspace_path": "/proc/x"}},
        {"id": "x", "image": "i", "parkable": true, "memory_mb": 1},
        {"id": "x", "image": "i"},
        {"id": "x", "image": "i", "memory_mb": 1, "network": "host"},
        {"zeta": 1, "alpha": 2},
        {"id": "x", "memory_mb": 512.0},
        {"id": "x", "cpus": true},
        {"id": "x", "network_policy": {"egress": "relay", "relay": "Bad"}},
        {"id": "x", "network_policy": {"egress": "sideways"}},
        {"id": "x", "network_policy": null},
        {"id": "x", "security": {"user": 5}},
        {"id": "x", "filesystem": []},
        {"id": "x", "ssh": {"port": 1}},
    ]);
    let python = run_python(
        &interpreter,
        r#"
out = []
for raw in args:
    try:
        spec = SandboxSpec.from_dict(raw)
    except Exception as exc:
        out.append({"error": str(exc)})
        continue
    try:
        spec.validate()
        valid = None
    except Exception as exc:
        valid = str(exc)
    try:
        disk = spec.requested_resources().disk_mb
    except Exception as exc:
        disk = str(exc)
    out.append({"encoded": json.dumps(spec.to_dict(), sort_keys=True, separators=(",", ":")),
                "sha256": sandbox_spec_fingerprint(spec), "validate": valid, "disk": disk})
print(json.dumps(out))
"#,
        corpus.clone(),
    );
    for (raw, expected) in corpus.as_array().unwrap().iter().zip(python.as_array().unwrap()) {
        let rust = match SandboxSpec::from_dict(raw) {
            Err(error) => json!({"error": error}),
            Ok(spec) => {
                // The environment catalog check is Python's alone.
                let validate = match spec.validate() {
                    Ok(())
                        if raw
                            .get("required_features")
                            .is_some_and(|f| f.as_array().is_some_and(|f| !f.is_empty())) =>
                    {
                        expected["validate"].clone()
                    }
                    Ok(()) => Value::Null,
                    Err(error) => json!(error),
                };
                let disk = match spec.requested_disk_mb() {
                    Ok(disk) => json!(disk),
                    Err(error) => json!(error),
                };
                json!({"encoded": spec.canonical_json(), "sha256": spec.sha256(), "validate": validate, "disk": disk})
            }
        };
        assert_eq!(&rust, expected, "spec {raw}");
        if let Ok(spec) = SandboxSpec::from_dict(raw) {
            // The stored form is a fixed point.
            assert_eq!(SandboxSpec::from_dict(spec.to_dict()).unwrap().canonical_json(), spec.canonical_json());
        }
    }
}

#[test]
fn rust_writes_and_python_reads_owns_and_continues() {
    let Some(interpreter) = python() else { return };
    let dir = TempDir::new("rust-first");
    let path = dir.0.join("direct-registry.sqlite");
    let root = &dir.0;
    let owner = Registry::owner(&path, 100_000).unwrap();
    owner.bind_runtime_compatibility(COMPAT).unwrap();
    // a: planned; b: owned (v3); c: split, dynamic, rootfs_ready; d: importing;
    // e: deleted; f: owned linux_host with non-ASCII text and a float cpus.
    owner.plan(request(spec(image_spec("a")), 1)).unwrap();
    let b = owner.plan(request(spec(image_spec("b")), 2)).unwrap();
    let b = owner
        .commit_rootfs("b", b.revision, &image_id(), &rootfs(root, "b", 2), Some(&quota(root, "b", 4096)))
        .unwrap();
    owner.commit_owned("b", b.revision).unwrap();
    let parkable = json!({"id": "c", "image": "i", "memory_mb": 1024, "disk_mb": 2048, "parkable": true, "cpus": 2,
        "network_policy": {"egress": "relay", "relay": "default"}});
    let c = owner
        .plan(PlanRequest {
            split_memory_backing: true,
            initial_claim: Some(DiskClaim::new(512, 64).unwrap()),
            ..request(spec(parkable), 3)
        })
        .unwrap();
    owner.commit_rootfs("c", c.revision, &image_id(), &rootfs(root, "c", 3), Some(&quota(root, "c", 5184))).unwrap();
    owner.update_disk_claim("c", 3, Some(700), None, true, false).unwrap();
    let migration = Migration { id: "move:9".into(), sha256: "9".repeat(64) };
    let d = owner.plan_import(spec(image_spec("d")), 4, "import:4".into(), COMPAT.into(), migration, false).unwrap();
    owner.commit_import_quota("d", d.revision, &quota(root, "d", 4096)).unwrap();
    let e = owner.plan(request(spec(image_spec("e")), 5)).unwrap();
    let e = owner.begin_delete("e", e.revision, Some(5)).unwrap();
    owner.commit_deleted("e", 5, e.revision).unwrap();
    let unicode = json!({"id": "f", "image": "i", "memory_mb": 1024, "cpus": 0.5, "profile": "linux_host",
        "env": {"GREETING": "grüß dich 😀", "Z": "\u{7f}"}, "labels": {"ключ": "値", "a": "\u{2028}"}});
    let f = owner.plan(request(spec(unicode), 6)).unwrap();
    let f = owner
        .commit_rootfs("f", f.revision, &image_id(), &rootfs(root, "f", 6), Some(&quota(root, "f", 2048)))
        .unwrap();
    owner.commit_owned("f", f.revision).unwrap();
    owner.growth_intent("f", 6, GrowthAction::Launch, "job-1", &"1".repeat(64), "").unwrap();
    owner.growth_intent("f", 6, GrowthAction::Activate, "job-1", &"1".repeat(64), "req-1").unwrap();
    owner.reserve_reflink_overlap("f", 6, 1, 4096, &"7".repeat(64)).unwrap();
    owner.reserve_workspace_for_mount("f", 6).unwrap();
    assert!(owner.release_published_workspace("f", 6, 100, 1).unwrap());
    owner
        .save_drain(&DrainState {
            draining: true, token: "tök".into(), drain_activity_epoch: 7, admission_open: false
        })
        .unwrap();
    let written = rust_state(&owner);

    // Python reads the live owner's file, and its owner lock excludes Python's.
    let read = run_python(
        &interpreter,
        r#"
path = Path(args["path"])
registry = DirectSandboxRegistry(path)
cached = DirectSandboxRegistry(path, cached_reads=True)
print(json.dumps({"state": state(registry), "cached": state(cached), "owner": try_owner(path)}))
"#,
        json!({"path": path}),
    );
    assert_eq!(read["state"], written);
    assert_eq!(read["cached"], written);
    assert_eq!(read["owner"], "direct registry has another live owner");

    // Python takes ownership and continues the state machine.
    owner.close();
    let continued = run_python(
        &interpreter,
        r#"
path = Path(args["path"])
registry = DirectSandboxRegistry(path, hard_disk_capacity_mb=100000, owner=True)
before = state(registry)
c = registry.get("c")
registry.commit_owned("c", expected_revision=c.revision)
a = registry.get("a")
deleting = registry.begin_delete("a", expected_revision=a.revision)
registry.commit_deleted("a", sandbox_generation=1, expected_revision=deleting.revision)
registry.plan(spec=SandboxSpec.from_dict({"id": "g", "image": "i", "memory_mb": 64, "cpus": 3}),
              sandbox_generation=1, operation_id="create:g", runtime_compatibility_sha256="b" * 64)
registry.growth_intent("f", 6, action="park")
after = state(registry)
registry.close()
print(json.dumps({"before": before, "after": after}))
"#,
        json!({"path": path}),
    );
    assert_eq!(continued["before"], written);

    // Rust takes ownership back, reads Python's writes and continues.
    let owner = Registry::owner(&path, 100_000).unwrap();
    assert_eq!(rust_state(&owner), continued["after"]);
    assert_eq!(owner.get("c").unwrap().unwrap().phase, Phase::Owned);
    assert!(owner.get("g").unwrap().unwrap().spec.canonical_json().contains(r#""cpus":3.0"#));
    let tombstoned = owner.plan(request(spec(image_spec("a")), 1)).unwrap_err();
    assert_eq!(tombstoned.message(), "direct registration is fenced by a tombstone");
    let g = owner.get("g").unwrap().unwrap();
    let g = owner.begin_delete("g", g.revision, None).unwrap();
    owner.commit_deleted("g", 1, g.revision).unwrap();
    let last = rust_state(&owner);
    let reread = run_python(
        &interpreter,
        "print(json.dumps(state(DirectSandboxRegistry(Path(args['path'])))))",
        json!({"path": path}),
    );
    assert_eq!(reread, last);
}

#[test]
fn python_writes_and_rust_reads_and_continues() {
    let Some(interpreter) = python() else { return };
    let dir = TempDir::new("python-first");
    let path = dir.0.join("direct-registry.sqlite");
    let written = run_python(
        &interpreter,
        r#"
path = Path(args["path"])
root = Path(args["root"])
registry = DirectSandboxRegistry(path, hard_disk_capacity_mb=100000, owner=True)
registry.bind_runtime_compatibility("b" * 64)

def spec(raw):
    return SandboxSpec.from_dict(raw)

def sandbox(name, generation, record):
    return DirectSandbox(sandbox_id=name, sandbox_generation=generation, container_id=f"{generation:064x}",
                         spec_sha256=record.spec_sha256, rootfs_sha256="d" * 64,
                         bundle=root / "bundles" / name, memory_directory=f"{name}.{generation}")

image = "sha256:" + "e" * 64
p1 = registry.plan(spec=spec({"id": "p1", "image": "i", "memory_mb": 512, "cpus": 1.5,
                              "env": {"NAME": "Jürgen ☃"}, "labels": {"€": "x"}}),
                   sandbox_generation=1, operation_id="create:1", runtime_compatibility_sha256="b" * 64)
p1 = registry.commit_rootfs("p1", expected_revision=p1.revision, image_id=image, sandbox=sandbox("p1", 1, p1),
                            quota=(200001, 1024, root / "quota" / "p1"))
p2 = registry.plan(spec=spec({"id": "p2", "image": "i", "memory_mb": 1024, "disk_mb": 2048, "parkable": True,
                              "managed_process": True, "cpus": 1e-05}),
                   sandbox_generation=2, operation_id="create:2", runtime_compatibility_sha256="b" * 64,
                   split_memory_backing=True, initial_claim=DiskClaim(256, 128))
p3 = registry.plan_import(spec=spec({"id": "p3", "image": "i", "memory_mb": 64, "profile": "linux_session"}),
                          sandbox_generation=3, operation_id="import:3", runtime_compatibility_sha256="b" * 64,
                          migration_id="move:3", migration_sha256="3" * 64)
p3 = registry.commit_import_quota("p3", expected_revision=p3.revision, project_id=200003, total_mb=512,
                                  quota_path=root / "quota" / "p3")
p4 = registry.plan(spec=spec({"id": "p4", "image": "i", "memory_mb": 256, "cpus": 1e16}),
                   sandbox_generation=4, operation_id="create:4", runtime_compatibility_sha256="b" * 64)
p4 = registry.commit_rootfs("p4", expected_revision=p4.revision, image_id=image, sandbox=sandbox("p4", 4, p4),
                            quota=(200004, 1024, root / "quota" / "p4"))
p4 = registry.commit_owned("p4", expected_revision=p4.revision)
registry.growth_intent("p4", 4, action="launch", job_id="job", launch_sha256="4" * 64)
registry.relay_wake_fence("p4", 4, "req-4", record=True)
registry.save_drain(NodeDrainState(draining=False, token="", drain_activity_epoch=11, admission_open=True))
out = state(registry)
registry.close()
print(json.dumps(out))
"#,
        json!({"path": path, "root": dir.0}),
    );
    // Rust validates Python's schema and decodes every row identically.
    let reader = Registry::new(&path, 100_000).unwrap();
    assert_eq!(rust_state(&reader), written);
    let owner = Registry::owner(&path, 100_000).unwrap();
    assert_eq!(rust_state(&owner), written);
    assert_eq!(
        owner.snapshot().unwrap().records.iter().map(|r| r.phase).collect::<Vec<_>>(),
        [Phase::RootfsReady, Phase::Planned, Phase::Importing, Phase::Owned]
    );
    let p1 = owner.get("p1").unwrap().unwrap();
    owner.commit_owned("p1", p1.revision).unwrap();
    let p2 = owner.get("p2").unwrap().unwrap();
    assert_eq!(owner.disk_claim("p2", 2).unwrap(), Some(DiskClaim { workspace_mb: 256, memory_mb: 128 }));
    owner.update_disk_claim("p2", 2, None, Some(512), true, false).unwrap();
    owner
        .commit_rootfs("p2", p2.revision, &image_id(), &rootfs(&dir.0, "p2", 2), Some(&quota(&dir.0, "p2", 5184)))
        .unwrap();
    let p3 = owner.get("p3").unwrap().unwrap();
    owner.commit_import_rootfs("p3", p3.revision, &image_id(), &rootfs(&dir.0, "p3", 3)).unwrap();
    assert!(owner.relay_wake_fence("p4", 4, "req-4", false).unwrap());
    let p4 = owner.get("p4").unwrap().unwrap();
    let p4 = owner.begin_delete("p4", p4.revision, Some(4)).unwrap();
    owner.commit_deleted("p4", 4, p4.revision).unwrap();
    assert!(owner.growth_intents().unwrap().is_empty());
    let expected = rust_state(&owner);
    drop(owner);
    let reread = run_python(
        &interpreter,
        r#"
path = Path(args["path"])
registry = DirectSandboxRegistry(path, owner=True)
out = state(registry)
try:
    registry.plan(spec=SandboxSpec.from_dict({"id": "p4", "image": "i", "memory_mb": 256, "cpus": 1e16}),
                  sandbox_generation=4, operation_id="create:4", runtime_compatibility_sha256="b" * 64)
    out["replanned"] = "planned"
except DirectRegistryError as exc:
    out["replanned"] = str(exc)
print(json.dumps(out))
"#,
        json!({"path": path}),
    );
    let mut reread = reread.as_object().unwrap().clone();
    assert_eq!(reread.remove("replanned").unwrap(), "direct registration is fenced by a tombstone");
    assert_eq!(Value::Object(reread), expected);
    let phases: BTreeMap<String, Phase> =
        Registry::new(&path, 0).unwrap().list().unwrap().iter().map(|r| (r.sandbox_id().to_owned(), r.phase)).collect();
    assert_eq!(
        phases,
        BTreeMap::from([
            ("p1".into(), Phase::Owned),
            ("p2".into(), Phase::RootfsReady),
            ("p3".into(), Phase::RootfsReady)
        ])
    );
}

#[test]
fn python_foreign_writer_interleaves_with_the_rust_owner() {
    let Some(interpreter) = python() else { return };
    let dir = TempDir::new("interleave");
    let path = dir.0.join("direct-registry.sqlite");
    let owner = Registry::owner(&path, 0).unwrap();
    let script = r#"
path = Path(args["path"])
# The node agent's mode while the daemon owns the file.
registry = DirectSandboxRegistry(path, cached_reads=True)
for index in range(16):
    name = f"py{index}"
    planned = registry.plan(spec=SandboxSpec.from_dict({"id": name, "image": "i", "memory_mb": 64}),
                            sandbox_generation=1, operation_id="create:1", runtime_compatibility_sha256="b" * 64)
    ready = registry.commit_quota(name, expected_revision=planned.revision, project_id=200001, total_mb=64,
                                  quota_path=Path("/q") / name)
    if index % 2:
        deleting = registry.begin_delete(name, expected_revision=ready.revision)
        registry.commit_deleted(name, sandbox_generation=1, expected_revision=deleting.revision)
print(json.dumps(registry.activity_revision()))
"#;
    let done = std::sync::atomic::AtomicBool::new(false);
    let (foreign, created) = std::thread::scope(|scope| {
        let foreign = scope.spawn(|| {
            let revision = run_python(&interpreter, script, json!({"path": path}));
            done.store(true, std::sync::atomic::Ordering::SeqCst);
            revision
        });
        // Write until the foreign process is done, so the two interleave.
        let writers: Vec<_> = (0..4)
            .map(|writer| {
                let (owner, root, done) = (&owner, &dir.0, &done);
                scope.spawn(move || {
                    let mut index = 0;
                    while index < 4 || !done.load(std::sync::atomic::Ordering::SeqCst) {
                        let name = format!("rs{writer}-{index}");
                        let planned = owner.plan(request(spec(image_spec(&name)), 1)).unwrap();
                        owner.commit_quota(&name, planned.revision, &quota(root, &name, 64)).unwrap();
                        let snapshot = owner.snapshot().unwrap();
                        assert!(snapshot.records.iter().all(|r| r.revision <= snapshot.activity_revision));
                        index += 1;
                        // SQLite's busy handler polls: leave the foreign writer gaps.
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    index
                })
            })
            .collect();
        let created: i64 = writers.into_iter().map(|writer| writer.join().unwrap()).sum();
        (foreign.join().unwrap(), created)
    });
    let python_revisions = 16 * 2 + 8 * 2;
    assert!(foreign.as_i64().unwrap() >= python_revisions);
    // The owner's next write proves its index against every foreign commit.
    owner.plan(request(spec(image_spec("last")), 1)).unwrap();
    let snapshot = owner.snapshot().unwrap();
    assert_eq!(snapshot.activity_revision, python_revisions + created * 2 + 1);
    assert_eq!(snapshot.records.len() as i64, 8 + created + 1);
    let journal = run_python(
        &interpreter,
        "print(json.dumps(state(DirectSandboxRegistry(Path(args['path'])))))",
        json!({"path": path}),
    );
    assert_eq!(journal, rust_state(&owner));
}
