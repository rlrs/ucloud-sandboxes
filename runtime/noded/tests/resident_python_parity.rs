//! Phase 3b's gate (docs/admission-port-3c.md, step 3c.0(a)): the daemon's
//! resident sampler against the agent's own (`ucloud_sandboxes.resident_memory.
//! ResidentMemorySampler`, the oracle) on the same fake cgroup trees, and the
//! agent's reader of the daemon's `resident.json` (`PublishedResidentMemory`).
//! Every field must be equal, `sampled_at` aside (each process reads at its
//! own instant, on the same CLOCK_MONOTONIC); what the agent reads back from
//! `resident.json` must equal the daemon's sample exactly, `sampled_at`
//! included. Skipped when the repository's virtualenv is absent;
//! `UCLOUD_SANDBOXES_PYTHON` overrides the interpreter.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

use serde_json::{Value, json};
use ucloud_noded::pause_policy::files::ResidentFile;
use ucloud_noded::pause_policy::resident::{ResidentSample, ResidentSampler};
use ucloud_noded::pause_policy::{Clock, SystemClock};

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

/// Samples every case with Python's sampler, then reads the daemon's
/// `resident.json` back through `PublishedResidentMemory`.
const SCRIPT: &str = r#"
import dataclasses, json, sys, time
from pathlib import Path
from ucloud_sandboxes.resident_memory import PublishedResidentMemory, ResidentMemorySampler

request = json.load(sys.stdin)
sampler = ResidentMemorySampler(cgroup_root=Path(request["cgroup_root"]), proc_root=Path(request["proc_root"]))
before = time.monotonic()
sampled = {}
for case in request["cases"]:
    sample = sampler.sample((case["name"], 1), pid=case["pid"], start_time_ticks=case["ticks"],
                            container_id=case["container_id"], expected_path=case["expected_path"])
    sampled[case["name"]] = None if sample is None else dataclasses.asdict(sample)
published = PublishedResidentMemory(Path(request["resident_path"]), ResidentMemorySampler(
    cgroup_root=Path(request["cgroup_root"]), proc_root=Path(request["proc_root"])))
loaded = published.load()
read = {}
for case in request["cases"]:
    sample = published.historical((case["name"], 1))
    read[case["name"]] = None if sample is None else dataclasses.asdict(sample)
json.dump({"before": before, "sampled": sampled, "loaded": loaded, "read": read}, sys.stdout)
"#;

fn run_python(interpreter: &Path, input: Value) -> Value {
    let mut child = Command::new(interpreter)
        .arg("-c")
        .arg(SCRIPT)
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

/// One fake runtime: its sentry and its cgroup's files.
struct Case {
    name: &'static str,
    /// `/proc/<pid>/cgroup`; `{c}` is the container's cgroup path.
    membership: &'static str,
    /// The OCI config's path (`{c}` as above), or none (legacy).
    expected: Option<&'static str>,
    /// The sentry's start ticks on disk (the caller expects 456).
    ticks: u64,
    files: &'static [(&'static str, &'static str)],
    /// Whether both samplers must accept it.
    accepted: bool,
}

const STAT: &str = "shmem 0\nanon 100\nfile 800\nfile_dirty 50\nfile_writeback 20\nworkingset_refault_file 7\n";

fn cases() -> Vec<Case> {
    let case = |name, files, accepted| Case { name, membership: "0::{c}", expected: Some("{c}"), ticks: 456, files, accepted };
    vec![
        case("base", &[("memory.current", "1000"), ("memory.stat", STAT)], true),
        case("peak-and-swap", &[("memory.current", "1000"), ("memory.peak", "900"), ("memory.swap.current", "400"), ("memory.stat", STAT)], true),
        case("peak-above-current", &[("memory.current", "1000"), ("memory.peak", "4096"), ("memory.stat", STAT)], true),
        case("peak-zero", &[("memory.current", "1000"), ("memory.peak", "0"), ("memory.stat", STAT)], true),
        case("peak-negative", &[("memory.current", "1000"), ("memory.peak", "-5"), ("memory.stat", STAT)], true),
        case("swap-negative", &[("memory.current", "1000"), ("memory.swap.current", "-7"), ("memory.stat", STAT)], true),
        case("tmpfs", &[("memory.current", "3000"), ("memory.stat", "shmem 2500\nanon 0\nfile 2900\nfile_dirty 0\nfile_writeback 0\nworkingset_refault_file 0\n")], true),
        case(
            "huge",
            &[
                ("memory.current", "4611686018427387904"),
                ("memory.peak", "9223372036854775807"),
                ("memory.swap.current", "1152921504606846976"),
                ("memory.stat", "shmem 4611686018427387903\nanon 9007199254740993\nfile 9223372036854775807\nfile_dirty 1\nfile_writeback 2\nworkingset_refault_file 9007199254740993\n"),
            ],
            true,
        ),
        case(
            "whitespace-extra-and-duplicate-counters",
            &[("memory.current", " 1000\n"), ("memory.stat", "shmem\t0\nanon  1\nanon 100\nfile 800\nfile_dirty 50\nfile_writeback +20\nworkingset_refault_file 7\nunevictable 3\n")],
            true,
        ),
        case("negative-anon", &[("memory.current", "1000"), ("memory.stat", "shmem 0\nanon -1\nfile 800\nfile_dirty 50\nfile_writeback 20\nworkingset_refault_file 7\n")], false),
        case("negative-current", &[("memory.current", "-1"), ("memory.stat", STAT)], false),
        case("negative-shmem", &[("memory.current", "1000"), ("memory.stat", "shmem -1\nanon 100\nfile 800\nfile_dirty 50\nfile_writeback 20\nworkingset_refault_file 7\n")], false),
        case("missing-counter", &[("memory.current", "1000"), ("memory.stat", "shmem 0\nanon 100\nfile 800\nfile_dirty 50\nfile_writeback 20\n")], false),
        case("missing-shmem", &[("memory.current", "1000"), ("memory.stat", "anon 100\nfile 800\nfile_dirty 50\nfile_writeback 20\nworkingset_refault_file 7\n")], false),
        case("missing-current", &[("memory.stat", STAT)], false),
        case("garbage-current", &[("memory.current", "abc"), ("memory.stat", STAT)], false),
        case("garbage-peak", &[("memory.current", "1000"), ("memory.peak", "x"), ("memory.stat", STAT)], false),
        case("three-field-stat-line", &[("memory.current", "1000"), ("memory.stat", "shmem 0 1\nanon 100\nfile 800\nfile_dirty 50\nfile_writeback 20\nworkingset_refault_file 7\n")], false),
        case("blank-stat-line", &[("memory.current", "1000"), ("memory.stat", "shmem 0\n\nanon 100\nfile 800\nfile_dirty 50\nfile_writeback 20\nworkingset_refault_file 7\n")], false),
        Case { ticks: 999, ..case("pid-reused", &[("memory.current", "1000"), ("memory.stat", STAT)], false) },
        Case { expected: Some("/another/runtime"), ..case("not-the-oci-cgroup", &[("memory.current", "1000"), ("memory.stat", STAT)], false) },
        Case { expected: None, ..case("legacy-by-container-id", &[("memory.current", "1000"), ("memory.stat", STAT)], true) },
        Case { expected: None, membership: "0::/ucloud-sandboxes", ..case("legacy-shared-parent", &[("memory.current", "1000"), ("memory.stat", STAT)], false) },
        Case { membership: "0::/ucloud-sandboxes/../{c}", expected: None, ..case("dot-dot", &[("memory.current", "1000"), ("memory.stat", STAT)], false) },
        Case { membership: "0::/x{c}\n0::{c}\n", ..case("two-unified", &[("memory.current", "1000"), ("memory.stat", STAT)], false) },
        Case { membership: "12:memory:/legacy\n0::{c}\n", ..case("v1-lines-beside-unified", &[("memory.current", "1000"), ("memory.stat", STAT)], true) },
    ]
}

#[test]
fn the_daemon_samples_and_publishes_exactly_what_the_agent_would() {
    let Some(interpreter) = python() else { return };
    let root = std::env::temp_dir().join(format!("noded-resident-parity-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let root = root.canonicalize().unwrap();
    let (proc, cgroup, state) = (root.join("proc"), root.join("cgroup"), root.join("state"));
    let cases = cases();
    let mut requests = Vec::new();
    for (index, case) in cases.iter().enumerate() {
        let pid = 1000 + index as u32;
        let container = format!("{:x<64}", case.name.replace('-', ""));
        let path = format!("/ucloud-sandboxes/{container}");
        let directory = cgroup.join(path.trim_start_matches('/'));
        std::fs::create_dir_all(&directory).unwrap();
        for (name, text) in case.files {
            std::fs::write(directory.join(name), text).unwrap();
        }
        let process = proc.join(pid.to_string());
        std::fs::create_dir_all(&process).unwrap();
        std::fs::write(process.join("stat"), format!("{pid} (runsc-sandbox) S {}{} 0", "0 ".repeat(18), case.ticks)).unwrap();
        std::fs::write(process.join("cgroup"), case.membership.replace("{c}", &path)).unwrap();
        requests.push(json!({
            "name": case.name, "pid": pid, "ticks": 456, "container_id": container,
            "expected_path": case.expected.map(|expected| expected.replace("{c}", &path)),
        }));
    }
    // A runtime whose cgroup is a link out of the trusted root.
    let outside = root.join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    for (name, text) in [("memory.current", "1000"), ("memory.stat", STAT)] {
        std::fs::write(outside.join(name), text).unwrap();
    }
    let escaping = format!("{:x<64}", "escaping");
    std::os::unix::fs::symlink(&outside, cgroup.join("ucloud-sandboxes").join(&escaping)).unwrap();
    std::fs::create_dir_all(proc.join("999")).unwrap();
    std::fs::write(proc.join("999/stat"), format!("999 (runsc-sandbox) S {}456 0", "0 ".repeat(18))).unwrap();
    std::fs::write(proc.join("999/cgroup"), format!("0::/ucloud-sandboxes/{escaping}")).unwrap();
    requests.push(json!({"name": "escaping", "pid": 999, "ticks": 456, "container_id": escaping, "expected_path": format!("/ucloud-sandboxes/{escaping}")}));

    // The daemon first, then the agent on the same trees.
    let clock = Arc::new(SystemClock::new());
    let sampler = ResidentSampler::new(cgroup.clone(), proc.clone(), clock.clone());
    let started = clock.monotonic();
    let mut published: Vec<((String, u64), ResidentSample)> = Vec::new();
    let mut rust: Vec<Option<ResidentSample>> = Vec::new();
    for request in &requests {
        let name = request["name"].as_str().unwrap().to_string();
        let sample = sampler.sample(
            &(name.clone(), 1),
            request["pid"].as_u64().unwrap() as u32,
            456,
            request["container_id"].as_str().unwrap(),
            request["expected_path"].as_str(),
        );
        if let Some(sample) = &sample {
            published.push(((name, 1), sample.clone()));
        }
        rust.push(sample);
    }
    published.sort_by(|left, right| left.0.cmp(&right.0));
    let mut file = ResidentFile::open(&state, "parity-session");
    let document = file.write(&published).unwrap();
    let python = run_python(&interpreter, json!({
        "cgroup_root": cgroup, "proc_root": proc, "resident_path": file.path(), "cases": requests,
    }));
    assert_eq!(python["loaded"], json!(true));

    let rendered: std::collections::HashMap<String, Value> = document["samples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|sample| (sample["sandbox_id"].as_str().unwrap().to_string(), sample.clone()))
        .collect();
    let python_started = python["before"].as_f64().unwrap();
    // One clock: the agent sampled after the daemon, moments later.
    assert!(python_started >= started && python_started - started < 30.0, "{started} vs {python_started}");
    let mut accepted = 0;
    for (request, rust) in requests.iter().zip(&rust) {
        let name = request["name"].as_str().unwrap();
        let expected = cases.iter().find(|case| case.name == name).is_some_and(|case| case.accepted);
        let python_sample = &python["sampled"][name];
        let read = &python["read"][name];
        let Some(rust) = rust else {
            assert!(!expected, "{name}: the daemon refused what the agent accepts: {python_sample}");
            assert_eq!((python_sample, read), (&Value::Null, &Value::Null), "{name}");
            continue;
        };
        assert!(expected, "{name}: the daemon accepted what should be unknown");
        accepted += 1;
        // Python's own sample: every field equal but the instant of the read.
        let mut ours = rendered[name].clone();
        let ours_map = ours.as_object_mut().unwrap();
        ours_map.remove("sandbox_id");
        ours_map.remove("generation");
        let mut theirs = python_sample.clone();
        let python_at = theirs.as_object_mut().unwrap().remove("sampled_at").unwrap().as_f64().unwrap();
        let rust_at = ours_map.remove("sampled_at").unwrap().as_f64().unwrap();
        assert_eq!(ours, theirs, "{name}");
        assert!(python_at >= rust_at && rust_at >= started, "{name}: {rust_at} vs {python_at}");
        // What the agent reads back is the daemon's sample, bit for bit.
        let mut read = read.clone();
        let read_at = read.as_object_mut().unwrap().remove("sampled_at").unwrap().as_f64().unwrap();
        assert_eq!(read_at.to_bits(), rust.sampled_at.to_bits(), "{name}");
        assert_eq!(read, theirs, "{name}");
    }
    assert_eq!(accepted, cases.iter().filter(|case| case.accepted).count());
    let _ = std::fs::remove_dir_all(&root);
}
