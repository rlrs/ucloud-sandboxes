//! The exec fence against the agent's own implementation
//! (ucloud_sandboxes/exec_fence.py), across processes.

use std::path::PathBuf;
use std::process::Command;

use ucloud_noded::exec_fence::{ExecFence, Fenced};

fn python() -> Option<PathBuf> {
    let path = std::env::var_os("UCLOUD_SANDBOXES_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../.venv/bin/python")));
    path.exists().then_some(path)
}

fn run(python: &PathBuf, directory: &std::path::Path, script: &str) -> String {
    let output = Command::new(python)
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .arg("-c")
        .arg(script)
        .arg(directory)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

#[test]
fn python_transitions_and_rust_execs_exclude_each_other() {
    let Some(python) = python() else { return };
    let directory = std::env::temp_dir().join(format!("noded-fence-py-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&directory);
    std::fs::create_dir_all(&directory).unwrap();
    let fence = ExecFence::new(&directory);
    let Fenced::Held(lease) = fence.acquire("box").unwrap() else { panic!("free fence is busy") };
    // Park and pause fail fast; delete and wake tolerate the running exec.
    let script = r#"
import sys
from pathlib import Path
from ucloud_sandboxes.exec_fence import ExecFence
from ucloud_sandboxes.sandbox import SandboxBusyError
fence = ExecFence(Path(sys.argv[1]))
try:
    with fence.transition("box", allow_shared=False):
        print("parked")
except SandboxBusyError as exc:
    print("busy:", exc)
with fence.transition("box", allow_shared=True):
    print("deleted", fence.activity_idle("box"))
"#;
    assert_eq!(run(&python, &directory, script), "busy: sandbox has active exec/file activity: box\ndeleted False");
    drop(lease);
    // A Python transition holding T sends the daemon's execs to the agent.
    let holder = r#"
import sys, time
from pathlib import Path
from ucloud_sandboxes.exec_fence import ExecFence
fence = ExecFence(Path(sys.argv[1]))
with fence.transition("box", allow_shared=False):
    print("held", flush=True)
    time.sleep(2)
"#;
    let mut child = Command::new(&python)
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .arg("-c")
        .arg(holder)
        .arg(&directory)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    std::io::BufRead::read_line(&mut std::io::BufReader::new(child.stdout.as_mut().unwrap()), &mut line).unwrap();
    assert_eq!(line.trim(), "held");
    assert!(matches!(fence.acquire("box").unwrap(), Fenced::Busy));
    child.wait().unwrap();
    assert!(matches!(fence.acquire("box").unwrap(), Fenced::Held(_)));
    let _ = std::fs::remove_dir_all(&directory);
}
