#!/usr/bin/env python3
"""Spike: managed-process state on a host bind mount under the production runsc.

Run as root with the production runsc and sandbox init (from a sandbox node package):
  sudo python3 managed_host_mount.py RUNSC UCLOUD_SANDBOX_INIT
docs/managed-process-reads.md records the results. Questions:
  Q1 guest writes (state.json renames, log appends) are visible on the host at once, and inotify fires;
  Q2 host reads during `runsc pause` need nothing from the guest;
  Q3 checkpoint --hibernate / delete / restore with the mount (and control.sock in it, host-uds none);
  Q4 restore with the mount source at a different host path (a move to another node);
  Q5 a guest-made symlink in the mount is a host symlink (readers must refuse it);
  Q6 log offsets after restore: no gap, no duplicate.
"""
import ctypes, json, os, shutil, struct, subprocess, sys, time
from pathlib import Path

RUNSC = str(Path(sys.argv[1]).resolve())
INIT = Path(sys.argv[2]).resolve()
WORK = Path("/var/tmp/spike-managed")
ROOT = WORK / "runsc-root"
CID = "spike-managed"

class R:
    def __init__(self, rc, out, err): self.returncode, self.stdout, self.stderr = rc, out, err

def run(*args, check=True, input=None, timeout=60):
    # Files, not pipes: runsc's sandbox and gofer inherit stdio and outlive the command.
    import tempfile
    with tempfile.TemporaryFile() as o, tempfile.TemporaryFile() as e:
        p = subprocess.run(args, stdout=o, stderr=e, input=input, timeout=timeout,
                           stdin=None if input is not None else subprocess.DEVNULL)
        o.seek(0); e.seek(0)
        r = R(p.returncode, o.read(), e.read())
    if check and r.returncode:
        raise SystemExit(f"FAILED {' '.join(map(str, args))}\n{r.stdout.decode()[-2000:]}\n{r.stderr.decode()[-3000:]}")
    return r

def runsc(*args, **kw):
    return run(RUNSC, f"--root={ROOT}", "--platform=systrap", "--network=none", "--debug", f"--debug-log={WORK}/logs/", f"--application-memory-file-dir={WORK}/mem", 
               "--allow-connected-on-save=true", *args, **kw)

# --- minimal inotify (ctypes) ---
libc = ctypes.CDLL("libc.so.6", use_errno=True)
IN_MODIFY, IN_CLOSE_WRITE, IN_MOVED_TO, IN_CREATE, IN_NONBLOCK = 0x2, 0x8, 0x80, 0x100, 0o4000
def inotify(path):
    fd = libc.inotify_init1(IN_NONBLOCK)
    wd = libc.inotify_add_watch(fd, str(path).encode(), IN_MODIFY | IN_CLOSE_WRITE | IN_MOVED_TO | IN_CREATE)
    assert fd >= 0 and wd >= 0, ctypes.get_errno()
    return fd
def drain(fd):
    names = []
    while True:
        try:
            buf = os.read(fd, 65536)
        except BlockingIOError:
            return names
        i = 0
        while i < len(buf):
            _, mask, _, ln = struct.unpack_from("iIII", buf, i)
            name = buf[i + 16:i + 16 + ln].rstrip(b"\0").decode()
            names.append((name, mask))
            i += 16 + ln

def ctl(payload):
    r = runsc("exec", "--user=0:0", CID, "/.ucloud-job-init", "ctl", "--timeout", "5000ms",
              input=json.dumps(payload).encode(), check=False)
    try:
        return json.loads(r.stdout)
    except Exception:
        return {"raw": r.stdout.decode()[-500:], "err": r.stderr.decode()[-800:], "rc": r.returncode}

def bundle(managed_src):
    b = WORK / "bundle"
    cfg = {
        "ociVersion": "1.0.0",
        "process": {"user": {"uid": 0, "gid": 0}, "args": ["/.ucloud-job-init", "supervise", "--state-dir", "/.ucloud-managed"],
                    "env": ["PATH=/usr/sbin:/usr/bin:/sbin:/bin"], "cwd": "/",
                    "capabilities": {k: ["CAP_SETUID", "CAP_SETGID", "CAP_KILL", "CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FOWNER"]
                                     for k in ("bounding", "effective", "permitted")}},
        "root": {"path": "rootfs", "readonly": False},
        "hostname": "spike",
        "annotations": {"dev.gvisor.internal.application-memory-directory": "alloc-" + managed_src.parent.parent.name},
        "mounts": [
            {"destination": "/proc", "type": "proc", "source": "proc"},
            {"destination": "/tmp", "type": "tmpfs", "source": "tmpfs"},
            {"destination": "/.ucloud-managed", "type": "bind", "source": str(managed_src),
             "options": ["rbind", "rw", "nosuid", "nodev", "noexec"]},
        ],
        "linux": {"namespaces": [{"type": t} for t in ("pid", "ipc", "uts", "mount", "network")]},
    }
    (b / "config.json").write_text(json.dumps(cfg))
    return b

def size(p):
    try:
        return os.lstat(p).st_size
    except FileNotFoundError:
        return -1

def main():
    if WORK.exists():
        subprocess.run([RUNSC, f"--root={ROOT}", "delete", "--force", CID], capture_output=True)
        shutil.rmtree(WORK)
    (WORK / "bundle/rootfs").mkdir(parents=True)
    ROOT.mkdir(); (WORK / "logs").mkdir(); (WORK / "mem").mkdir(); (WORK / "mem/alloc-bundle").mkdir(); (WORK / "mem/alloc-moved").mkdir()
    cid = run("docker", "create", "alpine:latest").stdout.decode().strip()
    exp = subprocess.Popen(["docker", "export", cid], stdout=subprocess.PIPE)
    run("tar", "-x", "-C", str(WORK / "bundle/rootfs"), input=exp.stdout.read())
    run("docker", "rm", cid)
    shutil.copy2(INIT, WORK / "bundle/rootfs/.ucloud-job-init")
    m1 = WORK / "bundle/rootfs/.ucloud-managed"
    m1.mkdir(mode=0o700)
    b = bundle(m1)
    results = {}

    fd = inotify(m1)
    runsc("create", f"--bundle={b}", CID)
    runsc("start", CID)
    time.sleep(1)
    results["Q1.socket_on_host"] = os.path.exists(m1 / "control.sock") and __import__("stat").S_ISSOCK(os.lstat(m1 / "control.sock").st_mode)
    start = ctl({"version": 1, "action": "start", "job_id": "j1",
                 "argv": ["/bin/sh", "-c", "i=0; while true; do echo line $i; i=$((i+1)); sleep 0.1; done"],
                 "uid": 0, "gid": 0, "cwd": "/", "max_stdout_bytes": 1 << 20, "max_stderr_bytes": 1 << 20})
    results["start"] = start.get("ok"), start.get("error"), start.get("err")
    # Q1: host sees growth live, with inotify events.
    samples = []
    for _ in range(10):
        time.sleep(0.2)
        samples.append(size(m1 / "stdout.log"))
    ev = drain(fd)
    results["Q1.stdout_sizes_live"] = samples
    results["Q1.inotify_events"] = sorted({(n, hex(m)) for n, m in ev})[:12]
    results["Q1.state_on_host"] = json.loads((m1 / "state.json").read_text()) if (m1 / "state.json").exists() else None
    st = ctl({"version": 1, "action": "status", "job_id": "j1"})
    results["Q1.ctl_stdout_bytes_vs_host"] = (st.get("job", {}).get("stdout_bytes"), size(m1 / "stdout.log"))

    # Q2: pause; host reads; nothing grows; no events.
    runsc("pause", CID)
    s0 = size(m1 / "stdout.log"); drain(fd); time.sleep(1.0)
    results["Q2.paused_stable"] = (s0, size(m1 / "stdout.log"), len(drain(fd)))
    results["Q2.paused_host_read_ok"] = (m1 / "stdout.log").read_bytes()[-20:].decode(errors="replace")
    runsc("resume", CID)
    time.sleep(0.5)
    results["Q2.after_resume_grows"] = size(m1 / "stdout.log") > s0

    # Q5: guest symlink.
    r = runsc("exec", CID, "/bin/sh", "-c", "ln -s /etc/shadow /.ucloud-managed/evil; ls -la /.ucloud-managed", check=False)
    results["Q5.guest_ls"] = r.stdout.decode()[-600:]
    results["Q5.host_is_symlink"] = os.path.islink(m1 / "evil"), (os.readlink(m1 / "evil") if os.path.islink(m1 / "evil") else None)
    os.unlink(m1 / "evil") if os.path.lexists(m1 / "evil") else None

    # Q3: checkpoint --hibernate, delete, restore (same source).
    img = WORK / "img1"; img.mkdir()
    before = size(m1 / "stdout.log")
    r = runsc("checkpoint", "--hibernate", f"--image-path={img}", CID, check=False, timeout=120)
    results["Q3.checkpoint_rc"] = r.returncode, r.stderr.decode()[-800:]
    if r.returncode == 0:
        at_ckpt = size(m1 / "stdout.log")
        time.sleep(0.5)
        results["Q3.no_growth_after_ckpt"] = (at_ckpt, size(m1 / "stdout.log"))
        runsc("delete", "--force", CID, check=False)
        r = runsc("restore", "--detach", f"--image-path={img}", f"--bundle={b}", CID, check=False, timeout=120)
        results["Q3.restore_rc"] = r.returncode, r.stderr.decode()[-1500:]
        time.sleep(1.0)
        st = ctl({"version": 1, "action": "status", "job_id": "j1"})
        results["Q3.status_after_restore"] = {k: st.get("job", {}).get(k) for k in ("state", "stdout_bytes", "sequence")} if st.get("ok") else st
        results["Q3.host_grows_after_restore"] = (at_ckpt, size(m1 / "stdout.log"))
        lines = (m1 / "stdout.log").read_text().split("\n")
        nums = [int(l.split()[1]) for l in lines if l.startswith("line ")]
        results["Q6.contiguous"] = nums == list(range(nums[0], nums[0] + len(nums))), len(nums)

        # Q4: checkpoint again, copy the managed dir to a new path, restore with a regenerated bundle.
        img2 = WORK / "img2"; img2.mkdir()
        r = runsc("checkpoint", "--hibernate", f"--image-path={img2}", CID, check=False, timeout=120)
        results["Q4.checkpoint_rc"] = r.returncode, r.stderr.decode()[-500:]
        runsc("delete", "--force", CID, check=False)
        m2 = WORK / "moved/rootfs/.ucloud-managed"
        (WORK / "moved/rootfs").mkdir(parents=True)
        run("cp", "-a", "--sparse=always", str(m1), str(m2))
        bundle(m2)
        r = runsc("restore", "--detach", f"--image-path={img2}", f"--bundle={b}", CID, check=False, timeout=120)
        results["Q4.restore_rc"] = r.returncode, r.stderr.decode()[-1500:]
        time.sleep(1.0)
        results["Q4.new_source_grows"] = (size(m2 / "stdout.log"), size(m1 / "stdout.log"))
        time.sleep(0.5)
        results["Q4.new_source_grows_2"] = (size(m2 / "stdout.log"), size(m1 / "stdout.log"))
        st = ctl({"version": 1, "action": "status", "job_id": "j1"})
        results["Q4.status"] = {k: st.get("job", {}).get(k) for k in ("state", "stdout_bytes", "sequence")} if st.get("ok") else st
    runsc("delete", "--force", CID, check=False)
    print(json.dumps(results, indent=1, default=str))

main()
