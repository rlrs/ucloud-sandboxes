"""S11 spike helpers: registry access, nydusd control, our EROFS/NBD path, runsc sandboxes.

Runs as root on the disposable spike VM only.
"""
import base64
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tarfile
import threading
import time
import urllib.request

W = Path("/root/s11")
NYDUS_REG = "127.0.0.1:5001"
PROXY_REG = "127.0.0.1:5002"
DOMAIN = "s11"
FSCACHE_DIR = W / "fscache-cache"
BOOT_DIR = W / "boot"
MNT_DIR = W / "mnt"
SB_DIR = W / "sb"
RUNSC_ROOT = W / "runsc-root"
RUNSC = W / "bundle/runtime/direct/runsc"
NYDUSD_SOCK = Path("/run/s11-nydusd.sock")
SUP_SOCK = Path("/run/s11-supervisor.sock")
ENVIO_ROOT = W / "envio"
ENVSTORE_ROOT = W / "envstore"
ENVIO_SOCK = Path("/run/s11-envio.sock")
GUEST_CAPS = sorted([
    "CAP_AUDIT_WRITE", "CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FOWNER", "CAP_FSETID",
    "CAP_KILL", "CAP_MKNOD", "CAP_NET_BIND_SERVICE", "CAP_NET_RAW", "CAP_SETFCAP",
    "CAP_SETGID", "CAP_SETPCAP", "CAP_SETUID", "CAP_SYS_CHROOT"])


def run(argv, check=True, timeout=600, **kw):
    kw.setdefault("stdin", subprocess.DEVNULL)
    p = subprocess.run([str(a) for a in argv], capture_output=True, text=True, timeout=timeout, **kw)
    if check and p.returncode:
        raise RuntimeError(f"{argv!r} rc={p.returncode} stdout={p.stdout[-800:]} stderr={p.stderr[-1500:]}")
    return p


# ---------------------------------------------------------------- registry
def reg_get(host, path, accept=None, range_=None):
    req = urllib.request.Request(f"http://{host}{path}")
    if accept:
        req.add_header("Accept", accept)
    if range_:
        req.add_header("Range", range_)
    with urllib.request.urlopen(req, timeout=300) as r:
        return r.read(), dict(r.headers)


MANIFEST_ACCEPT = ",".join([
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.oci.image.index.v1+json"])


def manifest(host, repo, ref):
    data, _ = reg_get(host, f"/v2/{repo}/manifests/{ref}", MANIFEST_ACCEPT)
    return json.loads(data)


def split_ref(ref):
    """10.42.0.2:5000/ucloud-managed/x:latest@sha256:... -> (repo, digest)."""
    rest = ref.split("/", 1)[1]
    repo_tag, digest = rest.split("@", 1)
    return repo_tag.rsplit(":", 1)[0], digest


def image_config(prepared_reference):
    repo, digest = split_ref(prepared_reference)
    m = manifest(PROXY_REG, repo, digest)
    data, _ = reg_get(PROXY_REG, f"/v2/{repo}/blobs/{m['config']['digest']}")
    return json.loads(data), m


def nydus_layers(name):
    m = manifest(NYDUS_REG, f"s11/{name}", "nydus")
    boot = [l for l in m["layers"] if l.get("annotations", {}).get("containerd.io/snapshot/nydus-bootstrap") == "true"]
    blobs = [l for l in m["layers"] if l.get("annotations", {}).get("containerd.io/snapshot/nydus-blob") == "true"]
    assert len(boot) == 1, m
    return m, boot[0], blobs


def fetch_bootstrap(name):
    """Download the bootstrap layer and extract image/image.boot (what nydus-snapshotter does)."""
    _, boot, _ = nydus_layers(name)
    data, _ = reg_get(NYDUS_REG, f"/v2/s11/{name}/blobs/{boot['digest']}")
    assert "sha256:" + hashlib.sha256(data).hexdigest() == boot["digest"]
    BOOT_DIR.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        member = tar.getmember("image/image.boot")
        payload = tar.extractfile(member).read()
    out = BOOT_DIR / f"{name}.boot"
    tmp = out.with_suffix(".tmp")
    tmp.write_bytes(payload)
    tmp.replace(out)
    return out, len(data), boot["digest"]


def nft_counters():
    p = run(["nft", "-j", "list", "counters", "table", "inet", "s11"])
    out = {}
    for item in json.loads(p.stdout)["nftables"]:
        c = item.get("counter")
        if c:
            out[c["name"]] = c["bytes"]
    return out


# ---------------------------------------------------------------- nydusd
class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path, timeout=60):
        super().__init__("localhost", timeout=timeout)
        self.path = str(path)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def api(method, path, body=None, sock=NYDUSD_SOCK, timeout=60):
    c = UnixHTTP(sock, timeout)
    payload = None if body is None else json.dumps(body)
    c.request(method, path, body=payload, headers={"Content-Type": "application/json"} if payload else {})
    r = c.getresponse()
    data = r.read()
    c.close()
    if r.status >= 300:
        raise RuntimeError(f"nydusd {method} {path}: {r.status} {data[:500]!r}")
    return json.loads(data) if data else None


class Nydusd:
    def __init__(self, threads=16, log="nydusd.log", supervisor=False, upgrade=False, log_level="info"):
        self.threads, self.log, self.supervisor, self.upgrade, self.log_level = threads, log, supervisor, upgrade, log_level
        self.proc = None

    def argv(self):
        a = ["nydusd", "singleton", "--fscache", str(FSCACHE_DIR), "--fscache-threads", str(self.threads),
             "--apisock", str(NYDUSD_SOCK), "--log-level", self.log_level, "--log-file", str(W / "logs" / self.log)]
        if self.supervisor:
            a += ["--id", "s11", "--supervisor", str(SUP_SOCK)]
        if self.upgrade:
            a += ["--upgrade"]
        return a

    def start(self, wait=True):
        FSCACHE_DIR.mkdir(parents=True, exist_ok=True)
        (W / "logs").mkdir(exist_ok=True)
        if not self.upgrade and NYDUSD_SOCK.exists():
            NYDUSD_SOCK.unlink()
        self.proc = subprocess.Popen(self.argv(), stdout=open(W / "logs" / (self.log + ".out"), "a"),
                                     stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, start_new_session=True)
        if wait:
            deadline = time.monotonic() + 20
            while True:
                try:
                    info = api("GET", "/api/v1/daemon")
                    if self.upgrade or info.get("state") in ("RUNNING", "Running", "READY", "Ready"):
                        return info
                except (OSError, RuntimeError, json.JSONDecodeError):
                    pass
                if self.proc.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("nydusd failed to start: " + (W / "logs" / (self.log + ".out")).read_text()[-2000:])
                time.sleep(0.05)

    @property
    def pid(self):
        return self.proc.pid

    def kill(self, sig=9):
        if self.proc and self.proc.poll() is None:
            os.kill(self.proc.pid, sig)
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.kill(self.proc.pid, 9)
                self.proc.wait()


def bootstrap_entry(name, *, domain=DOMAIN, backend=None, validate=True, prefetch=False, boot=None):
    backend = backend or {"type": "registry", "registry": {"scheme": "http", "host": NYDUS_REG, "repo": f"s11/{name}",
                                                            "timeout": 30, "connect_timeout": 10, "retry_limit": 2}}
    return {
        "type": "bootstrap", "id": name, "domain_id": domain,
        "config_v2": {
            "version": 2, "id": name, "backend": backend,
            "cache": {"type": "fscache", "fscache": {"work_dir": str(FSCACHE_DIR)}, "validate": validate,
                      "prefetch": {"enable": prefetch, "threads": 4, "batch_size": 1048576}},
            "metadata_path": str(boot or BOOT_DIR / f"{name}.boot"),
        },
    }


def fscache_attach(name, *, domain=DOMAIN, validate=True, prefetch=False, backend=None, fetch=True):
    """Cold attach: fetch bootstrap, bind it to nydusd, mount EROFS over fscache. Returns (mountpoint, timings)."""
    t = {}
    t0 = time.monotonic()
    if fetch or not (BOOT_DIR / f"{name}.boot").exists():
        _, t["bootstrap_layer_bytes"], _ = fetch_bootstrap(name)
    t1 = time.monotonic()
    api("PUT", "/api/v2/blobs", bootstrap_entry(name, domain=domain, validate=validate, prefetch=prefetch, backend=backend))
    t2 = time.monotonic()
    mnt = MNT_DIR / f"{domain}-{name}"
    mnt.mkdir(parents=True, exist_ok=True)
    run(["mount", "-t", "erofs", "-o", f"fsid={name},domain_id={domain}", "none", mnt])
    t3 = time.monotonic()
    t.update(bootstrap_fetch_s=t1 - t0, bind_s=t2 - t1, mount_s=t3 - t2, attach_s=t3 - t0)
    return mnt, t


# ---------------------------------------------------------------- our EROFS/NBD path
def stub_trust():
    """The spike was not given the production producer trust file. Signature checks are
    stubbed; every content digest (roots, indexes, 256 KiB chunks) is still verified."""
    import ucloud_sandboxes.environment_artifact as ea
    import ucloud_sandboxes.environment_metadata as em
    ea._authenticate = lambda component, trusted_keys: component
    ea.ImmutableEnvironment.authenticate = lambda self, trusted_keys: self

    def hint_auth(self, trusted_keys, *, component_digest, component):
        if (self.component != component_digest or self.image_digest != component.image_digest
                or self.chunk_count != len(component.chunks)):
            raise ValueError("environment metadata hint describes another component")
        return self
    em.MetadataHint.authenticate = hint_auth


def dummy_keys():
    path = W / "dummy-producers.json"
    if not path.exists():
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
        pub = Ed25519PrivateKey.generate().public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        path.write_text(json.dumps({"sha256:" + hashlib.sha256(pub).hexdigest(): base64.b64encode(pub).decode()}))
        os.chmod(path, 0o644)
    return path


def env_registry():
    stub_trust()
    from ucloud_sandboxes.environment_config import configured_environment_registry
    return configured_environment_registry(f"http://{PROXY_REG}", "environments", dummy_keys())


class NbdBackend:
    """Our storage process (serve-environment-io), with metrics exported, as the qualification harness runs it."""
    def __init__(self, cache_bytes=64 * 1024 ** 3, prefetch=True):
        self.cache_bytes, self.prefetch = cache_bytes, prefetch
        self.proc = None

    def start(self):
        ENVIO_ROOT.mkdir(parents=True, exist_ok=True)
        os.chmod(ENVIO_ROOT, 0o700)
        if ENVIO_SOCK.exists():
            ENVIO_SOCK.unlink()
        settings = {"cache_bytes": self.cache_bytes, "prefetch": self.prefetch}
        (W / "logs").mkdir(exist_ok=True)
        self.proc = subprocess.Popen([str(W / "venv/bin/python"), str(W / "nbd_backend.py"), json.dumps(settings)],
                                     stdout=open(W / "logs/envio.log", "a"), stderr=subprocess.STDOUT, cwd=str(W),
                                     stdin=subprocess.DEVNULL, start_new_session=True)
        deadline = time.monotonic() + 20
        while not ENVIO_SOCK.exists():
            if self.proc.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError("backend failed: " + (W / "logs/envio.log").read_text()[-2000:])
            time.sleep(0.05)

    @property
    def pid(self):
        return self.proc.pid

    def kill(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()


class NbdFrontend:
    def __init__(self):
        from ucloud_sandboxes.environment_backend import EnvironmentBackendClient
        from ucloud_sandboxes.environment_rootfs import EnvironmentRootfsStore
        self.registry = env_registry()
        ENVSTORE_ROOT.mkdir(parents=True, exist_ok=True)
        os.chmod(ENVSTORE_ROOT, 0o700)
        self.client = EnvironmentBackendClient(ENVIO_SOCK)
        # Spike-only workaround: the backend listens with socketserver's default backlog (5) and the
        # client's timeout socket gets EAGAIN from connect(2) when it is full. Retry, and count.
        self.connect_retries = 0
        original = self.client._call

        def call(request, timeout=None, _orig=original):
            for attempt in range(200):
                try:
                    return _orig(request, timeout)
                except BlockingIOError:
                    self.connect_retries += 1
                    time.sleep(0.01)
            return _orig(request, timeout)
        self.client._call = call
        self.store = EnvironmentRootfsStore(ENVSTORE_ROOT / "images", self.registry, self.client)

    def attach(self, prepared_reference):
        ref = prepared_reference.replace("10.42.0.2:5000", PROXY_REG, 1)
        t0 = time.monotonic()
        with self.store.operation_lease(ref) as image:
            rootfs = image.rootfs
            components = len(image.environment.components) if hasattr(image.environment, "components") else None
        return Path(rootfs), {"attach_s": time.monotonic() - t0, "components": components}

    def metrics(self):
        return self.client.metrics()


# ---------------------------------------------------------------- sandboxes (runsc)
def oci_spec(config, args, cid):
    cfg = config.get("config") or {}
    env = list(cfg.get("Env") or ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"])
    if not any(e.startswith("PATH=") for e in env):
        env.append("PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
    return {
        "ociVersion": "1.0.2", "hostname": cid,
        "root": {"path": "rootfs", "readonly": False},
        "process": {"terminal": False, "user": {"uid": 0, "gid": 0}, "args": args, "env": env,
                    "cwd": cfg.get("WorkingDir") or "/", "noNewPrivileges": False,
                    "capabilities": {k: GUEST_CAPS for k in ("bounding", "effective", "inheritable", "permitted")},
                    "rlimits": [{"type": "RLIMIT_NOFILE", "hard": 1048576, "soft": 1048576}]},
        "mounts": [
            {"destination": "/proc", "type": "proc", "source": "proc"},
            {"destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": ["nosuid", "strictatime", "mode=755", "size=65536k"]},
            {"destination": "/dev/pts", "type": "devpts", "source": "devpts", "options": ["nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620"]},
            {"destination": "/dev/shm", "type": "tmpfs", "source": "shm", "options": ["nosuid", "noexec", "nodev", "mode=1777", "size=65536k"]},
            {"destination": "/sys", "type": "sysfs", "source": "sysfs", "options": ["nosuid", "noexec", "nodev", "ro"]},
        ],
        "linux": {"namespaces": [{"type": t} for t in ("pid", "network", "ipc", "uts", "mount")],
                  "cgroupsPath": f"/s11/{cid}",
                  "resources": {"memory": {"limit": 2 * 1024 ** 3, "swap": 4 * 1024 ** 3},
                                "cpu": {"period": 100000, "quota": 200000}},
                  "maskedPaths": ["/proc/kcore", "/proc/keys", "/sys/firmware"],
                  "readonlyPaths": ["/proc/sys", "/proc/sysrq-trigger"]},
        "annotations": {"dev.ucloud-sandboxes.sandbox-id": cid},
    }


RUNSC_FLAGS = ["--platform=systrap", "--network=none"]


def runsc(*args, check=True, timeout=300):
    return run([RUNSC, f"--root={RUNSC_ROOT}", *RUNSC_FLAGS, *args], check=check, timeout=timeout)


class Sandbox:
    """Host OverlayFS (lower = image tree, private upper) + runsc with its default --overlay2=root:self,
    as image_rootfs.OverlayRootfsManager and direct_warden compose a bundle."""
    def __init__(self, cid, lower, config):
        self.cid, self.lower, self.config = cid, Path(lower), config
        self.dir = SB_DIR / cid
        self.started = False

    def start(self):
        t0 = time.monotonic()
        upper, work, rootfs = self.dir / "upper", self.dir / "work", self.dir / "rootfs"
        for p in (upper, work, rootfs):
            p.mkdir(parents=True, exist_ok=True)
        run(["mount", "-t", "overlay", "overlay", "-o", f"lowerdir={self.lower},upperdir={upper},workdir={work}", rootfs])
        t1 = time.monotonic()
        spec = oci_spec(self.config, ["sleep", "2147483647"], self.cid)
        (self.dir / "config.json").write_text(json.dumps(spec))
        RUNSC_ROOT.mkdir(parents=True, exist_ok=True)
        # The sandbox inherits create's stdio as the container's: never a pipe we wait on.
        with open(self.dir / "stdio.log", "w") as log:
            p = subprocess.run([str(RUNSC), f"--root={RUNSC_ROOT}", *RUNSC_FLAGS, "create", f"--bundle={self.dir}", self.cid],
                               stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, timeout=300)
        if p.returncode:
            raise RuntimeError("runsc create failed: " + (self.dir / "stdio.log").read_text()[-1500:])
        runsc("start", self.cid)
        self.started = True
        t2 = time.monotonic()
        return {"overlay_s": t1 - t0, "runsc_start_s": t2 - t1}

    def exec(self, argv, cwd=None):
        env = oci_spec(self.config, [], self.cid)["process"]["env"]
        a = ["exec"] + [f"--env={e}" for e in env]
        if cwd:
            a.append(f"--cwd={cwd}")
        t0 = time.monotonic()
        p = runsc(*a, self.cid, *argv, check=False, timeout=600)
        return {"seconds": time.monotonic() - t0, "rc": p.returncode,
                "stdout": p.stdout[-200:], "stderr": p.stderr[-300:]}

    def stop(self):
        runsc("delete", "--force", self.cid, check=False)
        run(["umount", self.dir / "rootfs"], check=False)
        shutil.rmtree(self.dir, ignore_errors=True)


def workdir_candidates(config):
    cfg = config.get("config") or {}
    return cfg.get("WorkingDir") or "/"


def cold_commands(config):
    wd = workdir_candidates(config)
    return [
        ("python_import_sys", ["python3", "-c", "import sys"], None),
        ("git_status", ["git", "-C", wd, "status", "--porcelain", "--untracked-files=no"], None),
        ("pip_version", ["python3", "-m", "pip", "--version"], None),
        ("pytest_import", ["python3", "-c", "import pytest"], None),
    ]


# ---------------------------------------------------------------- host accounting
def cpu_jiffies():
    fields = Path("/proc/stat").read_text().splitlines()[0].split()[1:]
    vals = list(map(int, fields))
    idle = vals[3] + vals[4]
    return sum(vals) - idle, sum(vals)


def proc_cpu_seconds(pid):
    try:
        parts = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return (int(parts[11]) + int(parts[12])) / os.sysconf("SC_CLK_TCK")
    except FileNotFoundError:
        return None


def du(path):
    """(allocated bytes, apparent bytes) under path, never crossing into mounted filesystems."""
    if not Path(path).exists():
        return 0, 0
    a = int(run(["du", "-xsB1", path], check=False).stdout.split()[0] or 0)
    b = int(run(["du", "-xsb", "--apparent-size", path], check=False).stdout.split()[0] or 0)
    return a, b


def fs_used(path="/root/s11"):
    """Used bytes of the filesystem holding path, after writeback (cachefiles keeps in-use
    backing files as unlinked tmpfiles, which du cannot see)."""
    os.sync()
    st = os.statvfs(path)
    return (st.f_blocks - st.f_bfree) * st.f_frsize


def drop_caches():
    run(["sync"])
    Path("/proc/sys/vm/drop_caches").write_text("3\n")


def mounts_under(root):
    root = str(root)
    out = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        mp = line.split()[4].replace("\\040", " ")
        if mp.startswith(root + "/"):
            out.append(mp)
    return sorted(out, key=lambda m: (m.count("/"), m), reverse=True)


def unmount_all(root):
    for _ in range(3):
        ms = mounts_under(root)
        if not ms:
            return
        for m in ms:
            run(["umount", m], check=False)
    left = mounts_under(root)
    if left:
        for m in left:
            run(["umount", "-l", m], check=False)


def delete_all_containers():
    subprocess.run(["pkill", "-9", "-f", "[r]unsc-(gofer|sandbox) --root=/root/s11"])
    if not RUNSC_ROOT.exists():
        return
    p = runsc("list", "-format=json", check=False)
    try:
        items = json.loads(p.stdout or "[]") or []
    except json.JSONDecodeError:
        items = []
    for it in items:
        runsc("delete", "--force", it["id"], check=False)
    subprocess.run(["pkill", "-9", "-f", "[r]unsc-(gofer|sandbox) --root=/root/s11"])
    run(["umount", RUNSC_ROOT / "null-netns"], check=False)
    shutil.rmtree(RUNSC_ROOT, ignore_errors=True)


def sha256_file(path, limit=None):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()
