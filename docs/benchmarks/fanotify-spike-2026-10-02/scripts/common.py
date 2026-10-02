"""S13 shared helpers: shell, mounts, listeners, tree hashing."""
import hashlib
import json
import os
import shutil
import subprocess
import time

S13 = "/root/s13"


def sh(*cmd, check=True, timeout=None):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f"{cmd}: rc={r.returncode} {r.stderr[-400:]}")
    return r


def drop_caches():
    os.sync()
    with open("/proc/sys/vm/drop_caches", "w") as f:
        f.write("3\n")


def mount_erofs(src, mnt, opts="ro"):
    os.makedirs(mnt, exist_ok=True)
    t = time.perf_counter()
    r = sh("mount", "-t", "erofs", "-o", opts, src, mnt, check=False)
    return {"rc": r.returncode, "err": r.stderr.strip()[-300:], "s": time.perf_counter() - t}


def umount(mnt):
    return sh("umount", mnt, check=False).returncode


def dmesg_tail(n=20):
    return sh("dmesg", check=False).stdout.splitlines()[-n:]


def hash_tree(root, limit_files=None):
    """{relative path: sha256} of every regular file, plus counters; errors recorded per path."""
    out, errors, nbytes = {}, {}, 0
    for dp, dn, fn in os.walk(root):
        dn.sort()
        for f in sorted(fn):
            p = os.path.join(dp, f)
            if os.path.islink(p) or not os.path.isfile(p):
                continue
            try:
                with open(p, "rb") as fh:
                    h = hashlib.file_digest(fh, "sha256")
                out[p[len(root):]] = h.hexdigest()
                nbytes += os.path.getsize(p)
            except OSError as e:
                errors[p[len(root):]] = f"{e.errno} {e.strerror}"
            if limit_files and len(out) >= limit_files:
                return out, errors, nbytes
    return out, errors, nbytes


def compare_hashes(ref, got):
    mism = [p for p in ref if p in got and got[p] != ref[p]]
    missing = [p for p in ref if p not in got]
    return {"files": len(ref), "equal": len(ref) - len(mism) - len(missing), "mismatch": len(mism),
            "missing": len(missing), "examples": (mism + missing)[:5]}


class Listener:
    """fanl.py as a subprocess; waits for its ready line."""

    def __init__(self, *args, log=None):
        self.log = log
        cmd = ["python3", f"{S13}/fanl.py", *args] + (["--log", log] if log else [])
        if log and os.path.exists(log):
            os.unlink(log)
        self.p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        line = self.p.stdout.readline()
        if not line:
            raise RuntimeError("listener failed: " + self.p.stderr.read()[-800:])
        self.ready = json.loads(line)

    def events(self):
        if not self.log or not os.path.exists(self.log):
            return []
        return [json.loads(l) for l in open(self.log) if l.strip()]

    def kill(self, sig=9):
        try:
            os.kill(self.p.pid, sig)
        except ProcessLookupError:
            pass
        try:
            self.p.wait(10)
        except subprocess.TimeoutExpired:
            pass


def summarize_events(evs):
    ranges = [r for e in evs for r in e.get("ranges", [])]
    counts = sorted(c for _, c in ranges)
    comms = {}
    for e in evs:
        comms[e.get("comm")] = comms.get(e.get("comm"), 0) + 1
    pct = (lambda q: counts[min(len(counts) - 1, int(q * len(counts)))] if counts else None)
    return {"events": len(evs), "with_range": sum(1 for e in evs if e.get("ranges")),
            "masks": sorted({e["mask"] for e in evs}), "info_types": sorted({i[0] for e in evs for i in e.get("infos", [])}),
            "range_count_bytes": {"min": counts[0] if counts else None, "p50": pct(.5), "p90": pct(.9),
                                  "max": counts[-1] if counts else None, "sum": sum(counts)},
            "offsets_4k_aligned": all(o % 4096 == 0 for o, _ in ranges), "comms": comms,
            "fill_us_p50": sorted(e.get("fill_us", 0) for e in evs)[len(evs) // 2] if evs else None,
            "responses": sorted({e.get("response", e.get("response_error")) for e in evs})}


def fresh_copy(src, dst):
    """Copy keeping holes as holes (the sparse backing file's unfilled ranges)."""
    if os.path.exists(dst):
        os.unlink(dst)
    sh("cp", "--sparse=always", src, dst)
