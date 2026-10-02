#!/usr/bin/env python3
"""S13 gate 3: correctness and speed of file-backed EROFS + fanotify against S12's NBD backend,
both reading the same local pack store, under gVisor.

  coldfan.py tree  [--images 0,60,63,72,90,130,170]
  coldfan.py seq   [--images ...] [--paths nbd:demand,fan:demand,...] [--reps 2]
  coldfan.py burst --n 20 --path fan:demand [--tag x]

Paths:
  nbd:demand       S12's s3nbd.py --fetch local (chunk cache in memory and on disk, misses merged into
                   <= 1 MiB pack windows), one NBD device per image, mount -t erofs
  nbd:readaround   the same, plus S12's 1 MiB read-around in the pack
  fan:demand       fand.py: a sparse unified backing file per image on ext4, filled per event with the
                   chunks the event range overlaps (misses merged into <= 1 MiB pack windows)
  fan:window       fand.py --window 1 MiB: each fill covers the unfilled chunks of the aligned 1 MiB
                   window around the event
  fan:demand-dio   fan:demand with -o directio
  fan:multidev     per-image bootstrap file + one sparse file per blob (layer), shared by every image
                   with that blob, mounted with -o device= (EROFS multi-device, file-backed)
  ...+f            fand.py --fast-noop: events whose range is already filled are answered on the
                   reader thread, without a hop through the worker pool
tree: full-tree comparison (names, types, modes, owners, sizes, sha256, symlinks, xattrs, mtimes,
hardlink groups) of each image through fan:demand against the OCI layers, and the full-read time
through both paths. seq/burst: as S12's coldrun.py (runsc --platform=systrap --network=none run,
gofer-served rootfs, host OverlayFS upper), cold then warm.
"""
import argparse
import hashlib
import json
import os
import shutil
import socket
import stat
import subprocess
import sys
import tarfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/root/s12")
import coldrun as C  # noqa: E402  (runsc, sh, drop_caches, config, NBD Backend, SAMPLE)

S13 = "/data/s13"
BF = f"{S13}/bf"
FSOCK = "/run/s13fan.sock"
RES = "/data/results"


class FanBackend:
    def __init__(self, extra=()):
        subprocess.run(["pkill", "-f", "fand.py"])
        time.sleep(0.3)
        shutil.rmtree(BF, ignore_errors=True)
        os.makedirs(BF)
        self.log = open("/data/logs/fand.log", "a")
        self.p = subprocess.Popen(["python3", "/root/s13/fand.py", "--sock", FSOCK, *extra], stdout=subprocess.PIPE,
                                  stderr=self.log, text=True)
        line = self.p.stdout.readline()
        assert line.startswith("LISTENING"), line
        self.tls = threading.local()

    call = C.Backend.call
    cpu = C.Backend.cpu

    def close(self):
        try:
            self.call(op="quit")
        except Exception:  # noqa: BLE001
            pass
        self.p.wait(10)


C.SOCK_FAN = FSOCK


def make_backend(path):
    kind, mode = path.split(":")
    if kind == "nbd":
        return C.Backend("local")
    mode, _, flags = mode.partition("+")
    extra = ["--fast-noop"] if "f" in flags else []
    if mode.startswith("window"):
        extra += ["--window", str(1 << 20)]
    return FanBackend(extra)


def fan_call(be, **req):
    s = getattr(be.tls, "s", None)
    if s is None:
        s = socket.socket(socket.AF_UNIX)
        s.connect(FSOCK)
        be.tls.s = s
        be.tls.f = s.makefile("rb")
    s.sendall((json.dumps(req) + "\n").encode())
    return json.loads(be.tls.f.readline())


FanBackend.call = fan_call


class Sandbox:
    """Attach + mount + OverlayFS upper, for either path."""

    def __init__(self, be, idx, path, tag, name=None):
        self.be, self.idx, self.path, self.tag = be, idx, path, tag
        kind, mode = path.split(":")
        mode = mode.partition("+")[0]
        self.kind = kind
        self.base = f"/data/mt/{tag}"
        shutil.rmtree(self.base, ignore_errors=True)
        os.makedirs(f"{self.base}/lower")
        name = name or f"img-{idx:03d}"
        t0 = time.time()
        if kind == "nbd":
            self.dev = C.next_dev()
            r = be.call(op="attach", dev=self.dev, name=name, mode="readaround" if mode == "readaround" else "demand")
            src, opts = self.dev, "ro"
        elif mode == "multidev":
            # Per-image bootstrap file + per-blob sparse files shared across images, -o device=.
            r = be.call(op="attach_multidev", name=name, dir=f"{BF}/md")
            self.keys = r.get("keys", [])
            src, opts = r.get("boot"), "ro," + ",".join(f"device={d}" for d in r.get("devices", []))
        else:
            self.key = f"{BF}/{tag}.img"
            r = be.call(op="attach_image", name=name, path=self.key)
            src, opts = self.key, "ro,directio" if mode.endswith("dio") else "ro"
        self.mode = mode
        if not r.get("ok"):
            raise RuntimeError(f"attach {idx}: {r}")
        self.attach = r
        m = C.sh("mount", "-t", "erofs", "-o", opts, src, f"{self.base}/lower", check=False)
        if m.returncode:
            raise RuntimeError(f"mount {idx}: {m.stderr}")
        self.lower = f"{self.base}/lower"
        for d in ("upper", "work", "rootfs"):
            os.makedirs(f"{self.base}/{d}")
        C.sh("mount", "-t", "overlay", "overlay", "-o",
             f"lowerdir={self.lower},upperdir={self.base}/upper,workdir={self.base}/work", f"{self.base}/rootfs")
        self.attach_wall = time.time() - t0
        self.root = f"{self.base}/rootfs"

    def close(self, unlink=True):
        C.sh("umount", self.root, check=False)
        C.sh("umount", self.lower, check=False)
        if self.kind == "nbd":
            r = self.be.call(op="detach", dev=self.dev)
        elif self.mode == "multidev":
            r = {"blobs": [self.be.call(op="detach", key=k) for k in self.keys]}
        else:
            r = self.be.call(op="detach", key=self.key, unlink=unlink)
        shutil.rmtree(self.base, ignore_errors=True)
        return r


def cpu_jiffies():
    v = list(map(int, open("/proc/stat").readline().split()[1:]))
    return sum(v) - v[3] - v[4]


# ---------------------------------------------------------------- tree
_OPQ = ".wh..wh..opq"


def _normal(name):
    name = name[2:] if name.startswith("./") else name
    name = name.strip("/")
    return "" if name == "." else name


def expected_tree(layer_tars):
    """chunk_convert.expected_tree's semantics (M1), streaming each layer once."""
    tree = {}
    for path in layer_tars:
        entries, whiteouts = [], []
        with tarfile.open(path, "r|*") as reader:
            for m in reader:
                name = _normal(m.name)
                parent, _, base = name.rpartition("/")
                prefix = parent + "/" if parent else ""
                if base == _OPQ:
                    whiteouts.append(("opq", parent))
                    continue
                if base.startswith(".wh."):
                    whiteouts.append(("wh", prefix + base[4:]))
                    continue
                if not name:
                    continue
                content = None
                if m.isfile():
                    content = hashlib.file_digest(reader.extractfile(m), "sha256").hexdigest()
                elif m.issym():
                    content = m.linkname
                xattrs = tuple(sorted((k[len("SCHILY.xattr."):], v) for k, v in m.pax_headers.items()
                                      if k.startswith("SCHILY.xattr.") and not k.startswith("SCHILY.xattr.trusted.overlay.")))
                entries.append((name, m.isdir(), m.islnk(), _normal(m.linkname) if m.islnk() else None,
                                ("dir" if m.isdir() else "symlink" if m.issym() else "file" if m.isfile() else
                                 "char" if m.ischr() else "block" if m.isblk() else "fifo"),
                                m.mode & 0o7777, m.uid, m.gid, m.size if m.isfile() else 0, content, int(m.mtime), xattrs))
        for kind, x in whiteouts:
            if kind == "opq":
                pre = x + "/" if x else ""
                for k in [k for k in tree if k.startswith(pre) and k != x]:
                    del tree[k]
            else:
                for k in [k for k in tree if k == x or k.startswith(x + "/")]:
                    del tree[k]
        for (name, isdir, islnk, target, kind, mode, uid, gid, size, content, mtime, xattrs) in entries:
            if islnk:
                if target in tree:
                    tree[name] = tree[target][:-1] + (tree[target][-1] or target,)
                continue
            if not isdir and name in tree and tree[name][0] == "dir":
                for k in [k for k in tree if k.startswith(name + "/")]:
                    del tree[k]
            tree[name] = (kind, None if kind == "symlink" else mode, uid, gid, size, content,
                          None if kind == "dir" else mtime, xattrs, None)
    return tree


def scan_tree(root):
    tree, inodes = {}, {}
    for directory, names, files in os.walk(root):
        for name in names + files:
            p = os.path.join(directory, name)
            info = os.lstat(p)
            rel = p[len(root) + 1:]
            mode = info.st_mode
            kind = ("dir" if stat.S_ISDIR(mode) else "symlink" if stat.S_ISLNK(mode) else "file" if stat.S_ISREG(mode)
                    else "char" if stat.S_ISCHR(mode) else "block" if stat.S_ISBLK(mode) else "fifo")
            content = None
            if kind == "file":
                with open(p, "rb") as fh:
                    content = hashlib.file_digest(fh, "sha256").hexdigest()
            elif kind == "symlink":
                content = os.readlink(p)
            try:
                xn = os.listxattr(p, follow_symlinks=False)
            except OSError:
                xn = []
            xattrs = tuple(sorted((k, os.getxattr(p, k, follow_symlinks=False).decode(errors="surrogateescape"))
                                  for k in xn if not k.startswith("trusted.overlay.")))
            group = None
            if kind == "file" and info.st_nlink > 1:
                group = inodes.setdefault((info.st_dev, info.st_ino), rel)
            tree[rel] = (kind, None if kind == "symlink" else stat.S_IMODE(mode), info.st_uid, info.st_gid,
                         info.st_size if kind == "file" else 0, content, None if kind == "dir" else int(info.st_mtime),
                         xattrs, group)
    return tree


def compare_trees(expected, actual, limit=20):
    def groups(tree):
        found = {}
        for name, at in tree.items():
            if at[-1] is not None:
                found.setdefault(at[-1], {at[-1]}).add(name)
        return sorted(sorted(g) for g in found.values() if len(g) > 1)
    diffs = []
    for name in sorted(set(expected) | set(actual)):
        if name not in actual or name not in expected:
            diffs.append(f"{name}: {'missing' if name not in actual else 'unexpected'}")
        elif expected[name][:-1] != actual[name][:-1]:
            diffs.append(f"{name}: expected {expected[name][:-1]}, found {actual[name][:-1]}")
        if len(diffs) >= limit:
            return diffs
    if groups(expected) != groups(actual):
        diffs.append("hardlink groups differ")
    return diffs


def classify(expected, actual):
    """Count differences by field: (kind, mode, uid, gid, size, content, mtime, xattrs)."""
    fields = ("kind", "mode", "uid", "gid", "size", "content", "mtime", "xattrs")
    out = {"missing": 0, "unexpected": 0, "owner_only": 0, "by_field": dict.fromkeys(fields, 0), "examples_non_owner": []}
    for name in set(expected) | set(actual):
        if name not in actual:
            out["missing"] += 1
            continue
        if name not in expected:
            out["unexpected"] += 1
            continue
        e, a = expected[name][:-1], actual[name][:-1]
        if e == a:
            continue
        diff = [f for f, x, y in zip(fields, e, a) if x != y]
        for f in diff:
            out["by_field"][f] += 1
        if set(diff) <= {"uid", "gid"}:
            out["owner_only"] += 1
        elif len(out["examples_non_owner"]) < 10:
            out["examples_non_owner"].append(f"{name}: {e} != {a}")
    out["equal_ignoring_owner"] = not (out["missing"] or out["unexpected"] or any(
        v for f, v in out["by_field"].items() if f not in ("uid", "gid")))
    return out


def tree(args):
    out = []
    for idx in map(int, args.images.split(",")):
        s = C.SAMPLE[idx]
        rec = {"idx": idx, "family": s["family"], "image": s["image"]}
        t = time.time()
        exp = expected_tree([f"/data/oci/blobs/sha256/{l['digest'].split(':')[1]}" for l in s["manifest"]["layers"]])
        rec["expected_s"] = time.time() - t
        rec["expected_entries"] = len(exp)
        trees = {}
        for path in args.paths.split(","):
            be = make_backend(path)
            C.drop_caches()
            sb = Sandbox(be, idx, path, f"tree-{idx}")
            t = time.time()
            j0 = cpu_jiffies()
            trees[path] = scan_tree(sb.lower)
            r = {"read_s": time.time() - t, "host_cpu_s": (cpu_jiffies() - j0) / os.sysconf("SC_CLK_TCK"),
                 "attach_s": sb.attach_wall, "entries": len(trees[path]),
                 "bytes": sum(v[4] for v in trees[path].values())}
            r["stats"] = be.call(op="stats")
            if path.startswith("fan"):
                # Warm re-read after dropping caches: every chunk is filled now; do events still come?
                C.drop_caches()
                be.call(op="stats", reset=True)
                t = time.time()
                C.sh("sh", "-c", f"find {sb.lower} -type f -exec cat {{}} + > /dev/null")
                r["refill_read_s"] = time.time() - t
                st = be.call(op="stats")
                r["refill_events"] = st["counters"]["events"]
                r["refill_event_ms"] = st["event_ms"]
                r["filled_after_full_read"] = st["filled"].get(getattr(sb, "key", None))
            r["backend_cpu_s"] = be.cpu()
            sb.close()
            be.close()
            rec[path] = r
        rec["diff_fan_vs_oci"] = compare_trees(exp, trees["fan:demand"])
        rec["diff_nbd_vs_oci"] = compare_trees(exp, trees["nbd:demand"]) if "nbd:demand" in trees else None
        rec["fan_equals_nbd"] = trees["fan:demand"] == trees["nbd:demand"] if "nbd:demand" in trees else None
        rec["classified_fan_vs_oci"] = classify(exp, trees["fan:demand"])
        rec["expected_nonroot_owned"] = sum(1 for v in exp.values() if v[2] or v[3])
        rec["equal"] = not rec["diff_fan_vs_oci"]
        out.append(rec)
        print(json.dumps({k: rec[k] for k in ("idx", "family", "expected_entries", "equal", "fan_equals_nbd")}),
              "fan_read_s", round(rec["fan:demand"]["read_s"], 2),
              "refill_events", rec["fan:demand"].get("refill_events"),
              "classified", json.dumps({k: v for k, v in rec["classified_fan_vs_oci"].items() if k != "examples_non_owner"}),
              flush=True)
        json.dump(out, open(f"{RES}/tree-{args.tag}.json", "w"), indent=1)


# ---------------------------------------------------------------- seq
def seq(args):
    out = []
    for rep in range(args.reps):
        for idx in map(int, args.images.split(",")):
            cfg = C.config(idx)
            for path in args.paths.split(","):
                for cmd in args.cmds.split(","):
                    be = make_backend(path)
                    C.drop_caches()
                    rec = {"idx": idx, "family": C.SAMPLE[idx]["family"], "path": path, "cmd": cmd, "rep": rep}
                    try:
                        j0 = cpu_jiffies()
                        sb = Sandbox(be, idx, path, f"seq-{idx}")
                        rec["attach_wall"] = sb.attach_wall
                        rec["attach"] = sb.attach
                        rec["cold"] = C.runsc(sb.root, cfg, C.CMDS[cmd], f"s13-{idx}-a")
                        rec["host_cpu_cold_s"] = (cpu_jiffies() - j0) / os.sysconf("SC_CLK_TCK")
                        rec["stats_after_cold"] = be.call(op="stats")
                        rec["warm"] = C.runsc(sb.root, cfg, C.CMDS[cmd], f"s13-{idx}-b")
                        rec["device"] = sb.close()
                        rec["backend_cpu_s"] = be.cpu()
                    except Exception as e:  # noqa: BLE001
                        rec["error"] = repr(e)[-600:]
                    be.close()
                    out.append(rec)
                    st = rec.get("stats_after_cold", {})
                    c = st.get("counters", {})
                    mb = (c.get("fetched_bytes_demand", 0) + c.get("fetched_bytes_prefetch", 0)) if path.startswith("nbd") \
                        else c.get("pack_bytes", 0)
                    print(json.dumps({k: rec.get(k) for k in ("idx", "path", "cmd", "rep", "error")}),
                          "attach", round(rec.get("attach_wall", -1), 3),
                          "cold", round(rec.get("cold", {}).get("wall", -1), 3), rec.get("cold", {}).get("rc"),
                          "warm", round(rec.get("warm", {}).get("wall", -1), 3), "MB", round(mb / 1e6, 2),
                          "events", c.get("events"), "ev_ms", st.get("event_ms", {}).get("p50"), flush=True)
                    json.dump(out, open(f"{RES}/seq-{args.tag}.json", "w"), indent=1)


# ---------------------------------------------------------------- burst
def burst(args):
    idxs = json.load(open("/root/s12/burst-images.json"))[: args.n]
    be = make_backend(args.path)
    C.drop_caches()
    time.sleep(1)
    b0, t_all0, cpu0 = cpu_jiffies(), time.time(), be.cpu()

    def one(k, idx):
        r = {"idx": idx, "family": C.SAMPLE[idx]["family"]}
        t0 = time.time()
        try:
            sb = Sandbox(be, idx, args.path, f"burst-{k}")
            r["attach_s"] = time.time() - t0
            cfg = C.config(idx)
            r["import_sys"] = C.runsc(sb.root, cfg, C.CMDS["import_sys"], f"s13b-{k}-a")
            r["first_command_done_s"] = time.time() - t0
            r["pip_version"] = C.runsc(sb.root, cfg, C.CMDS["pip_version"], f"s13b-{k}-b")
            r["all_done_s"] = time.time() - t0
            r["_sb"] = sb
        except Exception as e:  # noqa: BLE001
            r["error"] = repr(e)[-600:]
        return r
    with ThreadPoolExecutor(len(idxs)) as ex:
        rs = list(ex.map(lambda a: one(*a), enumerate(idxs)))
    wall = time.time() - t_all0
    b1 = cpu_jiffies()
    stats = be.call(op="stats")
    cpu1 = be.cpu()
    def close(r):  # in parallel: an NBD detach waits up to 5 s for its kernel thread (S12's backend)
        sb = r.pop("_sb", None)
        if sb:
            r["device"] = sb.close()
    with ThreadPoolExecutor(32) as ex:
        list(ex.map(close, rs))
    be.close()
    ok = [r for r in rs if "error" not in r]

    def med(xs):
        xs = sorted(xs)
        return xs[len(xs) // 2] if xs else None
    c = stats.get("counters", {})
    summary = {"n": len(idxs), "ok": len(ok), "path": args.path, "wall_s": wall,
               "attach_median": med([r["attach_s"] for r in ok]), "attach_max": max((r["attach_s"] for r in ok), default=None),
               "first_cmd_median": med([r["first_command_done_s"] for r in ok]),
               "first_cmd_max": max((r["first_command_done_s"] for r in ok), default=None),
               "pip_median": med([r["pip_version"]["wall"] for r in ok]),
               "all_done_max": max((r["all_done_s"] for r in ok), default=None),
               "host_busy_cpu_s": (b1 - b0) / os.sysconf("SC_CLK_TCK"), "backend_cpu_s": cpu1 - cpu0,
               "bytes_read": (c.get("fetched_bytes_demand", 0) + c.get("fetched_bytes_prefetch", 0))
               if args.path.startswith("nbd") else c.get("pack_bytes", 0),
               "events": c.get("events"), "event_ms": stats.get("event_ms"),
               "cmd_failures": sum(1 for r in ok if r["import_sys"]["rc"] or r["pip_version"]["rc"]),
               "errors": [r["error"][-200:] for r in rs if "error" in r][:3]}
    print(json.dumps(summary), flush=True)
    json.dump({"summary": summary, "backend": stats, "sandboxes": rs}, open(f"{RES}/burst-{args.tag}.json", "w"), indent=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["tree", "seq", "burst"])
    ap.add_argument("--images", default="0,60,63,72,90,130,170")
    ap.add_argument("--paths", default="nbd:demand,fan:demand")
    ap.add_argument("--cmds", default="import_sys,git_status,pip_version")
    ap.add_argument("--reps", type=int, default=2)
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--path", default="fan:demand")
    ap.add_argument("--tag", default="main")
    a = ap.parse_args()
    os.makedirs("/data/mt", exist_ok=True)
    {"tree": tree, "seq": seq, "burst": burst}[a.what](a)


if __name__ == "__main__":
    main()
