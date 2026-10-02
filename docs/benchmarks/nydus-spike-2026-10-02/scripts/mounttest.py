#!/usr/bin/env python3
"""Attach + first-command timing: Nydus RAFS v6 (kernel EROFS, flat device over NBD) vs today's
EROFS components (one NBD per component + overlay), both under runsc with a writable overlay upper.

Usage: mounttest.py <sample idx> <nydus run dir> [--repeat N]
"""
import glob, hashlib, json, os, random, shutil, struct, subprocess, sys, time, urllib.request
sys.path.insert(0, "/root")
import rafs
REG = "http://10.42.0.2:5000"
RUNSC = "/usr/local/libexec/ucloud-gvisor/runsc"
SAMPLE = json.load(open(os.environ.get("MT_SAMPLE", "/data/manifests/sample-manifests.json")))
PATHS = os.environ.get("MT_PATHS", "nydus,today").split(",")
NBD_POOL = [f"/dev/nbd{i}" for i in range(16, 64)]


def sh(*cmd, check=True, **kw):
    return subprocess.run(cmd, check=check, capture_output=True, text=True, **kw)


def drop_caches():
    os.sync(); open("/proc/sys/vm/drop_caches", "w").write("3\n")


def component_file(digest):
    path = f"/data/components/{digest.split(':')[1]}"
    if not os.path.exists(path):
        os.makedirs("/data/components", exist_ok=True)
        with urllib.request.urlopen(f"{REG}/v2/environments/blobs/{digest}", timeout=600) as r, open(path + ".part", "wb") as f:
            shutil.copyfileobj(r, f, 8 << 20)
        os.rename(path + ".part", path)
    return path


class Attach:
    def __init__(self, tag):
        self.tag, self.procs, self.mounts, self.devs = tag, [], [], []
        self.base = f"/data/mt/{tag}"
        shutil.rmtree(self.base, ignore_errors=True); os.makedirs(self.base)

    def nbd(self, *args):
        dev = next(d for d in NBD_POOL if d not in USED); USED.add(dev); self.devs.append(dev)
        p = subprocess.Popen(["python3", "/root/spike_nbd.py", dev, *args], stdout=subprocess.PIPE, text=True)
        line = p.stdout.readline()
        if not line.startswith("READY"):
            raise RuntimeError(f"nbd failed: {line}")
        self.procs.append(p)
        return dev

    def mount(self, *args):
        target = args[-1]; os.makedirs(target, exist_ok=True)
        r = sh("mount", *args, check=False)
        if r.returncode:
            raise RuntimeError(f"mount {args}: {r.stderr}")
        self.mounts.append(target)

    def overlay(self, lowers):
        up, wk, root = f"{self.base}/upper", f"{self.base}/work", f"{self.base}/rootfs"
        for d in (up, wk, root):
            os.makedirs(d, exist_ok=True)
        self.mount("-t", "overlay", "overlay", "-o", f"lowerdir={':'.join(lowers)},upperdir={up},workdir={wk}", root)
        return root

    def close(self):
        stats = []
        for m in reversed(self.mounts):
            sh("umount", m, check=False)
        for p in self.procs:
            p.terminate(); out = p.communicate(timeout=20)[0]
            stats += [l for l in out.splitlines() if l.startswith("STATS")]
        for d in self.devs:
            USED.discard(d)
        return stats


USED = set()


def attach_nydus(rundir, idx):
    a = Attach(f"nydus-{idx}")
    t = time.time()
    dev = a.nbd("nydus", f"{rundir}/images/{idx:03d}.boot", f"{rundir}/blobs")
    t_nbd = time.time() - t
    a.mount("-t", "erofs", "-o", "ro", dev, f"{a.base}/lower")
    t_mount = time.time() - t
    root = a.overlay([f"{a.base}/lower"])
    return a, root, {"nbd_ready": t_nbd, "erofs_mounted": t_mount, "attach": time.time() - t, "devices": 1}


def attach_today(idx):
    comps = [component_file(c["layers"][0]["digest"]) for c in SAMPLE[idx]["env"]["components"]]
    for f in comps:  # build the signed-index stand-in outside the timed attach
        if not os.path.exists(f + ".index.json"):
            sh("python3", "-c", f"import sys; sys.argv=['x','/dev/null','raw','{f}']; exec(open('/root/spike_nbd.py').read().split('def main')[0]); RawSource('{f}')")
    drop_caches()
    a = Attach(f"today-{idx}")
    t = time.time()
    lowers = []
    for i, f in enumerate(comps):
        dev = a.nbd("raw", f)
        a.mount("-t", "erofs", "-o", "ro", dev, f"{a.base}/c{i}")
        lowers.append(f"{a.base}/c{i}")
    t_mount = time.time() - t
    root = a.overlay(list(reversed(lowers)))
    return a, root, {"erofs_mounted": t_mount, "attach": time.time() - t, "devices": len(comps)}


def image_config(idx):
    s = SAMPLE[idx]
    local = f"/data/oci/blobs/sha256/{s['manifest']['config']['digest'].split(':')[1]}"
    if os.path.exists(local):
        return json.load(open(local)).get("config", {})
    with urllib.request.urlopen(f"{REG}/v2/{s['repo']}/blobs/{s['manifest']['config']['digest']}") as r:
        return json.load(r).get("config", {})


def runsc(root, cfg, argv, cid):
    b = f"/data/mt/bundle-{cid}"; shutil.rmtree(b, ignore_errors=True); os.makedirs(b)
    sh(RUNSC, "spec", "--bundle", b)
    spec = json.load(open(f"{b}/config.json"))
    spec["root"] = {"path": root, "readonly": False}
    spec["process"]["args"] = argv; spec["process"]["terminal"] = False
    spec["process"]["env"] = cfg.get("Env") or ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"]
    spec["process"]["cwd"] = cfg.get("WorkingDir") or "/"
    spec["process"]["user"] = {"uid": 0, "gid": 0}
    json.dump(spec, open(f"{b}/config.json", "w"))
    t = time.time()
    r = sh(RUNSC, "--root=/run/spike-runsc", "--platform=systrap", "--network=none", "run", "--bundle", b, cid, check=False)
    wall = time.time() - t
    sh(RUNSC, "--root=/run/spike-runsc", "delete", "-force", cid, check=False)
    return {"wall": wall, "rc": r.returncode, "out": (r.stdout + r.stderr)[-300:]}


def pyc_check(root, limit=4000):
    """Compare timestamp .pyc headers with their source stat (what CPython's default check does)."""
    res = {"pyc_checked": 0, "pyc_stale": 0, "py_mtime_zero": 0}
    for pyc in glob.iglob(f"{root}/usr/**/__pycache__/*.pyc", recursive=True):
        if res["pyc_checked"] >= limit:
            break
        name = os.path.basename(pyc).split(".")[0] + ".py"
        src = os.path.join(os.path.dirname(os.path.dirname(pyc)), name)
        try:
            hdr = open(pyc, "rb").read(16); st = os.stat(src)
        except OSError:
            continue
        flags, mt, sz = struct.unpack("<III", hdr[4:16])
        if flags != 0:
            continue
        res["pyc_checked"] += 1
        res["pyc_stale"] += (mt != (int(st.st_mtime) & 0xffffffff) or sz != (st.st_size & 0xffffffff))
        res["py_mtime_zero"] += int(st.st_mtime) == 0
    return res


def compare_trees(a, b, limit=20000, hashes=300):
    diff = {"files": 0, "missing": 0, "mtime_diff": 0, "mtime_diff_by_ext": {}, "mode_diff": 0, "size_diff": 0, "hash_diff": 0, "hashed": 0}
    rnd = random.Random(1)
    for dp, dn, fn in os.walk(b):
        for f in fn:
            pb = os.path.join(dp, f); pa = a + pb[len(b):]
            try:
                sb = os.lstat(pb)
            except OSError:
                continue
            diff["files"] += 1
            try:
                sa = os.lstat(pa)
            except OSError:
                diff["missing"] += 1; continue
            if int(sa.st_mtime) != int(sb.st_mtime):
                diff["mtime_diff"] += 1
                ext = os.path.splitext(f)[1] or "(none)"
                diff["mtime_diff_by_ext"][ext] = diff["mtime_diff_by_ext"].get(ext, 0) + 1
            diff["mode_diff"] += sa.st_mode != sb.st_mode
            diff["size_diff"] += sa.st_size != sb.st_size
            if os.path.isfile(pb) and not os.path.islink(pb) and diff["hashed"] < hashes and rnd.random() < 0.05:
                diff["hashed"] += 1
                diff["hash_diff"] += hashlib.sha256(open(pa, "rb").read()).digest() != hashlib.sha256(open(pb, "rb").read()).digest()
            if diff["files"] >= limit:
                return diff
    return diff


def tar_mtime_check(idx, roots, limit=4000):
    """Final-tree mtimes from the OCI layers (last writer wins) vs stat() in each mounted tree."""
    import tarfile
    final = {}
    for l in SAMPLE[idx]["manifest"]["layers"]:
        with tarfile.open(f"/data/oci/blobs/sha256/{l['digest'].split(':')[1]}", "r|gz") as t:
            for m in t:
                name = m.name.lstrip("./") if m.name.startswith("./") else m.name
                base = os.path.basename(name)
                if base.startswith(".wh."):
                    final.pop(os.path.join(os.path.dirname(name), base[4:]), None); continue
                final[name] = (m.mtime, m.type)
    paths = sorted(p for p, (_, ty) in final.items() if ty in (tarfile.REGTYPE, tarfile.AREGTYPE, tarfile.SYMTYPE, tarfile.DIRTYPE))
    random.Random(2).shuffle(paths)
    out = {r: {"checked": 0, "match": 0, "zero": 0, "by_kind": {}} for r in roots}
    for p in paths[:limit]:
        mt, ty = final[p]
        kind = {tarfile.DIRTYPE: "dir", tarfile.SYMTYPE: "symlink"}.get(ty, os.path.splitext(p)[1] if os.path.splitext(p)[1] in (".py", ".pyc", ".so") else "file")
        for name, root in roots.items():
            try:
                st = os.lstat(os.path.join(root, p))
            except OSError:
                continue
            o = out[name]; o["checked"] += 1
            ok = int(st.st_mtime) == int(mt)
            o["match"] += ok; o["zero"] += int(st.st_mtime) == 0
            k = o["by_kind"].setdefault(kind, [0, 0]); k[0] += 1; k[1] += ok
    return out


def main():
    idx, rundir = int(sys.argv[1]), sys.argv[2]
    reps = int(sys.argv[3]) if len(sys.argv) > 3 else 2
    s = SAMPLE[idx]
    cfg = image_config(idx)
    has_testbed = True
    cmds = {"python_import_sys": ["sh", "-c", "python3 -c 'import sys' || python -c 'import sys'"],
            "python_import_stdlib": ["sh", "-c", "python3 -c 'import json, email.mime.text, http.client, asyncio, decimal' || python -c 'import json'"],
            "git_status": ["sh", "-c", "cd /testbed 2>/dev/null && git status --porcelain | wc -l || echo no-testbed"]}
    result = {"idx": idx, "image": s["image"], "family": s["family"], "rundir": rundir, "runs": []}
    for rep in range(reps):
        for path in PATHS:
            for first in ("python_import_sys", "git_status"):
                drop_caches()
                try:
                    a, root, at = attach_nydus(rundir, idx) if path == "nydus" else attach_today(idx)
                except Exception as e:
                    result["runs"].append({"path": path, "error": str(e)[-500:]}); continue
                rec = {"path": path, "rep": rep, "first": first, **at}
                rec[first + "_cold"] = runsc(root, cfg, cmds[first], f"{path}-{idx}-{rep}-a")
                rec[first + "_warm"] = runsc(root, cfg, cmds[first], f"{path}-{idx}-{rep}-b")
                if first == "python_import_sys":
                    rec["python_import_stdlib_after"] = runsc(root, cfg, cmds["python_import_stdlib"], f"{path}-{idx}-{rep}-c")
                    rec["python_import_stdlib_again"] = runsc(root, cfg, cmds["python_import_stdlib"], f"{path}-{idx}-{rep}-d")
                    rec["upper_pyc_written"] = sum(1 for _ in glob.iglob(f"/data/mt/{path}-{idx}/upper/**/*.pyc", recursive=True))
                rec["nbd_stats"] = a.close()
                result["runs"].append(rec)
                print(json.dumps(rec)[:600], flush=True)
    # Content, mtime and .pyc checks (warm, both mounted at once).
    an, rn, _ = attach_nydus(rundir, idx)
    if "today" in PATHS:
        at_, rt, _ = attach_today(idx)
        result["tree_compare_nydus_vs_today"] = compare_trees(f"{an.base}/lower", f"{at_.base}/rootfs")
        result["tar_mtime"] = tar_mtime_check(idx, {"nydus": f"{an.base}/lower", "today": f"{at_.base}/rootfs"})
        result["pyc_today"] = pyc_check(f"{at_.base}/rootfs")
        at_.close()
    else:
        result["tar_mtime"] = tar_mtime_check(idx, {"nydus": f"{an.base}/lower"})
    result["pyc_nydus"] = pyc_check(f"{an.base}/lower")
    an.close()
    print(json.dumps({k: v for k, v in result.items() if k != "runs"}), flush=True)
    os.makedirs("/data/results", exist_ok=True)
    json.dump(result, open(f"/data/results/mount-{idx:03d}" + ("" if rundir.endswith("nodict-1m") else "-" + os.path.basename(rundir)) + ".json", "w"), indent=1)


main()
