#!/usr/bin/env python3
"""S12 (c): cold first commands and cold bursts over the chunk-store NBD backend (s3nbd.py).

  coldrun.py seq   --images 0,60,63,72,90,130,170 --modes local:demand,s3:demand,s3:readaround,s3:trace --reps 2
  coldrun.py burst --n 20 --mode s3:readaround [--trace] [--tag x]

seq: each cycle is a fresh backend, an empty chunk cache and dropped page caches, then attach
(fetch + verify bootstrap and chunk map/locator), EROFS mount, a writable OverlayFS upper, and one
`runsc run` of the command cold, then warm (as S10's mounttest.py). Traces (chunk ids in first-touch
order) are recorded by the s3:readaround cycles and replayed by s3:trace cycles of the same command.
burst: N distinct images on one cold backend, all at once: attach, mount, then `import sys` and
`pip --version` in runsc, per sandbox.
"""
import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

RUNSC = "/usr/local/libexec/ucloud-gvisor/runsc"
SAMPLE = json.load(open("/data/manifests/sample-manifests.json"))
S12 = "/data/s12"
SOCK = "/run/s12nbd.sock"
CMDS = {
    "import_sys": ["sh", "-c", "python3 -c 'import sys' || python -c 'import sys'"],
    "git_status": ["sh", "-c", "cd /testbed 2>/dev/null && git status --porcelain | wc -l || echo no-testbed"],
    "pip_version": ["sh", "-c", "python3 -m pip --version || pip --version"],
}


def sh(*cmd, check=True):
    return subprocess.run(cmd, check=check, capture_output=True, text=True)


def drop_caches():
    os.sync()
    open("/proc/sys/vm/drop_caches", "w").write("3\n")


class Backend:
    def __init__(self, fetch, extra=()):
        subprocess.run(["pkill", "-f", "s3nbd.py"])
        time.sleep(0.3)
        shutil.rmtree(f"{S12}/cache", ignore_errors=True)
        self.log = open(f"/data/logs/s3nbd-{fetch}.log", "a")
        self.p = subprocess.Popen(["python3", "/root/s12/s3nbd.py", "--fetch", fetch, "--sock", SOCK, *extra],
                                  stdout=subprocess.PIPE, stderr=self.log, text=True)
        line = self.p.stdout.readline()
        assert line.startswith("LISTENING"), line
        self.tls = threading.local()

    def call(self, **req):
        s = getattr(self.tls, "s", None)
        if s is None:
            s = socket.socket(socket.AF_UNIX)
            s.connect(SOCK)
            self.tls.s = s
            self.tls.f = s.makefile("rb")
        s.sendall((json.dumps(req) + "\n").encode())
        return json.loads(self.tls.f.readline())

    def cpu(self):
        parts = open(f"/proc/{self.p.pid}/stat").read().rsplit(")", 1)[1].split()
        return (int(parts[11]) + int(parts[12])) / os.sysconf("SC_CLK_TCK")

    def close(self):
        try:
            self.call(op="quit")
        except Exception:  # noqa: BLE001
            pass
        self.p.wait(10)


DEV_LOCK = threading.Lock()
NEXT_DEV = [0]


def next_dev():
    with DEV_LOCK:
        d = NEXT_DEV[0] % 1000
        NEXT_DEV[0] += 1
    return f"/dev/nbd{d}"


def config(idx):
    s = SAMPLE[idx]
    return json.load(open(f"/data/oci/blobs/sha256/{s['manifest']['config']['digest'].split(':')[1]}")).get("config", {})


def runsc(root, cfg, argv, cid):
    b = f"/data/mt/bundle-{cid}"
    shutil.rmtree(b, ignore_errors=True)
    os.makedirs(b)
    sh(RUNSC, "spec", "--bundle", b)
    spec = json.load(open(f"{b}/config.json"))
    spec["root"] = {"path": root, "readonly": False}
    spec["process"]["args"] = argv
    spec["process"]["terminal"] = False
    spec["process"]["env"] = cfg.get("Env") or ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"]
    spec["process"]["cwd"] = cfg.get("WorkingDir") or "/"
    spec["process"]["user"] = {"uid": 0, "gid": 0}
    json.dump(spec, open(f"{b}/config.json", "w"))
    t = time.time()
    r = sh(RUNSC, "--root=/run/s12-runsc", "--platform=systrap", "--network=none", "run", "--bundle", b, cid, check=False)
    wall = time.time() - t
    sh(RUNSC, "--root=/run/s12-runsc", "delete", "-force", cid, check=False)
    shutil.rmtree(b, ignore_errors=True)
    return {"wall": wall, "rc": r.returncode, "out": (r.stdout + r.stderr)[-200:]}


class Sandbox:
    def __init__(self, be, idx, mode, tag, trace=None, record=None):
        self.be, self.idx, self.tag = be, idx, tag
        self.base = f"/data/mt/{tag}"
        shutil.rmtree(self.base, ignore_errors=True)
        os.makedirs(self.base)
        self.dev = next_dev()
        self.record = record
        t0 = time.time()
        r = be.call(op="attach", dev=self.dev, name=f"img-{idx:03d}", mode=mode, trace=trace)
        if not r.get("ok"):
            raise RuntimeError(f"attach {idx}: {r}")
        self.attach = r
        os.makedirs(f"{self.base}/lower")
        m = sh("mount", "-t", "erofs", "-o", "ro", self.dev, f"{self.base}/lower", check=False)
        if m.returncode:
            raise RuntimeError(f"mount {idx}: {m.stderr}")
        for d in ("upper", "work", "rootfs"):
            os.makedirs(f"{self.base}/{d}")
        sh("mount", "-t", "overlay", "overlay", "-o",
           f"lowerdir={self.base}/lower,upperdir={self.base}/upper,workdir={self.base}/work", f"{self.base}/rootfs")
        self.attach_wall = time.time() - t0
        self.root = f"{self.base}/rootfs"

    def close(self):
        sh("umount", self.root, check=False)
        sh("umount", f"{self.base}/lower", check=False)
        r = self.be.call(op="detach", dev=self.dev, trace_out=self.record)
        shutil.rmtree(self.base, ignore_errors=True)
        return r


def seq(args):
    os.makedirs(f"{S12}/traces", exist_ok=True)
    out = []
    for rep in range(args.reps):
        for idx in map(int, args.images.split(",")):
            cfg = config(idx)
            for mode_s in args.modes.split(","):
                fetch, mode = mode_s.split(":")
                extra = []
                if fetch.startswith("s3h"):
                    extra = ["--hedge-ms", fetch[3:] or "150"]
                    fetch = "s3"
                for cmd in args.cmds.split(","):
                    trace_file = f"{S12}/traces/{idx:03d}-{cmd}.json"
                    be = Backend(fetch, extra)
                    drop_caches()
                    rec = {"idx": idx, "image": SAMPLE[idx]["image"], "family": SAMPLE[idx]["family"], "fetch": mode_s.split(":")[0],
                           "mode": mode, "cmd": cmd, "rep": rep}
                    try:
                        sb = Sandbox(be, idx, mode, f"seq-{idx}", trace=trace_file if mode == "trace" else None,
                                     record=trace_file if (mode_s == "s3:readaround" and rep == 0) else None)
                        rec["attach_wall"] = sb.attach_wall
                        rec["attach"] = sb.attach
                        rec["cold"] = runsc(sb.root, cfg, CMDS[cmd], f"s12-{idx}-a")
                        rec["stats_after_cold"] = be.call(op="stats", reset=False)
                        rec["warm"] = runsc(sb.root, cfg, CMDS[cmd], f"s12-{idx}-b")
                        rec["device"] = sb.close()
                        rec["stats"] = be.call(op="stats")
                        rec["backend_cpu_s"] = be.cpu()
                    except Exception as e:  # noqa: BLE001
                        rec["error"] = repr(e)[-600:]
                    be.close()
                    out.append(rec)
                    c = rec.get("stats_after_cold", {}).get("counters", {})
                    print(json.dumps({k: rec.get(k) for k in ("idx", "fetch", "mode", "cmd", "rep", "attach_wall", "error")}),
                          "cold", round(rec.get("cold", {}).get("wall", -1), 3), rec.get("cold", {}).get("rc"),
                          "warm", round(rec.get("warm", {}).get("wall", -1), 3),
                          "gets", c.get("gets_demand"), c.get("gets_prefetch"),
                          "MB", round((c.get("fetched_bytes_demand", 0) + c.get("fetched_bytes_prefetch", 0)) / 1e6, 2),
                          "touchedMB", round(rec.get("device", {}).get("touched_bytes_c", 0) / 1e6, 2), flush=True)
                    json.dump(out, open(f"/data/results/coldseq-{args.tag}.json", "w"), indent=1)


def cpu_jiffies():
    v = list(map(int, open("/proc/stat").readline().split()[1:]))
    return sum(v) - v[3] - v[4], sum(v)


def burst(args):
    idxs = json.load(open(args.images_file))[: args.n]
    fetch, mode = args.mode.split(":")
    extra = []
    if fetch.startswith("s3h"):
        extra, fetch = ["--hedge-ms", fetch[3:] or "150"], "s3"
    be = Backend(fetch, extra)
    drop_caches()
    os.makedirs(f"{S12}/traces-burst", exist_ok=True)
    b0, t_all0 = cpu_jiffies(), time.time()
    cpu0 = be.cpu()

    def one(k, idx):
        r = {"idx": idx, "family": SAMPLE[idx]["family"]}
        t0 = time.time()
        tr = f"{S12}/traces-burst/{idx:03d}.json"
        try:
            sb = Sandbox(be, idx, mode, f"burst-{k}", trace=tr if args.trace else None,
                         record=None if args.trace else tr)
            r["attach_s"] = time.time() - t0
            r["attach"] = sb.attach
            cfg = config(idx)
            r["import_sys"] = runsc(sb.root, cfg, CMDS["import_sys"], f"s12b-{k}-a")
            r["first_command_done_s"] = time.time() - t0
            r["pip_version"] = runsc(sb.root, cfg, CMDS["pip_version"], f"s12b-{k}-b")
            r["all_done_s"] = time.time() - t0
            r["_sb"] = sb
        except Exception as e:  # noqa: BLE001
            r["error"] = repr(e)[-600:]
        return r
    with ThreadPoolExecutor(len(idxs)) as ex:
        rs = list(ex.map(lambda a: one(*a), enumerate(idxs)))
    wall = time.time() - t_all0
    b1 = cpu_jiffies()
    stats = be.call(op="stats", raw=False)
    cpu1 = be.cpu()
    for r in rs:
        sb = r.pop("_sb", None)
        if sb:
            r["device"] = sb.close()
    be.close()
    ok = [r for r in rs if "error" not in r]

    def med(xs):
        xs = sorted(xs)
        return xs[len(xs) // 2] if xs else None
    summary = {"n": len(idxs), "ok": len(ok), "mode": args.mode, "trace": args.trace, "wall_s": wall,
               "attach_median": med([r["attach_s"] for r in ok]), "attach_max": max((r["attach_s"] for r in ok), default=None),
               "first_cmd_median": med([r["first_command_done_s"] for r in ok]),
               "first_cmd_max": max((r["first_command_done_s"] for r in ok), default=None),
               "pip_median": med([r["pip_version"]["wall"] for r in ok]),
               "all_done_max": max((r["all_done_s"] for r in ok), default=None),
               "host_busy_cpu_s": (b1[0] - b0[0]) / os.sysconf("SC_CLK_TCK"),
               "backend_cpu_s": cpu1 - cpu0,
               "cmd_failures": sum(1 for r in ok if r["import_sys"]["rc"] or r["pip_version"]["rc"])}
    print(json.dumps(summary), flush=True)
    print(json.dumps({k: stats[k] for k in stats if k.startswith("get_") or k == "counters"}), flush=True)
    json.dump({"summary": summary, "backend": stats, "sandboxes": rs},
              open(f"/data/results/burst-{args.tag}.json", "w"), indent=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["seq", "burst"])
    ap.add_argument("--images", default="0,60,63,72,90,130,170")
    ap.add_argument("--images-file", default="/root/s12/burst-images.json")
    ap.add_argument("--modes", default="local:demand,s3:demand,s3:readaround,s3:trace")
    ap.add_argument("--cmds", default="import_sys,git_status,pip_version")
    ap.add_argument("--reps", type=int, default=2)
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--mode", default="s3:readaround")
    ap.add_argument("--trace", action="store_true")
    ap.add_argument("--tag", default="main")
    args = ap.parse_args()
    os.makedirs("/data/mt", exist_ok=True)
    seq(args) if args.what == "seq" else burst(args)


if __name__ == "__main__":
    main()
