"""S11 step 4-5: gVisor sandboxes on the fscache path and on our EROFS/NBD path.

  bench.py <fscache|nbd> seq   per image: cold node cache + dropped page cache, attach, start,
                               cold commands, then the same commands warm
  bench.py <fscache|nbd> par   all 20 images at once on a cold node: attach + start + cold commands
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, "/root/s11")
import s11lib as L  # noqa: E402

PATH, PHASE = sys.argv[1], sys.argv[2]
COPIES = int(sys.argv[3]) if len(sys.argv) > 3 else 1
OUT = L.W / "out"
IMAGES = json.load(open(L.W / "images.json"))["images"]
CONFIGS = {img["name"]: L.image_config(img["prepared_reference"])[0] for img in IMAGES}


class Fscache:
    name = "fscache"
    counter = "nydus_tx"

    def __init__(self):
        self.nyd = None
        self.guard, self.locks, self.mounted = threading.Lock(), {}, {}

    def reset(self):
        L.delete_all_containers()
        L.unmount_all(L.SB_DIR)
        L.unmount_all(L.MNT_DIR)
        subprocess.run(["pkill", "-9", "-x", "nydusd"])
        time.sleep(0.3)
        shutil.rmtree(L.FSCACHE_DIR, ignore_errors=True)
        shutil.rmtree(L.BOOT_DIR, ignore_errors=True)
        shutil.rmtree(L.SB_DIR, ignore_errors=True)
        L.drop_caches()
        self.mounted.clear()
        self.fs0 = L.fs_used()
        self.nyd = L.Nydusd(threads=32, log=f"nydusd-bench-{PHASE}.log", log_level="warn")
        self.nyd.start()

    def attach(self, img):
        # One EROFS mount per image, shared by every sandbox of that image (as our store shares lowers).
        with self.guard:
            lock = self.locks.setdefault(img["name"], threading.Lock())
        with lock:
            if img["name"] in self.mounted:
                t0 = time.monotonic()
                return self.mounted[img["name"]], {"attach_s": time.monotonic() - t0, "reused": True}
            mnt, t = L.fscache_attach(img["name"])
            self.mounted[img["name"]] = mnt
            return mnt, t

    def daemon_pids(self):
        return [self.nyd.pid]

    def cache_usage(self):
        return [L.du(L.FSCACHE_DIR)[0], L.fs_used() - self.fs0]

    def daemon_metrics(self):
        out = {}
        for path in ("/api/v1/metrics/backend", "/api/v1/metrics/blobcache"):
            try:
                out[path] = L.api("GET", path)
            except Exception as exc:  # noqa: BLE001
                out[path] = repr(exc)[:200]
        return out

    def close(self):
        L.delete_all_containers()
        L.unmount_all(L.SB_DIR)
        L.unmount_all(L.MNT_DIR)
        if self.nyd:
            self.nyd.kill(15)


class Nbd:
    name = "nbd"
    counter = "proxy_tx"

    def __init__(self):
        self.backend = self.front = None

    def reset(self):
        L.delete_all_containers()
        L.unmount_all(L.SB_DIR)
        L.unmount_all(L.ENVSTORE_ROOT)
        L.unmount_all(L.ENVIO_ROOT)
        if self.backend:
            self.backend.kill()
        subprocess.run(["pkill", "-9", "-f", "nbd_backend.py"])
        time.sleep(0.5)
        for root in (L.ENVSTORE_ROOT, L.ENVIO_ROOT, L.SB_DIR):
            shutil.rmtree(root, ignore_errors=True)
        L.drop_caches()
        self.fs0 = L.fs_used()
        self.backend = L.NbdBackend(cache_bytes=64 * 1024 ** 3, prefetch=True)
        self.backend.start()
        self.front = L.NbdFrontend()

    def attach(self, img):
        return self.front.attach(img["prepared_reference"])

    def daemon_pids(self):
        return [self.backend.pid]

    def cache_usage(self):
        return [L.du(L.ENVIO_ROOT)[0], L.fs_used() - self.fs0]

    def daemon_metrics(self):
        try:
            return self.front.metrics() | {"spike_client_eagain_retries": self.front.connect_retries}
        except Exception as exc:  # noqa: BLE001
            return repr(exc)[:200]

    def close(self):
        L.delete_all_containers()
        L.unmount_all(L.SB_DIR)
        L.unmount_all(L.ENVSTORE_ROOT)
        L.unmount_all(L.ENVIO_ROOT)
        if self.backend:
            self.backend.kill()


def one_sandbox(p, img, cid, warm=True):
    cfg = CONFIGS[img["name"]]
    r = {"image": img["name"], "family": img["family"]}
    t0 = time.monotonic()
    lower, r["attach"] = p.attach(img)
    sb = L.Sandbox(cid, lower, cfg)
    r["start"] = sb.start()
    t_ready = time.monotonic()
    r["ready_s"] = t_ready - t0
    r["cold"] = {}
    for label, argv, cwd in L.cold_commands(cfg):
        r["cold"][label] = sb.exec(argv, cwd)
    r["first_command_done_s"] = t_ready - t0 + r["cold"]["python_import_sys"]["seconds"]
    r["all_cold_done_s"] = time.monotonic() - t0
    if warm:
        r["warm"] = {label: sb.exec(argv, cwd) for label, argv, cwd in L.cold_commands(cfg)}
    r["_sandbox"] = sb
    return r


def snapshot(p):
    busy, total = L.cpu_jiffies()
    return {"t": time.monotonic(), "busy": busy, "total": total,
            "daemon_cpu": sum(L.proc_cpu_seconds(pid) or 0 for pid in p.daemon_pids()),
            "bytes": L.nft_counters()[p.counter]}


def delta(a, b):
    hz = os.sysconf("SC_CLK_TCK")
    return {"wall_s": b["t"] - a["t"], "host_busy_cpu_s": (b["busy"] - a["busy"]) / hz,
            "host_cpu_util": (b["busy"] - a["busy"]) / max(1, b["total"] - a["total"]),
            "daemon_cpu_s": b["daemon_cpu"] - a["daemon_cpu"], "bytes_fetched": b["bytes"] - a["bytes"]}


def main():
    p = Fscache() if PATH == "fscache" else Nbd()
    results = {"path": PATH, "phase": PHASE, "kernel": os.uname().release, "images": []}
    try:
        if PHASE == "seq":
            for i, img in enumerate(IMAGES):
                p.reset()
                a = snapshot(p)
                r = one_sandbox(p, img, f"s11-{PATH}-seq-{i:02d}")
                b = snapshot(p)
                r["host"] = delta(a, b)
                r["daemon_metrics"] = p.daemon_metrics()
                r["cache_usage_bytes"] = p.cache_usage()
                r.pop("_sandbox").stop()
                results["images"].append(r)
                print(PATH, img["name"], f"attach={r['attach']['attach_s']:.3f} ready={r['ready_s']:.2f}",
                      {k: round(v["seconds"], 3) for k, v in r["cold"].items()},
                      {k: round(v["seconds"], 3) for k, v in r["warm"].items()},
                      r["host"]["bytes_fetched"], flush=True)
                (OUT / f"bench-{PATH}-{PHASE}.json").write_text(json.dumps(results, indent=1, default=str))
        else:
            p.reset()
            sampler_stop = threading.Event()
            samples = []

            def sampler():
                while not sampler_stop.is_set():
                    samples.append(snapshot(p))
                    time.sleep(0.5)
            th = threading.Thread(target=sampler, daemon=True)
            a = snapshot(p)
            th.start()
            with ThreadPoolExecutor(len(IMAGES) * COPIES) as ex:
                jobs = [img for img in IMAGES for _ in range(COPIES)]
                futs = [ex.submit(one_sandbox, p, img, f"s11-{PATH}-par-{i:03d}", False) for i, img in enumerate(jobs)]
                rs = []
                for f in futs:
                    try:
                        rs.append(f.result())
                    except Exception as exc:  # noqa: BLE001
                        rs.append({"error": repr(exc)[-800:]})
            b = snapshot(p)
            sampler_stop.set()
            th.join()
            results["host"] = delta(a, b)
            results["daemon_metrics"] = p.daemon_metrics()
            results["cache_usage_bytes"] = p.cache_usage()
            peak = 0.0
            for x, y in zip(samples, samples[1:]):
                peak = max(peak, (y["busy"] - x["busy"]) / max(1, y["total"] - x["total"]))
            results["host"]["peak_cpu_util_0.5s"] = peak
            for r in rs:
                sb = r.pop("_sandbox", None)
                if sb:
                    sb.stop()
            results["images"] = rs
            ok = [r for r in rs if "error" not in r]
            results["summary"] = {
                "sandboxes": len(rs), "ok": len(ok),
                "max_first_command_done_s": max(r["first_command_done_s"] for r in ok) if ok else None,
                "max_all_cold_done_s": max(r["all_cold_done_s"] for r in ok) if ok else None,
            }
            print(PATH, "par", json.dumps(results["summary"]), json.dumps(results["host"]), flush=True)
            results["copies_per_image"] = COPIES
            (OUT / f"bench-{PATH}-{PHASE}{'' if COPIES == 1 else f'-x{COPIES}'}.json").write_text(json.dumps(results, indent=1, default=str))
    finally:
        p.close()


if __name__ == "__main__":
    main()
