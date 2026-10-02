"""S11 step 3: fscache mounts — content vs OCI layers, domain blob sharing, digest validation,
nydusd kill/restart with and without a supervisor holding /dev/cachefiles."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import sys
import tarfile
import threading
import time

sys.path.insert(0, "/root/s11")
import s11lib as L  # noqa: E402

OUT = L.W / "out"
IMAGES = json.load(open(L.W / "images.json"))["images"]
report = {}


def save():
    (OUT / "verify-fscache.json").write_text(json.dumps(report, indent=1, default=str))


def reset_fscache():
    L.unmount_all(L.MNT_DIR)
    subprocess.run(["pkill", "-9", "-x", "nydusd"])
    time.sleep(0.5)
    shutil.rmtree(L.FSCACHE_DIR, ignore_errors=True)
    shutil.rmtree(L.BOOT_DIR, ignore_errors=True)
    if L.NYDUSD_SOCK.exists():
        L.NYDUSD_SOCK.unlink()
    L.drop_caches()


# ------------------------------------------------------------- 1. contents vs OCI layers
def sample_files(root, n=40, seed=1):
    files = []
    for dirpath, dirnames, filenames in os.walk(root):
        for f in filenames:
            p = Path(dirpath) / f
            if p.is_symlink() or not p.is_file():
                continue
            files.append(p)
    rnd = random.Random(seed)
    big = sorted(files, key=lambda p: p.stat().st_size, reverse=True)[:5]
    pick = set(rnd.sample(files, min(n, len(files)))) | set(big)
    return sorted(pick), len(files)


def oci_layer_lookup(prepared_reference, rel_paths):
    """sha256 of each path in the flattened OCI image (top layer wins; whiteouts honored)."""
    repo, digest = L.split_ref(prepared_reference)
    m = L.manifest(L.PROXY_REG, repo, digest)
    want = set(rel_paths)
    found, hidden = {}, set()
    for layer in reversed(m["layers"]):
        if not want - set(found) - hidden:
            break
        data, _ = L.reg_get(L.PROXY_REG, f"/v2/{repo}/blobs/{layer['digest']}")
        mt = layer["mediaType"]
        if "zstd" in mt:
            data = subprocess.run(["zstd", "-dc"], input=data, capture_output=True, check=True).stdout
            mode = "r:"
        elif "gzip" in mt or data[:2] == b"\x1f\x8b":
            mode = "r:gz"
        else:
            mode = "r:"
        import io
        whiteouts, opaque = set(), set()
        with tarfile.open(fileobj=io.BytesIO(data), mode=mode) as tar:
            for member in tar:
                name = member.name.lstrip("./").lstrip("/")
                base = os.path.basename(name)
                parent = os.path.dirname(name)
                if base == ".wh..wh..opq":
                    opaque.add(parent)
                    continue
                if base.startswith(".wh."):
                    whiteouts.add(os.path.join(parent, base[4:]))
                    continue
                if name in want and name not in found and name not in hidden:
                    if member.isfile():
                        found[name] = hashlib.sha256(tar.extractfile(member).read()).hexdigest()
                    elif member.islnk():
                        found[name] = ("hardlink", member.linkname.lstrip("./"))
                    else:
                        found[name] = ("type", member.type)
        # Lower layers cannot provide what this layer whited out or hid below an opaque dir.
        for w in want - set(found):
            if w in whiteouts or any(w == o or w.startswith(o + "/") for o in opaque):
                hidden.add(w)
    # Resolve hardlinks within the image.
    for k, v in list(found.items()):
        if isinstance(v, tuple) and v[0] == "hardlink":
            found[k] = found.get(v[1], v)
    return found


def check_contents():
    res = {}
    nyd = L.Nydusd(threads=16, log="nydusd-verify.log")
    nyd.start()
    try:
        for img in IMAGES:
            name = img["name"]
            mnt, t = L.fscache_attach(name)
            files, total = sample_files(mnt)
            rel = [str(p.relative_to(mnt)) for p in files]
            got = {r: L.sha256_file(mnt / r) for r in rel}
            ref = oci_layer_lookup(img["prepared_reference"], rel)
            mism = [r for r in rel if ref.get(r) != got[r]]
            res[name] = {"files_in_tree": total, "sampled": len(rel), "matched": len(rel) - len(mism),
                         "mismatches": [(r, got[r], ref.get(r)) for r in mism][:10], "attach": t}
            print("contents", name, res[name]["matched"], "/", len(rel), flush=True)
        report["contents_vs_oci"] = res
        save()
    finally:
        nyd.kill(15)
        reset_fscache()


def check_sharing():
    """Mount all 20 images in one domain; count blob references vs fscache objects."""
    reset_fscache()
    nyd = L.Nydusd(threads=16, log="nydusd-sharing.log")
    nyd.start()
    try:
        for img in IMAGES:
            L.fscache_attach(img["name"])
        # 2. Domain sharing: every image is now mounted in domain s11.
        blobs_per_image = {}
        for img in IMAGES:
            _, _, blobs = L.nydus_layers(img["name"])
            blobs_per_image[img["name"]] = [b["digest"].split(":")[1] for b in blobs]
        refs = sum(len(v) for v in blobs_per_image.values())
        distinct = len({b for v in blobs_per_image.values() for b in v})
        cache_files = [p for p in L.FSCACHE_DIR.rglob("*") if p.is_file()]
        data_cache_files = [p for p in cache_files if p.name.startswith("D")]
        shared = {}
        for n, v in blobs_per_image.items():
            for b in v:
                shared.setdefault(b, []).append(n)
        report["domain_sharing"] = {
            "images": len(IMAGES), "blob_references": refs, "distinct_blobs": distinct,
            "blobs_shared_by_2plus_images": {b[:16]: ns for b, ns in shared.items() if len(ns) > 1},
            "fscache_files": len(cache_files), "fscache_data_files": len(data_cache_files),
            "fscache_dir_tree_sample": sorted(str(p.relative_to(L.FSCACHE_DIR)) for p in cache_files)[:8],
        }
        save()
    finally:
        nyd.kill(15)
        reset_fscache()


# ------------------------------------------------------------- 3. chunk-dict sharing within a domain
def check_chunk_dict_sharing():
    res = {}
    for base, child in (("scaleswe-responses-0", "scaleswe-responses-1-dict"), ("scaleswe-oauthlib-0", "scaleswe-oauthlib-1-dict")):
        reset_fscache()
        nyd = L.Nydusd(threads=16, log="nydusd-dict.log")
        nyd.start()
        try:
            _, _, bblobs = L.nydus_layers(base)
            _, _, cblobs = L.nydus_layers(child)
            bset = {b["digest"] for b in bblobs}
            cset = {b["digest"] for b in cblobs}
            mb, _ = L.fscache_attach(base)
            c0 = L.nft_counters()["nydus_tx"]
            subprocess.run(["sh", "-c", f"find {mb} -type f -print0 | xargs -0 cat > /dev/null"], check=True)
            c1 = L.nft_counters()["nydus_tx"]
            mc, _ = L.fscache_attach(child)
            c2 = L.nft_counters()["nydus_tx"]
            subprocess.run(["sh", "-c", f"find {mc} -type f -print0 | xargs -0 cat > /dev/null"], check=True)
            c3 = L.nft_counters()["nydus_tx"]
            child_size = sum(b["size"] for b in cblobs if b["digest"] not in bset)
            res[child] = {"base": base, "child_blobs": len(cset), "child_blobs_shared_with_base": len(cset & bset),
                          "child_new_blob_bytes_compressed": child_size,
                          "base_full_read_fetched_bytes": c1 - c0,
                          "child_full_read_after_base_fetched_bytes": c3 - c2,
                          "child_bootstrap_fetch_bytes": c2 - c1}
            print("dict", child, res[child], flush=True)
        finally:
            nyd.kill(15)
    report["chunk_dict_domain_sharing"] = res
    save()
    reset_fscache()


# ------------------------------------------------------------- 4. digest validation
def check_digest_validation():
    name = "tlego-3-raw"
    res = {}
    reset_fscache()
    m, boot, blobs = L.nydus_layers(name)
    lfs = L.W / "localfs"
    shutil.rmtree(lfs, ignore_errors=True)
    lfs.mkdir()
    for b in blobs:
        data, _ = L.reg_get(L.NYDUS_REG, f"/v2/s11/{name}/blobs/{b['digest']}")
        (lfs / b["digest"].split(":")[1]).write_bytes(data)
    L.fetch_bootstrap(name)
    # Reference tree from a clean, validated read over the registry backend.
    nyd = L.Nydusd(threads=8, log="nydusd-validate.log")
    nyd.start()
    try:
        good, _ = L.fscache_attach(name, domain="good", fetch=False)
        ref = {}
        for dirpath, _, fns in os.walk(good):
            for f in fns:
                p = Path(dirpath) / f
                if p.is_file() and not p.is_symlink():
                    ref[str(p.relative_to(good))] = L.sha256_file(p)
        # Corrupt the largest blob: flip one byte every 64 KiB in its middle third.
        target = max(lfs.iterdir(), key=lambda p: p.stat().st_size)
        raw = bytearray(target.read_bytes())
        flipped = 0
        for off in range(len(raw) // 3, 2 * len(raw) // 3, 65536):
            raw[off] ^= 0xFF
            flipped += 1
        target.write_bytes(bytes(raw))
        res["corrupted_blob"] = {"blob": target.name[:16], "size": len(raw), "bytes_flipped": flipped}
        for validate in (True, False):
            dom = "val1" if validate else "val0"
            backend = {"type": "localfs", "localfs": {"dir": str(lfs)}}
            mnt, _ = L.fscache_attach(name, domain=dom, validate=validate, backend=backend, fetch=False)
            ok = bad = eio = 0
            errs = []
            for r, h in ref.items():
                try:
                    got = L.sha256_file(mnt / r)
                    if got == h:
                        ok += 1
                    else:
                        bad += 1
                except OSError as exc:
                    eio += 1
                    if len(errs) < 3:
                        errs.append(f"{r}: {exc}")
            res[f"validate_{validate}"] = {"files": len(ref), "correct": ok, "silently_wrong": bad, "read_errors": eio,
                                           "errors": errs}
            print("validate", validate, res[f"validate_{validate}"], flush=True)
        log = (L.W / "logs/nydusd-validate.log").read_text(errors="replace")
        res["nydusd_log_digest_lines"] = [l for l in log.splitlines() if "digest" in l.lower()][:5]
    finally:
        nyd.kill(15)
    report["digest_validation"] = res
    save()
    reset_fscache()


# ------------------------------------------------------------- 5. daemon death mid-read
class Reader(threading.Thread):
    """Reads every file of a tree, recording progress, errors and stalls."""
    def __init__(self, root, ref=None):
        super().__init__(daemon=True)
        self.root, self.ref = root, ref
        self.done = self.errors = self.wrong = 0
        self.last_progress = time.monotonic()
        self.finished = False
        self.first_errors = []

    def run(self):
        for dirpath, _, fns in os.walk(self.root):
            for f in fns:
                p = Path(dirpath) / f
                if p.is_symlink() or not p.is_file():
                    continue
                try:
                    h = L.sha256_file(p)
                    if self.ref is not None and self.ref.get(str(p.relative_to(self.root))) not in (None, h):
                        self.wrong += 1
                except OSError as exc:
                    self.errors += 1
                    if len(self.first_errors) < 3:
                        self.first_errors.append(f"{p.relative_to(self.root)}: {exc}")
                self.done += 1
                self.last_progress = time.monotonic()
        self.finished = True

    def state(self):
        return {"files_done": self.done, "errors": self.errors, "wrong": self.wrong, "finished": self.finished,
                "seconds_since_progress": round(time.monotonic() - self.last_progress, 2),
                "first_errors": self.first_errors}


class Supervisor:
    """What nydus-snapshotter does: holds nydusd's state and the /dev/cachefiles fd across a crash."""
    def __init__(self):
        if L.SUP_SOCK.exists():
            L.SUP_SOCK.unlink()
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.bind(str(L.SUP_SOCK))
        self.sock.listen(4)
        self.data, self.fds = b"", []

    def receive_async(self):
        def go():
            conn, _ = self.sock.accept()
            msg, fds, _, _ = socket.recv_fds(conn, 65536, 16)
            rest = b""
            while chunk := conn.recv(65536):
                rest += chunk
            conn.close()
            for fd in self.fds:
                os.close(fd)
            self.data, self.fds = msg + rest, list(fds)
        t = threading.Thread(target=go, daemon=True)
        t.start()
        return t

    def send_async(self):
        def go():
            conn, _ = self.sock.accept()
            socket.send_fds(conn, [self.data], self.fds)
            conn.close()
        t = threading.Thread(target=go, daemon=True)
        t.start()
        return t


def reference_tree(name):
    nyd = L.Nydusd(threads=16, log="nydusd-ref.log")
    nyd.start()
    try:
        mnt, _ = L.fscache_attach(name, domain="ref")
        ref = {}
        for dirpath, _, fns in os.walk(mnt):
            for f in fns:
                p = Path(dirpath) / f
                if p.is_file() and not p.is_symlink():
                    ref[str(p.relative_to(mnt))] = L.sha256_file(p)
        return ref
    finally:
        nyd.kill(15)
        reset_fscache()


def check_failover():
    name = "scaleswe-traitlets-0"
    res = {}
    reset_fscache()
    ref = reference_tree(name)
    res["reference_files"] = len(ref)
    # (a) no supervisor: kill -9 mid-read, start a fresh nydusd.
    nyd = L.Nydusd(threads=16, log="nydusd-kill-a.log")
    nyd.start()
    mnt, _ = L.fscache_attach(name)
    rd = Reader(mnt, ref)
    rd.start()
    time.sleep(1.5)
    before = rd.state()
    nyd.kill(9)
    time.sleep(5)
    after_kill = rd.state()
    nyd2 = L.Nydusd(threads=16, log="nydusd-kill-a2.log")
    t0 = time.monotonic()
    try:
        nyd2.start()
        restart = {"started": True, "seconds": round(time.monotonic() - t0, 2)}
    except RuntimeError as exc:
        restart = {"started": False, "error": str(exc)[-600:]}
    time.sleep(5)
    after_restart = rd.state()
    rd.join(timeout=60)
    final = rd.state()
    # Does a new read on the old mount work? Does a remount work?
    probe = {}
    try:
        L.sha256_file(next(p for p in Path(mnt).rglob("*.py") if p.is_file()))
        probe["old_mount_read"] = "ok"
    except Exception as exc:  # noqa: BLE001
        probe["old_mount_read"] = repr(exc)
    r = L.run(["umount", mnt], check=False)
    probe["umount_rc"] = r.returncode
    if restart["started"]:
        try:
            mnt2, _ = L.fscache_attach(name, fetch=False)
            rd2 = Reader(mnt2, ref)
            rd2.run()
            probe["remount_full_read"] = rd2.state()
        except Exception as exc:  # noqa: BLE001
            probe["remount_full_read"] = repr(exc)
    res["no_supervisor"] = {"before_kill": before, "5s_after_kill": after_kill, "restart": restart,
                            "5s_after_restart": after_restart, "final": final, "probe": probe}
    print("failover a", json.dumps(res["no_supervisor"])[:1500], flush=True)
    nyd2.kill(15)
    reset_fscache()
    save()
    # (b) with a supervisor holding the fd, then --upgrade + takeover.
    sup = Supervisor()
    nyd = L.Nydusd(threads=16, log="nydusd-kill-b.log", supervisor=True)
    nyd.start()
    mnt, _ = L.fscache_attach(name)
    t = sup.receive_async()
    L.api("PUT", "/api/v1/daemon/fuse/sendfd")
    t.join(timeout=10)
    saved = {"state_bytes": len(sup.data), "fds": len(sup.fds)}
    rd = Reader(mnt, ref)
    rd.start()
    time.sleep(1.5)
    before = rd.state()
    nyd.kill(9)
    time.sleep(5)
    after_kill = rd.state()
    nyd2 = L.Nydusd(threads=16, log="nydusd-kill-b2.log", supervisor=True, upgrade=True)
    t0 = time.monotonic()
    steps = {}
    try:
        nyd2.start(wait=False)
        deadline = time.monotonic() + 20
        while True:
            try:
                steps["upgrade_state"] = L.api("GET", "/api/v1/daemon").get("state")
                break
            except Exception:  # noqa: BLE001
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)
        ts = sup.send_async()
        L.api("PUT", "/api/v1/daemon/fuse/takeover")
        ts.join(timeout=10)
        steps["after_takeover"] = L.api("GET", "/api/v1/daemon").get("state")
        try:
            L.api("PUT", "/api/v1/daemon/start")
        except RuntimeError as exc:
            steps["start_error"] = str(exc)[:300]
        steps["after_start"] = L.api("GET", "/api/v1/daemon").get("state")
        steps["seconds"] = round(time.monotonic() - t0, 2)
    except Exception as exc:  # noqa: BLE001
        steps["error"] = repr(exc)[-600:]
    time.sleep(5)
    after_restart = rd.state()
    rd.join(timeout=120)
    final = rd.state()
    res["with_supervisor"] = {"saved": saved, "before_kill": before, "5s_after_kill": after_kill,
                              "takeover": steps, "5s_after_takeover": after_restart, "final": final}
    print("failover b", json.dumps(res["with_supervisor"])[:1500], flush=True)
    nyd2.kill(15)
    report["failover"] = res
    save()
    reset_fscache()


if __name__ == "__main__":
    steps = sys.argv[1:] or ["contents", "dict", "validate", "failover"]
    prior = OUT / "verify-fscache.json"
    if prior.exists():
        report.update(json.loads(prior.read_text()))
    for s in steps:
        t0 = time.monotonic()
        try:
            {"contents": check_contents, "sharing": check_sharing, "dict": check_chunk_dict_sharing,
             "validate": check_digest_validation, "failover": check_failover}[s]()
        except Exception as exc:  # noqa: BLE001
            import traceback
            report.setdefault("errors", {})[s] = traceback.format_exc()[-3000:]
            print("STEP FAILED", s, traceback.format_exc(), flush=True)
            reset_fscache()
        report.setdefault("step_seconds", {})[s] = round(time.monotonic() - t0, 1)
        save()
    print("VERIFY_DONE", flush=True)
