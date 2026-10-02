#!/usr/bin/env python3
"""S12 (c)/(e): chunk-store NBD backend. One process serves many /dev/nbdN devices (as the node's
storage process does), with one chunk cache keyed by chunk id shared by every device.

  s3nbd.py --sock /run/s12nbd.sock --fetch s3|local [--cache-dir /data/s12/cache] [--misses 32]
           [--prefetch-slots 8] [--mem-bytes N] [--fadvise] [--no-disk-cache]

Read path (design §4): the bootstrap region is served from the verified local bootstrap; elsewhere a
binary search of the image's chunk map gives chunk ids; each id is looked up in memory, then on disk
(verified again), then joins an in-flight fetch or is planned into a window:
  demand      group misses by pack, sort by offset, merge across gaps < 64 KiB while <= 1 MiB
  readaround  demand, then extend each window with other uncached chunks of this image's map in the
              same pack, up to 1 MiB
  trace       readaround, plus at attach a replay of a recorded chunk-id trace as pack-coalesced
              ranges of <= 8 MiB, on at most --prefetch-slots of the --misses fetch slots
Every chunk is zstd-decompressed (output capped at ulen) and checked (len == ulen, sha256 == id)
before it is installed or served. S3 reads are ranged GETs on presigned URLs: the backend holds no key.

Control: JSON lines on --sock: attach / detach / stats / quit.
"""
import argparse
import bisect
import errno
import fcntl
import hashlib
import json
import os
import socket
import socketserver
import struct
import sys
import threading
import time
from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from compression import zstd

sys.path.insert(0, "/root/s12")
import packfmt  # noqa: E402
import s3lib  # noqa: E402

REQUEST = struct.Struct("!II8sQI")
REPLY = struct.Struct("!II8s")
SET_SOCK, SET_BLKSIZE, SET_SIZE, DO_IT, CLEAR_SOCK = (0xab00 + n for n in range(5))
SET_SIZE_BLOCKS, DISCONNECT, SET_TIMEOUT, SET_FLAGS = 0xab07, 0xab08, 0xab09, 0xab0a
GAP = 64 << 10
WINDOW = 1 << 20
PF_RANGE = 8 << 20
PF_GAP = 256 << 10
S12 = "/data/s12"


def now():
    return time.perf_counter()


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        with getattr(self, "lock", threading.Lock()):
            self.c = {"gets_demand": 0, "gets_prefetch": 0, "fetched_bytes_demand": 0, "fetched_bytes_prefetch": 0,
                      "chunks_fetched": 0, "chunks_fetched_bytes_c": 0, "chunks_fetched_bytes_u": 0,
                      "mem_hits": 0, "disk_hits": 0, "joined": 0, "misses": 0, "verify_failures": 0,
                      "fetch_errors": 0, "fetch_retries": 0, "hedged": 0, "hedge_won": 0, "status": {}}
            self.lat = {"demand": [], "prefetch": []}  # (ttfb, total, bytes)
            self.fetched_ids = set()

    def add(self, **kw):
        with self.lock:
            for k, v in kw.items():
                self.c[k] += v


STATS = Stats()


class S3Fetcher:
    kind = "s3"

    def __init__(self, hedge_ms=0):
        meta = json.load(open("/root/s12/presigned.json"))
        self.host, self.urls = meta["host"], meta["by_sha"]
        self.packs = json.load(open(f"{S12}/packs.json"))
        self.reader = s3lib.PresignedReader(self.host)
        self.hedge = hedge_ms / 1000
        self.hpool = ThreadPoolExecutor(128, thread_name_prefix="hedge") if hedge_ms else None

    def fetch(self, pno, start, length):
        if not self.hedge:
            return self._fetch(pno, start, length)
        # Hedged GET: if the first request has not completed after `hedge` s, send a duplicate on
        # another connection and take whichever finishes first (latency counted from the first send).
        from concurrent.futures import FIRST_COMPLETED, wait
        t0 = now()
        futs = [self.hpool.submit(self._fetch, pno, start, length)]
        done, _ = wait(futs, timeout=self.hedge)
        if not done:
            STATS.add(hedged=1)
            futs.append(self.hpool.submit(self._fetch, pno, start, length))
            done, _ = wait(futs, return_when=FIRST_COMPLETED)
        f = next(iter(done))
        if f.exception() is not None:
            others = [x for x in futs if x is not f]
            if not others:
                raise f.exception()
            f = others[0]
        body, ttfb, _ = f.result()
        if len(futs) > 1 and f is futs[1]:
            STATS.add(hedge_won=1)
        return body, ttfb, now() - t0

    def _fetch(self, pno, start, length):
        url = self.urls[self.packs[pno]["sha"]]
        for attempt in range(3):
            try:
                st, body, ttfb, total = self.reader.get(url, start, length)
            except Exception:  # noqa: BLE001
                STATS.add(fetch_errors=1)
                if attempt == 2:
                    raise
                continue
            with STATS.lock:
                STATS.c["status"][st] = STATS.c["status"].get(st, 0) + 1
            if st == 206 and len(body) == length:
                return body, ttfb, total
            STATS.add(fetch_retries=1)
            time.sleep(0.05 * 2 ** attempt)
        raise OSError(errno.EIO, f"GET pack {pno} failed: {st}")

    def meta(self, name, suffix):
        _, body, _, _ = self._full(self.urls[f"meta/{name}.{suffix}.zst"])
        return zstd.decompress(body)

    def _full(self, url):
        c = self.reader._conn()
        t0 = now()
        c.request("GET", url)
        r = c.getresponse()
        body = r.read()
        if r.status != 200:
            raise OSError(errno.EIO, f"meta GET {r.status}")
        return r.status, body, None, now() - t0


class LocalFetcher:
    """Loopback baseline: the same packs, read from local NVMe."""
    kind = "local"

    def __init__(self, fadvise=False):
        self.packs = json.load(open(f"{S12}/packs.json"))
        self.fds = {}
        self.lock = threading.Lock()
        self.fadvise = fadvise

    def fd(self, pno):
        with self.lock:
            fd = self.fds.get(pno)
            if fd is None:
                fd = self.fds[pno] = os.open(f"{S12}/packs/{self.packs[pno]['sha']}.pack", os.O_RDONLY)
            return fd

    def fetch(self, pno, start, length):
        fd = self.fd(pno)
        t0 = now()
        body = os.pread(fd, length, start)
        t1 = now()
        if self.fadvise:
            os.posix_fadvise(fd, start, length, os.POSIX_FADV_DONTNEED)
        return body, t1 - t0, t1 - t0

    def meta(self, name, suffix):
        return zstd.decompress(open(f"{S12}/maps/{name}.{suffix}.zst", "rb").read())


class ChunkCache:
    def __init__(self, fetcher, cache_dir, mem_bytes, misses, prefetch_slots, disk=True):
        self.f, self.dir, self.disk = fetcher, cache_dir, disk
        os.makedirs(cache_dir, mode=0o700, exist_ok=True)
        self.mem, self.mem_size, self.mem_cap = OrderedDict(), 0, mem_bytes
        self.lock = threading.Lock()
        self.inflight = {}
        self.slots = threading.BoundedSemaphore(misses)
        self.pf_slots = threading.BoundedSemaphore(prefetch_slots)
        self.pool = ThreadPoolExecutor(misses + prefetch_slots + 8, thread_name_prefix="fetch")
        self.ondisk = set(os.listdir(cache_dir)) if disk else set()

    # ---- lookup
    def _mem_put(self, cid, data):
        if self.mem_cap <= 0:
            return
        with self.lock:
            if cid in self.mem:
                return
            self.mem[cid] = data
            self.mem_size += len(data)
            while self.mem_size > self.mem_cap:
                _, v = self.mem.popitem(last=False)
                self.mem_size -= len(v)

    def _disk_get(self, cid, ulen):
        h = cid.hex()
        if h not in self.ondisk:
            return None
        try:
            with open(f"{self.dir}/{h}", "rb") as fh:
                data = fh.read()
        except FileNotFoundError:
            return None
        if len(data) != ulen or hashlib.sha256(data).digest() != cid:
            STATS.add(verify_failures=1)
            return None
        return data

    def _install(self, cid, data):
        self._mem_put(cid, data)
        if self.disk:
            h = cid.hex()
            tmp = f"{self.dir}/.{h}.{threading.get_ident()}"
            with open(tmp, "wb") as fh:
                fh.write(data)
            os.rename(tmp, f"{self.dir}/{h}")
            self.ondisk.add(h)

    def cached(self, cid):
        return cid in self.mem or cid in self.inflight or cid.hex() in self.ondisk

    # ---- fetch
    def _run_window(self, pno, start, end, chunks, kind):
        sem2 = self.pf_slots if kind == "prefetch" else None
        if sem2:
            sem2.acquire()
        self.slots.acquire()
        try:
            try:
                body, ttfb, total = self.f.fetch(pno, start, end - start)
            except Exception as e:  # noqa: BLE001
                with self.lock:
                    for c in chunks:
                        fut = self.inflight.pop(c[0], None)
                        if fut:
                            fut.set_exception(e)
                return
        finally:
            self.slots.release()
            if sem2:
                sem2.release()
        with STATS.lock:
            STATS.lat[kind].append((ttfb, total, end - start))
            STATS.c["gets_" + kind] += 1
            STATS.c["fetched_bytes_" + kind] += end - start
        for (cid, off, clen, ulen, flags) in chunks:
            payload = body[off - start:off - start + clen]
            try:
                if flags & packfmt.F_ZSTD:
                    data = zstd.decompress(payload)
                else:
                    data = payload
                if len(data) != ulen or hashlib.sha256(data).digest() != cid:
                    raise OSError(errno.EIO, "chunk digest mismatch")
            except Exception as e:  # noqa: BLE001
                STATS.add(verify_failures=1)
                with self.lock:
                    fut = self.inflight.pop(cid, None)
                if fut:
                    fut.set_exception(OSError(errno.EIO, str(e)))
                continue
            self._install(cid, data)
            with STATS.lock:
                STATS.c["chunks_fetched"] += 1
                STATS.c["chunks_fetched_bytes_c"] += clen
                STATS.c["chunks_fetched_bytes_u"] += ulen
                STATS.fetched_ids.add(cid)
            with self.lock:
                fut = self.inflight.pop(cid, None)
            if fut:
                fut.set_result(data)

    @staticmethod
    def plan(entries, gap, window):
        """entries: (pno, off, clen, ulen, flags, cid) -> windows [(pno, start, end, [chunks])]."""
        out = []
        for e in sorted(entries):
            pno, off, clen = e[0], e[1], e[2]
            if out and out[-1][0] == pno and off - out[-1][2] < gap and off + clen - out[-1][1] <= window:
                w = out[-1]
                w[2] = max(w[2], off + clen)
                w[3].append(e)
            else:
                out.append([pno, off, off + clen, [e]])
        return out

    def get(self, ents, img, mode):
        """ents: map entries (dev_off, ulen, cid, pno, off, clen, flags). Returns {cid: bytes}."""
        res, wait, todo = {}, [], []
        for e in ents:
            cid = e[2]
            d = self.mem.get(cid)
            if d is not None:
                res[cid] = d
                STATS.add(mem_hits=1)
                continue
            d = self._disk_get(cid, e[1])
            if d is not None:
                res[cid] = d
                self._mem_put(cid, d)
                STATS.add(disk_hits=1)
                continue
            todo.append(e)
        windows = []
        if todo:
            with self.lock:
                mine = []
                for e in todo:
                    cid = e[2]
                    fut = self.inflight.get(cid)
                    if fut is not None:
                        wait.append((cid, fut))
                        STATS.c["joined"] += 1
                    elif cid not in res:
                        fut = self.inflight[cid] = Future()
                        wait.append((cid, fut))
                        mine.append((e[3], e[4], e[5], e[1], e[6], cid))
                        STATS.c["misses"] += 1
                windows = self.plan(mine, GAP, WINDOW)
                if mode in ("readaround", "trace"):
                    for w in windows:
                        self._extend(w, img)
            for pno, start, end, chunks in windows:
                self.pool.submit(self._run_window, pno, start, end,
                                 [(c[5], c[1], c[2], c[3], c[4]) for c in chunks], "demand")
        for cid, fut in wait:
            res[cid] = fut.result(timeout=120)
        return res

    def _extend(self, w, img):
        """Read-around (called under self.lock): add uncached chunks of this image's map in the
        same pack, forward then backward, while the window stays within 1 MiB."""
        pno = w[0]
        offs, lst = img.by_pack.get(pno, ([], []))
        j = bisect.bisect_left(offs, w[2])
        while j < len(lst):
            off, clen, ulen, flags, cid = lst[j]
            if off + clen - w[1] > WINDOW:
                break
            if not self.cached(cid):
                self.inflight[cid] = Future()
                w[3].append((pno, off, clen, ulen, flags, cid))
                w[2] = off + clen
            j += 1
        j = bisect.bisect_left(offs, w[1]) - 1
        while j >= 0:
            off, clen, ulen, flags, cid = lst[j]
            if w[2] - off > WINDOW:
                break
            if off + clen <= w[1] and not self.cached(cid):
                self.inflight[cid] = Future()
                w[3].append((pno, off, clen, ulen, flags, cid))
                w[1] = off
            j -= 1

    def prefetch(self, img, ids):
        ents = []
        with self.lock:
            for cid in ids:
                e = img.by_id.get(cid)
                if e is None or self.cached(cid):
                    continue
                self.inflight[cid] = Future()
                ents.append((e[3], e[4], e[5], e[1], e[6], cid))
        windows = self.plan(ents, PF_GAP, PF_RANGE)
        for pno, start, end, chunks in windows:
            self.pool.submit(self._run_window, pno, start, end,
                             [(c[5], c[1], c[2], c[3], c[4]) for c in chunks], "prefetch")
        return {"prefetch_chunks": len(ents), "prefetch_ranges": len(windows),
                "prefetch_bytes": sum(w[2] - w[1] for w in windows)}


class Image:
    def __init__(self, name, fetcher):
        t0 = now()
        mraw = fetcher.meta(name, "map")
        self.header, self.ents = packfmt.parse_map(mraw)
        boot = fetcher.meta(name, "boot")
        t1 = now()
        if hashlib.sha256(boot).hexdigest() != self.header["bootstrap_sha256"]:
            raise OSError(errno.EIO, "bootstrap digest mismatch")
        self.meta = boot
        self.size = self.header["device_size"]
        self.offs = [e[0] for e in self.ents]
        self.by_id = {}
        by_pack = {}
        for e in self.ents:
            self.by_id.setdefault(e[2], e)
            by_pack.setdefault(e[3], []).append((e[4], e[5], e[1], e[6], e[2]))
        self.by_pack = {}
        for p, lst in by_pack.items():
            lst = sorted(set(lst))
            self.by_pack[p] = ([x[0] for x in lst], lst)
        self.meta_s, self.parse_s = t1 - t0, now() - t1


class Device:
    def __init__(self, dev, img, cache, mode):
        self.dev, self.img, self.cache, self.mode = dev, img, cache, mode
        self.touched = OrderedDict()
        self.st = {"reads": 0, "read_bytes": 0, "read_s": [], "wait_s": 0.0}
        self.lock = threading.Lock()

    def read(self, off, n):
        t0 = now()
        out = bytearray(n)
        end = off + n
        img = self.img
        if off < len(img.meta):
            m = img.meta[off:min(end, len(img.meta))]
            out[:len(m)] = m
        i = max(bisect.bisect_right(img.offs, off) - 1, 0)
        need = []
        while i < len(img.ents) and img.ents[i][0] < end:
            e = img.ents[i]
            if e[0] + e[1] > off:
                need.append(e)
            i += 1
        if need:
            got = self.cache.get(need, img, self.mode)
            for e in need:
                data = got[e[2]]
                a, b = max(off, e[0]), min(end, e[0] + e[1])
                out[a - off:b - off] = data[a - e[0]:b - e[0]]
                self.touched.setdefault(e[2], None)
        dt = now() - t0
        with self.lock:
            self.st["reads"] += 1
            self.st["read_bytes"] += n
            self.st["read_s"].append(dt)
        return bytes(out)

    def start(self, pool):
        self.fd = os.open(self.dev, os.O_RDWR)
        ks, self.us = socket.socketpair()
        self.ks = ks
        fcntl.ioctl(self.fd, SET_SOCK, ks.fileno())
        fcntl.ioctl(self.fd, SET_BLKSIZE, 4096)
        fcntl.ioctl(self.fd, SET_SIZE_BLOCKS, self.img.size // 4096)
        fcntl.ioctl(self.fd, SET_FLAGS, 1 | 2)
        fcntl.ioctl(self.fd, SET_TIMEOUT, 120)
        self.wlock = threading.Lock()

        def reply(h, err, data=b""):
            with self.wlock:
                self.us.sendall(REPLY.pack(0x67446698, err, h) + data)

        def do_read(h, off, n):
            try:
                reply(h, 0, self.read(off, n))
            except Exception as e:  # noqa: BLE001
                sys.stderr.write(f"{self.dev} read error {off} {n}: {e!r}\n")
                reply(h, errno.EIO)

        def recv(k):
            b = b""
            while len(b) < k:
                x = self.us.recv(k - len(b))
                if not x:
                    raise EOFError
                b += x
            return b

        def serve():
            try:
                while True:
                    magic, cmd, h, off, n = REQUEST.unpack(recv(REQUEST.size))
                    if cmd == 2:
                        break
                    if cmd & 0xffff == 0:
                        pool.submit(do_read, h, off, n)
                    else:
                        if cmd & 0xffff == 1:
                            recv(n)
                        reply(h, errno.EROFS)
            except (EOFError, OSError):
                pass
        threading.Thread(target=serve, daemon=True).start()
        self.kt = threading.Thread(target=lambda: fcntl.ioctl(self.fd, DO_IT), daemon=True)
        self.kt.start()
        name = os.path.basename(self.dev)
        while True:
            try:
                if int(open(f"/sys/block/{name}/size").read()) * 512 == self.img.size and \
                        open(f"/sys/block/{name}/pid").read().strip():
                    break
            except OSError:
                pass
            time.sleep(0.002)

    def stop(self):
        try:
            fcntl.ioctl(self.fd, DISCONNECT)
        except OSError:
            pass
        self.kt.join(5)
        try:
            fcntl.ioctl(self.fd, CLEAR_SOCK)
        except OSError:
            pass
        os.close(self.fd)
        self.us.close()
        self.ks.close()


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))]


class Backend:
    def __init__(self, args):
        self.fetcher = S3Fetcher(args.hedge_ms) if args.fetch == "s3" else LocalFetcher(args.fadvise)
        self.cache = ChunkCache(self.fetcher, args.cache_dir, args.mem_bytes, args.misses, args.prefetch_slots,
                                disk=not args.no_disk_cache)
        self.pool = ThreadPoolExecutor(512, thread_name_prefix="nbd")
        self.devices = {}
        self.lock = threading.Lock()

    def attach(self, req):
        t0 = now()
        img = Image(req["name"], self.fetcher)
        t1 = now()
        d = Device(req["dev"], img, self.cache, req.get("mode", "demand"))
        pf = {}
        if req.get("trace"):
            ids = [bytes.fromhex(x) for x in json.load(open(req["trace"]))]
            pf = self.cache.prefetch(img, ids)
        d.start(self.pool)
        with self.lock:
            self.devices[req["dev"]] = d
        return {"ok": True, "size": img.size, "attach_s": now() - t0, "meta_s": img.meta_s, "parse_s": img.parse_s,
                "nbd_s": now() - t1, **pf}

    def detach(self, req):
        with self.lock:
            d = self.devices.pop(req["dev"])
        d.stop()
        touched = list(d.touched)
        if req.get("trace_out"):
            with open(req["trace_out"], "w") as f:
                json.dump([c.hex() for c in touched], f)
        tb_c = sum(d.img.by_id[c][5] for c in touched)
        tb_u = sum(d.img.by_id[c][1] for c in touched)
        rs = d.st["read_s"]
        return {"ok": True, "reads": d.st["reads"], "read_bytes": d.st["read_bytes"], "touched_chunks": len(touched),
                "touched_bytes_c": tb_c, "touched_bytes_u": tb_u,
                "touched_packs": len({d.img.by_id[c][3] for c in touched}),
                "read_ms": {"p50": (pct(rs, .5) or 0) * 1e3, "p99": (pct(rs, .99) or 0) * 1e3,
                            "max": (max(rs) if rs else 0) * 1e3, "sum": sum(rs) * 1e3}}

    def stats(self, req):
        with STATS.lock:
            c = dict(STATS.c)
            out = {"counters": c}
            for k, lst in STATS.lat.items():
                tt = [x[0] for x in lst]
                tl = [x[1] for x in lst]
                out[f"get_{k}"] = {"n": len(lst), "bytes": sum(x[2] for x in lst),
                                   "ttfb_ms": {p: (pct(tt, q) or 0) * 1e3 for p, q in (("p50", .5), ("p95", .95), ("p99", .99))},
                                   "total_ms": {p: (pct(tl, q) or 0) * 1e3 for p, q in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))},
                                   "size_p50": pct([x[2] for x in lst], .5)}
            if req.get("raw"):
                out["raw_lat"] = {k: lst for k, lst in STATS.lat.items()}
        ru = os.times()
        out["cpu_s"] = ru.user + ru.system
        if req.get("reset"):
            STATS.reset()
        return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sock", default="/run/s12nbd.sock")
    ap.add_argument("--fetch", choices=["s3", "local"], default="s3")
    ap.add_argument("--cache-dir", default=f"{S12}/cache")
    ap.add_argument("--misses", type=int, default=32)
    ap.add_argument("--prefetch-slots", type=int, default=8)
    ap.add_argument("--mem-bytes", type=int, default=2 << 30)
    ap.add_argument("--fadvise", action="store_true")
    ap.add_argument("--no-disk-cache", action="store_true")
    ap.add_argument("--hedge-ms", type=float, default=0)
    args = ap.parse_args()
    b = Backend(args)
    if os.path.exists(args.sock):
        os.unlink(args.sock)

    class H(socketserver.StreamRequestHandler):
        def handle(self):
            for line in self.rfile:
                req = json.loads(line)
                try:
                    if req["op"] == "quit":
                        self.wfile.write(b'{"ok": true}\n')
                        os._exit(0)
                    resp = getattr(b, req["op"])(req)
                except Exception as e:  # noqa: BLE001
                    resp = {"ok": False, "error": repr(e)[-800:]}
                self.wfile.write((json.dumps(resp) + "\n").encode())

    class S(socketserver.ThreadingUnixStreamServer):
        request_queue_size = 512
        daemon_threads = True
    srv = S(args.sock, H)
    print("LISTENING", args.sock, flush=True)
    srv.serve_forever()


main()
