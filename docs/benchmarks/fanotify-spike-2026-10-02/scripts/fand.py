#!/usr/bin/env python3
"""S13: chunk-store fill daemon for file-backed EROFS over fanotify pre-content events.

  fand.py --sock /run/s13fan.sock [--window 0|1048576] [--mem-bytes N] [--misses 32]
          [--chunk-cache DIR --fill reflink] [--unmark-when-full] [--workers 64]

One FAN_CLASS_PRE_CONTENT group serves every backing file on the node. A backing file is sparse:
  image     one image's unified address space (bootstrap at 0, blobs at mapped_blkaddr), as S12's NBD
            device exposes it; also used for per-layer RAFS images (layer-<hash> maps);
  multidev  one image's bootstrap file (complete, unmarked) plus one sparse file per blob, shared by
            every image that has the blob, mounted with -o device=.
Before EROFS opens a file, the daemon opens it read-write itself, writes the verified bootstrap and
places an inode mark (FAN_PRE_ACCESS). On an event it maps the range to chunk-map entries; for each
entry not yet filled it fetches the chunk from the local pack store (S12 packs: the store node's
stand-in), decompresses it and checks len == ulen and sha256 == id, writes it at its offset
(pwrite, or FICLONERANGE from a shared on-disk chunk cache with --fill reflink), then answers
FAN_ALLOW. Any failure answers FAN_DENY with EIO, never zeros. Misses on one pack are merged into
windows of at most 1 MiB (gaps under 64 KiB), as S12's backend does; --window extends each fill to
the unfilled entries of the aligned 1 MiB file window around the event.

Control: JSON lines on --sock: attach_image / attach_multidev / detach / stats / quit.
"""
import argparse
import bisect
import errno
import fcntl
import hashlib
import json
import mmap
import os
import resource
import socketserver
import struct
import sys
import threading
import time
from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from compression import zstd

sys.path.insert(0, "/root/s12")
sys.path.insert(0, "/root/s13")
import fan  # noqa: E402
import packfmt  # noqa: E402
import rafs  # noqa: E402

S12 = "/data/s12"
GAP = 64 << 10
WINDOW = 1 << 20
FICLONERANGE = 0x4020940D
CLONE = struct.Struct("<qQQQ")


def now():
    return time.perf_counter()


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


class Stats:
    KEYS = ("events", "events_noop", "events_filled", "events_denied", "event_range_bytes", "fill_chunks",
            "fill_bytes", "clone_chunks", "clone_bytes", "cache_file_writes", "pack_reads", "pack_bytes",
            "mem_hits", "joined", "verify_failures", "fill_waits", "unmarked_full")

    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        self.c = dict.fromkeys(self.KEYS, 0)
        self.lat = []      # event: read -> response, s
        self.ranges = []   # event range counts

    def add(self, **kw):
        with self.lock:
            for k, v in kw.items():
                self.c[k] += v


STATS = Stats()


class Packs:
    """The local pack store (S12 layout), standing in for the store node."""

    def __init__(self):
        self.packs = json.load(open(f"{S12}/packs.json"))
        self.fds, self.lock = {}, threading.Lock()

    def read(self, pno, start, length):
        with self.lock:
            fd = self.fds.get(pno)
            if fd is None:
                fd = self.fds[pno] = os.open(f"{S12}/packs/{self.packs[pno]['sha']}.pack", os.O_RDONLY)
        data = os.pread(fd, length, start)
        STATS.add(pack_reads=1, pack_bytes=length)
        return data


class ChunkSource:
    """Verified chunk bytes by id: memory LRU, single-flight, pack windows <= 1 MiB; optional on-disk
    chunk cache (one file per chunk id) as the reflink source."""

    def __init__(self, mem_bytes, misses, cache_dir=None):
        self.packs = Packs()
        self.mem, self.mem_size, self.mem_cap = OrderedDict(), 0, mem_bytes
        self.lock = threading.Lock()
        self.inflight = {}
        self.pool = ThreadPoolExecutor(misses, thread_name_prefix="fetch")
        self.cache_dir = cache_dir
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
        self.file_inflight = {}

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

    def _window(self, pno, start, end, chunks):
        try:
            body = self.packs.read(pno, start, end - start)
        except Exception as e:  # noqa: BLE001
            with self.lock:
                for c in chunks:
                    f = self.inflight.pop(c[0], None)
                    if f:
                        f.set_exception(e)
            return
        for cid, off, clen, ulen, flags in chunks:
            payload = body[off - start:off - start + clen]
            try:
                data = zstd.decompress(payload) if flags & packfmt.F_ZSTD else payload
                if len(data) != ulen or hashlib.sha256(data).digest() != cid:
                    raise OSError(errno.EIO, "chunk digest mismatch")
            except Exception as e:  # noqa: BLE001
                STATS.add(verify_failures=1)
                with self.lock:
                    f = self.inflight.pop(cid, None)
                if f:
                    f.set_exception(OSError(errno.EIO, str(e)))
                continue
            self._mem_put(cid, data)
            with self.lock:
                f = self.inflight.pop(cid, None)
            if f:
                f.set_result(data)

    @staticmethod
    def plan(entries):
        out = []
        for e in sorted(entries):
            pno, off, clen = e[0], e[1], e[2]
            if out and out[-1][0] == pno and off - out[-1][2] < GAP and off + clen - out[-1][1] <= WINDOW:
                w = out[-1]
                w[2] = max(w[2], off + clen)
                w[3].append(e)
            else:
                out.append([pno, off, off + clen, [e]])
        return out

    def get(self, ents):
        """ents: (file_off, ulen, cid, pno, poff, clen, flags) -> {cid: bytes}."""
        res, wait, mine = {}, [], []
        with self.lock:
            for e in ents:
                cid = e[2]
                if cid in res:
                    continue
                d = self.mem.get(cid)
                if d is not None:
                    res[cid] = d
                    STATS.c["mem_hits"] += 1
                    continue
                f = self.inflight.get(cid)
                if f is not None:
                    STATS.c["joined"] += 1
                else:
                    f = self.inflight[cid] = Future()
                    mine.append((e[3], e[4], e[5], e[1], e[6], cid))
                wait.append((cid, f))
        windows = self.plan(mine)
        futs = [self.pool.submit(self._window, pno, start, end, [(c[5], c[1], c[2], c[3], c[4]) for c in chunks])
                for pno, start, end, chunks in windows[1:]]
        if windows:
            pno, start, end, chunks = windows[0]
            self._window(pno, start, end, [(c[5], c[1], c[2], c[3], c[4]) for c in chunks])
        for f in futs:
            f.result()
        for cid, f in wait:
            res[cid] = f.result(timeout=120)
        return res

    def files(self, ents):
        """Reflink source: make sure each chunk is a file <cache>/<hh>/<id>; returns {cid: path}."""
        out, need = {}, []
        for e in ents:
            h = e[2].hex()
            p = f"{self.cache_dir}/{h[:2]}/{h}"
            out[e[2]] = p
            if not os.path.exists(p):
                need.append(e)
        if need:
            data = self.get(need)
            for e in need:
                p = out[e[2]]
                if os.path.exists(p):
                    continue
                os.makedirs(os.path.dirname(p), exist_ok=True)
                tmp = f"{p}.{threading.get_ident()}"
                with open(tmp, "wb") as fh:
                    # Padded with zeros to a 4 KiB multiple: XFS refuses to clone a partial EOF
                    # block into the middle of the destination (gate 4 align).
                    fh.write(data[e[2]] + b"\0" * (-len(data[e[2]]) % 4096))
                os.rename(tmp, p)
                STATS.add(cache_file_writes=1)
        return out


class Backing:
    ODIRECT = False

    def __init__(self, key, path, ents, size, prefill=(), reuse=False):
        self.key, self.path, self.size = key, path, size
        exists = reuse and os.path.exists(path)
        self.wfd = os.open(path, os.O_RDWR | os.O_CREAT | (0 if exists else os.O_TRUNC), 0o600)
        if not exists:
            os.ftruncate(self.wfd, size)
            for off, data in prefill:
                os.pwrite(self.wfd, data, off)
        self.ino = os.fstat(self.wfd).st_ino
        # --odirect: fills bypass the backing file's page cache (opened, like wfd, before the mark).
        self.dfd = os.open(path, os.O_RDWR | os.O_DIRECT) if Backing.ODIRECT else None
        self.index = ([e[0] for e in ents], ents)  # (offsets, entries), replaced atomically by merge()
        self.filled = set()                          # file offsets of filled entries
        self.lock = threading.Lock()
        self.inflight = {}                           # file offset -> Future
        self.marked = False
        self.users = 0

    @property
    def ents(self):
        return self.index[1]

    @property
    def nfilled(self):
        return len(self.filled)

    def merge(self, ents):
        """Add entries this Backing does not know yet (by file offset)."""
        with self.lock:
            offs, cur = self.index
            have = set(offs)
            extra = [e for e in ents if e[0] not in have]
            if extra:
                merged = sorted(cur + extra)
                self.index = ([e[0] for e in merged], merged)
            return len(extra)

    def overlapping(self, off, end):
        offs, ents = self.index
        i = max(bisect.bisect_right(offs, off) - 1, 0)
        out = []
        while i < len(ents) and ents[i][0] < end:
            if ents[i][0] + ents[i][1] > off:
                out.append(ents[i])
            i += 1
        return out


class Daemon:
    def __init__(self, a):
        self.a = a
        self.src = ChunkSource(a.mem_bytes, a.misses, a.chunk_cache)
        self.gfd = fan.init()
        self.backings, self.by_ino = {}, {}
        self.lock = threading.Lock()
        self.attach_lock = threading.Lock()
        self.pool = ThreadPoolExecutor(a.workers, thread_name_prefix="event")
        threading.Thread(target=self.reader, daemon=True).start()

    # ---- control
    def _load(self, name):
        header, ents = packfmt.parse_map(zstd.decompress(open(f"{S12}/maps/{name}.map.zst", "rb").read()))
        boot = zstd.decompress(open(f"{S12}/maps/{name}.boot.zst", "rb").read())
        if hashlib.sha256(boot).hexdigest() != header["bootstrap_sha256"]:
            raise OSError(errno.EIO, "bootstrap digest mismatch")
        return header, ents, boot

    def _add(self, b):
        fan.mark(self.gfd, b.path)
        b.marked = True
        with self.lock:
            self.backings[b.key] = b
            self.by_ino[b.ino] = b

    def attach_image(self, req):
        t0 = now()
        key = req.get("key", req["path"])
        with self.lock:
            b = self.backings.get(key)
        if b is None:
            header, ents, boot = self._load(req["name"])
            b = Backing(key, req["path"], ents, header["device_size"], prefill=[(0, boot)])
            self._add(b)
        b.users += 1
        return {"ok": True, "path": b.path, "size": b.size, "entries": len(b.ents), "attach_s": now() - t0}

    def attach_multidev(self, req):
        t0 = now()
        header, ents, boot = self._load(req["name"])
        d = req["dir"]
        os.makedirs(f"{d}/boot", exist_ok=True)
        os.makedirs(f"{d}/blobs", exist_ok=True)
        bpath = f"{d}/boot/{req['name']}.boot"
        with open(bpath, "wb") as f:
            f.write(boot)
        info = rafs.read(bpath)
        devices, keys, new = [], [], 0
        for dev in info["devices"]:
            base, size = dev["mapped_blkaddr"] * 4096, dev["blocks"] * 4096
            key = "blob:" + dev["blob_id"]
            mine = [(e[0] - base,) + tuple(e[1:]) for e in ents if base <= e[0] < base + size]
            with self.attach_lock:  # one Backing per blob, even when images attach concurrently
                with self.lock:
                    b = self.backings.get(key)
                if b is None:
                    b = Backing(key, f"{d}/blobs/{dev['blob_id']}.blob", mine, size)
                    self._add(b)
                    new += 1
                else:
                    # Images share a blob but not always the same chunks of it (a file shadowed in one
                    # image is live in another): the blob's entries are the union of its images' maps.
                    b.merge(mine)
                b.users += 1
            devices.append(b.path)
            keys.append(key)
        return {"ok": True, "boot": bpath, "devices": devices, "keys": keys, "new_blobs": new, "attach_s": now() - t0}

    def detach(self, req):
        with self.lock:
            b = self.backings.get(req["key"])
        if b is None:
            return {"ok": False}
        b.users -= 1
        if b.users <= 0:
            if b.marked:
                try:
                    fan.unmark(self.gfd, b.path)
                except OSError:
                    pass
            os.close(b.wfd)
            if b.dfd is not None:
                os.close(b.dfd)
            with self.lock:
                self.backings.pop(b.key, None)
                self.by_ino.pop(b.ino, None)
            if req.get("unlink"):
                os.unlink(b.path)
        return {"ok": True, "filled": b.nfilled, "entries": len(b.ents)}

    def stats(self, req):
        with STATS.lock:
            out = {"counters": dict(STATS.c),
                   "event_ms": {k: (pct(STATS.lat, q) or 0) * 1e3 for k, q in (("p50", .5), ("p90", .9), ("p99", .99), ("max", 1))},
                   "event_range": {k: pct(STATS.ranges, q) for k, q in (("p50", .5), ("p90", .9), ("max", 1))},
                   "n_lat": len(STATS.lat)}
            if req.get("raw"):
                out["raw_lat"] = STATS.lat
        ru = os.times()
        out["cpu_s"] = ru.user + ru.system
        out["filled"] = {k: [b.nfilled, len(b.ents)] for k, b in list(self.backings.items())[:200]}
        if req.get("reset"):
            with STATS.lock:
                STATS.reset()
        return out

    # ---- events
    def reader(self):
        while True:
            buf = os.read(self.gfd, 1 << 20)
            t = now()
            for mask, efd, pid, ranges, infos, *_ in fan.parse(buf):
                if self.a.fast_noop and self.noop(efd, ranges, t):
                    continue
                self.pool.submit(self.handle, efd, ranges, t)

    def noop(self, efd, ranges, t):
        """--fast-noop: answer on the reader thread when every chunk the range overlaps is filled."""
        try:
            b = self.by_ino.get(os.fstat(efd).st_ino)
        except OSError:
            return False
        if b is None or not ranges:
            return False
        for off, count in ranges:
            if any(e[0] not in b.filled for e in b.overlapping(off, off + count)):
                return False
        try:
            fan.respond(self.gfd, efd, fan.FAN_ALLOW)
        finally:
            os.close(efd)
        with STATS.lock:
            STATS.c["events"] += 1
            STATS.c["events_noop"] += 1
            STATS.lat.append(now() - t)
            for _, c in ranges:
                STATS.ranges.append(c)
                STATS.c["event_range_bytes"] += c
        return True

    def handle(self, efd, ranges, t):
        resp = fan.FAN_ALLOW
        try:
            b = self.by_ino.get(os.fstat(efd).st_ino)
            if b is not None:
                for off, count in ranges:
                    self.fill(b, off, off + count)
        except Exception as e:  # noqa: BLE001
            resp = fan.deny_errno(errno.EIO)
            STATS.add(events_denied=1)
            sys.stderr.write(f"fill failed: {e!r}\n")
        try:
            fan.respond(self.gfd, efd, resp)
        finally:
            os.close(efd)
        with STATS.lock:
            STATS.c["events"] += 1
            STATS.lat.append(now() - t)
            for _, c in ranges:
                STATS.ranges.append(c)
                STATS.c["event_range_bytes"] += c

    def fill(self, b, off, end):
        if self.a.window:
            off, end = off // self.a.window * self.a.window, -(-end // self.a.window) * self.a.window
        todo = [e for e in b.overlapping(off, end) if e[0] not in b.filled]
        if not todo:
            STATS.add(events_noop=1)
            return
        mine, wait = [], []
        with b.lock:
            for e in todo:
                if e[0] in b.filled:
                    continue
                f = b.inflight.get(e[0])
                if f is None:
                    f = b.inflight[e[0]] = Future()
                    mine.append(e)
                wait.append(f)
        if mine:
            try:
                ents = mine
                if self.a.fill == "reflink":
                    paths = self.src.files(ents)
                    for e in ents:
                        sfd = os.open(paths[e[2]], os.O_RDONLY)
                        try:
                            fcntl.ioctl(b.wfd, FICLONERANGE, CLONE.pack(sfd, 0, -(-e[1] // 4096) * 4096, e[0]))
                        finally:
                            os.close(sfd)
                    STATS.add(clone_chunks=len(ents), clone_bytes=sum(e[1] for e in ents))
                else:
                    data = self.src.get(ents)
                    for e in ents:
                        if b.dfd is not None:
                            d = data[e[2]]
                            buf = mmap.mmap(-1, -(-len(d) // 4096) * 4096)  # page-aligned, zero-padded
                            buf.write(d)
                            os.pwrite(b.dfd, buf, e[0])
                            buf.close()
                        else:
                            os.pwrite(b.wfd, data[e[2]], e[0])
                STATS.add(fill_chunks=len(ents), fill_bytes=sum(e[1] for e in ents), events_filled=1)
                with b.lock:
                    for e in mine:
                        b.filled.add(e[0])
                        b.inflight.pop(e[0]).set_result(True)
                    full = len(b.filled) == len(b.ents)
                if full and self.a.unmark_when_full and b.marked:
                    fan.unmark(self.gfd, b.path)
                    b.marked = False
                    STATS.add(unmarked_full=1)
            except Exception as e:
                with b.lock:
                    for x in mine:
                        f = b.inflight.pop(x[0], None)
                        if f is not None and not f.done():
                            f.set_exception(e)
                raise
        else:
            STATS.add(fill_waits=1)
        for f in wait:
            f.result(timeout=120)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sock", default="/run/s13fan.sock")
    ap.add_argument("--window", type=int, default=0)
    ap.add_argument("--mem-bytes", type=int, default=2 << 30)
    ap.add_argument("--misses", type=int, default=32)
    ap.add_argument("--workers", type=int, default=64)
    ap.add_argument("--chunk-cache")
    ap.add_argument("--fill", default="copy", choices=["copy", "reflink"])
    ap.add_argument("--unmark-when-full", action="store_true")
    ap.add_argument("--odirect", action="store_true")
    ap.add_argument("--fast-noop", action="store_true")
    a = ap.parse_args()
    Backing.ODIRECT = a.odirect
    resource.setrlimit(resource.RLIMIT_NOFILE, (1 << 20, 1 << 20))
    d = Daemon(a)
    if os.path.exists(a.sock):
        os.unlink(a.sock)

    class H(socketserver.StreamRequestHandler):
        def handle(self):
            for line in self.rfile:
                req = json.loads(line)
                try:
                    if req["op"] == "quit":
                        self.wfile.write(b'{"ok": true}\n')
                        os._exit(0)
                    resp = getattr(d, req["op"])(req)
                except Exception as e:  # noqa: BLE001
                    resp = {"ok": False, "error": repr(e)[-800:]}
                self.wfile.write((json.dumps(resp) + "\n").encode())

    class S(socketserver.ThreadingUnixStreamServer):
        request_queue_size = 512
        daemon_threads = True
    srv = S(a.sock, H)
    print("LISTENING", a.sock, flush=True)
    srv.serve_forever()


main()
