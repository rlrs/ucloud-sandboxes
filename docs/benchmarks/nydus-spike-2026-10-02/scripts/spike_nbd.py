#!/usr/bin/env python3
"""Spike NBD backend: serve one read-only block device from either

  nydus <bootstrap> <blobdir>   RAFS v6 unified (flat) address space: bootstrap blocks, then each
                                blob's *uncompressed* data at its mapped_blkaddr. Chunks are read
                                from the zstd-compressed blob, decompressed and sha256-verified
                                against the bootstrap's chunk table (our verified-chunk-cache model).
  raw <image>                   today's EROFS component: 256 KiB windows, each sha256-verified.

Usage: spike_nbd.py /dev/nbdN MODE ARGS...  (prints READY <bytes> when the device is live; SIGTERM stops)
Kernel plumbing mirrors ucloud_sandboxes/environment_nbd.py.
"""
import bisect, json, errno, fcntl, hashlib, os, signal, socket, struct, sys, threading, time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from compression import zstd
sys.path.insert(0, "/root")
import rafs

REQUEST = struct.Struct("!II8sQI"); REPLY = struct.Struct("!II8s")
SET_SOCK, SET_BLKSIZE, SET_SIZE, DO_IT, CLEAR_SOCK = (0xab00 + n for n in range(5))
SET_SIZE_BLOCKS, DISCONNECT, SET_TIMEOUT, SET_FLAGS = 0xab07, 0xab08, 0xab09, 0xab0a
WINDOW = 256 * 1024
STATS = {"reads": 0, "read_bytes": 0, "chunk_fetches": 0, "fetched_compressed": 0, "fetched_uncompressed": 0}


class Cache:
    def __init__(self, cap=4 << 30):
        self.d, self.size, self.cap, self.lock = OrderedDict(), 0, cap, threading.Lock()
        self.inflight = {}

    def get(self, key, load):
        with self.lock:
            if key in self.d:
                self.d.move_to_end(key); return self.d[key]
            ev = self.inflight.get(key)
            if ev is None:
                ev = self.inflight[key] = threading.Event(); owner = True
            else:
                owner = False
        if not owner:
            ev.wait()
            with self.lock:
                return self.d[key]
        data = load()
        with self.lock:
            self.d[key] = data; self.size += len(data)
            while self.size > self.cap:
                _, v = self.d.popitem(last=False); self.size -= len(v)
            del self.inflight[key]
        ev.set()
        return data


class NydusSource:
    def __init__(self, boot, blobdir):
        info = rafs.read(boot)
        self.meta = open(boot, "rb").read()
        self.regions = []  # (start, end, blob_index)
        for i, d in enumerate(info["devices"]):
            start = d["mapped_blkaddr"] * 4096
            self.regions.append((start, start + d["blocks"] * 4096, i))
        self.regions.sort()
        self.size = max([len(self.meta)] + [r[1] for r in self.regions])
        self.size = (self.size + 4095) // 4096 * 4096
        self.files = [open(os.path.join(blobdir, d["blob_id"]), "rb") for d in info["devices"]]
        self.chunks = {}
        for (dg, bi, fl, cs, us, co, uo) in info["chunks"]:
            self.chunks.setdefault(bi, []).append((uo, us, co, cs, fl, dg))
        for v in self.chunks.values():
            v.sort()
        self.keys = {b: [c[0] for c in v] for b, v in self.chunks.items()}
        self.cache = Cache()

    def _chunk(self, bi, c):
        uo, us, co, cs, fl, dg = c
        def load():
            raw = os.pread(self.files[bi].fileno(), cs, co)
            data = zstd.decompress(raw) if fl & 1 else raw
            if len(data) != us or hashlib.sha256(data).hexdigest() != dg:
                raise OSError(errno.EIO, "chunk digest mismatch")
            STATS["chunk_fetches"] += 1; STATS["fetched_compressed"] += cs; STATS["fetched_uncompressed"] += us
            return data
        return self.cache.get(dg, load)

    def read(self, off, n):
        out = bytearray(n)
        end = off + n
        if off < len(self.meta):
            m = self.meta[off:min(end, len(self.meta))]; out[:len(m)] = m
        for (rs, re_, bi) in self.regions:
            if re_ <= off or rs >= end:
                continue
            lo, hi = max(off, rs) - rs, min(end, re_) - rs
            ks, cl = self.keys.get(bi, []), self.chunks.get(bi, [])
            j = max(bisect.bisect_right(ks, lo) - 1, 0)
            while j < len(cl) and cl[j][0] < hi:
                uo, us = cl[j][0], cl[j][1]
                if uo + us > lo:
                    data = self._chunk(bi, cl[j])
                    a, b = max(lo, uo), min(hi, uo + us)
                    out[a + rs - off:b + rs - off] = data[a - uo:b - uo]
                j += 1
        return bytes(out)


class PartSource:
    """One piece of the flat space as its own device: the bootstrap (part -1) or one blob's
    uncompressed data (multi-device mounts with -o device=...)."""
    def __init__(self, boot, blobdir, part):
        self.src = NydusSource(boot, blobdir); part = int(part)
        if part < 0:
            self.base, self.size = 0, (len(self.src.meta) + 4095) // 4096 * 4096
        else:
            rs, re_, _ = next(r for r in self.src.regions if r[2] == part)
            self.base, self.size = rs, re_ - rs

    def read(self, off, n):
        if self.base == 0:
            m = self.src.meta[off:off + n]; return m + b"\0" * (n - len(m))
        return self.src.read(self.base + off, n)


class RawSource:
    def __init__(self, image):
        self.f = open(image, "rb"); self.size = os.fstat(self.f.fileno()).st_size
        # Today's signed index: sha256 per 256 KiB window (computed here, as the builder would sign it).
        # Loaded like the signed index a worker downloads; computed once per component and kept
        # beside it, then evicted from the page cache so the first attach starts cold.
        idx = image + ".index.json"
        if not os.path.exists(idx):
            ds = []
            while b := self.f.read(WINDOW):
                ds.append(hashlib.sha256(b).hexdigest())
            json.dump(ds, open(idx, "w"))
            os.posix_fadvise(self.f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        self.digests = json.load(open(idx))
        self.cache = Cache()

    def _window(self, i):
        def load():
            data = os.pread(self.f.fileno(), WINDOW, i * WINDOW)
            if hashlib.sha256(data).hexdigest() != self.digests[i]:
                raise OSError(errno.EIO, "window digest mismatch")
            STATS["chunk_fetches"] += 1; STATS["fetched_uncompressed"] += len(data)
            return data
        return self.cache.get(self.digests[i] + str(i), load)

    def read(self, off, n):
        out = bytearray()
        i = off // WINDOW
        while off + len(out) < off + n and i * WINDOW < self.size:
            data = self._window(i)
            a = max(off - i * WINDOW, 0); b = min(off + n - i * WINDOW, len(data))
            out += data[a:b]; i += 1
        return bytes(out.ljust(n, b"\0"))


def main():
    dev, mode, *rest = sys.argv[1:]
    t0 = time.time()
    src = {"nydus": NydusSource, "raw": RawSource, "part": PartSource}[mode](*rest)
    fd = os.open(dev, os.O_RDWR)
    ks, us = socket.socketpair()
    fcntl.ioctl(fd, SET_SOCK, ks.fileno())
    fcntl.ioctl(fd, SET_BLKSIZE, 4096)
    fcntl.ioctl(fd, SET_SIZE_BLOCKS, src.size // 4096)
    fcntl.ioctl(fd, SET_FLAGS, 1 | 2)
    fcntl.ioctl(fd, SET_TIMEOUT, 30)
    lock, pool, stop = threading.Lock(), ThreadPoolExecutor(16), threading.Event()

    def reply(h, err, data=b""):
        with lock:
            us.sendall(REPLY.pack(0x67446698, err, h) + data)

    def do_read(h, off, n):
        try:
            data = src.read(off, n); STATS["reads"] += 1; STATS["read_bytes"] += n; reply(h, 0, data)
        except Exception as e:
            sys.stderr.write(f"read error {off} {n}: {e}\n"); reply(h, errno.EIO)

    def recv(k):
        b = b""
        while len(b) < k:
            x = us.recv(k - len(b))
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
    kt = threading.Thread(target=lambda: fcntl.ioctl(fd, DO_IT), daemon=True); kt.start()
    name = os.path.basename(dev)
    while True:
        try:
            if int(open(f"/sys/block/{name}/size").read()) * 512 == src.size and open(f"/sys/block/{name}/pid").read().strip():
                break
        except OSError:
            pass
        time.sleep(0.002)
    print(f"READY {src.size} {time.time() - t0:.4f}", flush=True)

    def term(*_):
        print("STATS", STATS, flush=True)
        try:
            fcntl.ioctl(fd, DISCONNECT)
        except OSError:
            pass
        kt.join(5)
        try:
            fcntl.ioctl(fd, CLEAR_SOCK)
        except OSError:
            pass
        os._exit(0)
    signal.signal(signal.SIGTERM, term); signal.signal(signal.SIGINT, term)
    while True:
        signal.pause()


main()
