#!/usr/bin/env python3
"""Walk a `docker export` tar stream (stdin) and record the files that differ from a foundation.

Chunking follows RAFS v6 as built by nydus-image: each regular file is cut into fixed 256 KiB
chunks from offset 0, and a chunk is identified by the sha256 of its uncompressed bytes.
For each chunk we also record its zstd size (level 3, per chunk), as stored in a chunk store.

--full:  record every file (used for foundations); output also lists every chunk digest.
--base:  foundation manifest (from --full); a file whose type/size/mtime/mode/link target match the
         foundation entry is unchanged and is not hashed.
Output: gzip jsonl, one record per changed/added entry: [path, type, size, [[digest_hex16, usize, csize]...], link]
plus a final {"deleted": [...]} record listing foundation paths missing from this tree.
"""
import argparse, gzip, hashlib, json, sys, tarfile
try:
    import zstandard
    def zsize(b, level, _c={}):
        c = _c.get(level) or _c.setdefault(level, zstandard.ZstdCompressor(level=level))
        return len(c.compress(b))
except ImportError:              # Python 3.14 stdlib
    from compression import zstd
    def zsize(b, level):
        return len(zstd.compress(b, level=level))

CHUNK = 256 * 1024
ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True)
ap.add_argument("--base")
ap.add_argument("--full", action="store_true")
ap.add_argument("--level", type=int, default=3)
a = ap.parse_args()
base = {}
if a.base:
    with gzip.open(a.base, "rt") as f:
        for line in f:
            r = json.loads(line)
            if isinstance(r, list):
                base[r[0]] = r[5]  # meta key
seen_cs = {}
out = gzip.open(a.out, "wt", compresslevel=3)
present = set()
n_files = n_hashed = bytes_hashed = 0
tf = tarfile.open(fileobj=sys.stdin.buffer, mode="r|", bufsize=1 << 20)
for m in tf:
    p = "/" + m.name.lstrip("./").rstrip("/") if m.name not in (".", "./") else "/"
    if m.isreg():
        t = "f"
    elif m.isdir():
        t = "d"
    elif m.issym():
        t = "l"
    elif m.islnk():
        t = "h"
    else:
        t = "o"
    link = m.linkname if t in ("l", "h") else ""
    meta = f"{t}:{m.size if t == 'f' else 0}:{int(m.mtime)}:{m.mode:o}:{link}"
    present.add(p)
    n_files += 1
    if not a.full and base.get(p) == meta:
        continue
    chunks = []
    if t == "f":
        fobj = tf.extractfile(m)
        n_hashed += 1
        while True:
            b = fobj.read(CHUNK)
            if not b:
                break
            d = hashlib.sha256(b).hexdigest()[:32]
            cs = seen_cs.get(d)
            if cs is None:
                cs = zsize(b, a.level)
                seen_cs[d] = cs
            chunks.append([d, len(b), cs])
            bytes_hashed += len(b)
    out.write(json.dumps([p, t, m.size if t == "f" else 0, chunks, link, meta]) + "\n")
deleted = [p for p in base if p not in present] if base else []
out.write(json.dumps({"deleted": deleted, "entries": n_files, "hashed_files": n_hashed, "hashed_bytes": bytes_hashed}) + "\n")
out.close()
print(json.dumps({"out": a.out, "entries": n_files, "hashed_files": n_hashed, "hashed_bytes": bytes_hashed, "deleted": len(deleted)}))
