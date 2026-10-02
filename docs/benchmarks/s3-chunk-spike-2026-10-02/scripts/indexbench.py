#!/usr/bin/env python3
"""S12 (d): synthetic chunk index at full-corpus scale (design §1.3 `chunks` table, §5).

  indexbench.py build  --rows 500000000 --db /data/idx/chunks.sqlite
  indexbench.py probe  --rows 500000000 --db /data/idx/chunks.sqlite
  indexbench.py fill   --rows 20000000            (random-order vs sorted fill, for the B-tree fill factor)

Ids are uniform 32-byte values: id(i) = (i * 2^64 // N) as 8 bytes, then sha256(i)[:24], so the build
can insert in key order and the probe can regenerate any existing id. A key-order build packs the
B-tree full; the `fill` run measures how much larger random-order insertion (what production does)
makes the same table, and the report scales the 500M file by that factor.
probe: 30k-id batch lookups (half present, half absent) cold (after drop_caches) and warm, with
three query shapes; then the insert rate of new random ids into the 500M-row table in per-pack
transactions of 7.5k rows (WAL, synchronous=NORMAL), as builders would commit.
"""
import argparse
import hashlib
import json
import os
import random
import sqlite3
import struct
import time

SCHEMA = """CREATE TABLE IF NOT EXISTS chunks (id BLOB PRIMARY KEY, pack INTEGER, off INTEGER, clen INTEGER,
            ulen INTEGER, flags INTEGER, condemned INTEGER DEFAULT 0) WITHOUT ROWID"""


def cid(i, n):
    return struct.pack(">Q", i * ((1 << 64) // n)) + hashlib.sha256(struct.pack("<Q", i)).digest()[:24]


def row(i, n):
    return (cid(i, n), i // 3000, (i % 3000) * 19000, 19000, 65536, 1)


def open_db(path, bulk=False):
    db = sqlite3.connect(path, isolation_level=None)
    db.execute("PRAGMA page_size=4096")
    if bulk:
        db.execute("PRAGMA journal_mode=OFF")
        db.execute("PRAGMA synchronous=OFF")
    else:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA cache_size=-4000000")  # 4 GB, the design's index RAM
    db.execute(SCHEMA)
    return db


def build(a):
    os.makedirs(os.path.dirname(a.db), exist_ok=True)
    db = open_db(a.db, bulk=True)
    t0 = time.time()
    B = 1_000_000
    for s in range(0, a.rows, B):
        db.execute("BEGIN")
        db.executemany("INSERT INTO chunks (id, pack, off, clen, ulen, flags) VALUES (?,?,?,?,?,?)",
                       (row(i, a.rows) for i in range(s, min(a.rows, s + B))))
        db.execute("COMMIT")
        if s % (50 * B) == 0:
            print(json.dumps({"rows": s + B, "s": round(time.time() - t0, 1),
                              "bytes": os.path.getsize(a.db)}), flush=True)
    db.close()
    res = {"rows": a.rows, "build_s": time.time() - t0, "file_bytes": os.path.getsize(a.db),
           "rows_per_s": a.rows / (time.time() - t0)}
    print(json.dumps(res), flush=True)
    json.dump(res, open(f"/data/results/index-build-{a.rows}.json", "w"), indent=1)


def fill(a):
    out = {}
    for order in ("sorted", "random"):
        p = f"/data/idx/fill-{order}.sqlite"
        for x in (p, p + "-wal", p + "-shm"):
            if os.path.exists(x):
                os.unlink(x)
        db = open_db(p, bulk=True)
        ids = list(range(a.rows))
        if order == "random":
            random.Random(1).shuffle(ids)
        t0 = time.time()
        B = 100_000
        for s in range(0, a.rows, B):
            db.execute("BEGIN")
            db.executemany("INSERT INTO chunks (id, pack, off, clen, ulen, flags) VALUES (?,?,?,?,?,?)",
                           (row(i, a.rows) for i in ids[s:s + B]))
            db.execute("COMMIT")
        db.close()
        out[order] = {"bytes": os.path.getsize(p), "s": time.time() - t0, "bytes_per_row": os.path.getsize(p) / a.rows}
        os.unlink(p)
    out["random_over_sorted"] = out["random"]["bytes"] / out["sorted"]["bytes"]
    print(json.dumps(out), flush=True)
    json.dump(out, open(f"/data/results/index-fill-{a.rows}.json", "w"), indent=1)


def drop_caches():
    os.sync()
    open("/proc/sys/vm/drop_caches", "w").write("3\n")


def batch_ids(n_rows, k, seed):
    rnd = random.Random(seed)
    present = [cid(rnd.randrange(n_rows), n_rows) for _ in range(k // 2)]
    absent = [os.urandom(32) for _ in range(k - k // 2)]
    ids = present + absent
    rnd.shuffle(ids)
    return ids


def lookup(db, ids, shape):
    if shape == "in999":
        found = 0
        for s in range(0, len(ids), 999):
            part = ids[s:s + 999]
            found += len(db.execute(f"SELECT id, pack, off, clen FROM chunks WHERE id IN ({','.join('?' * len(part))})",
                                    part).fetchall())
        return found
    if shape == "point":
        q = "SELECT pack, off, clen FROM chunks WHERE id = ?"
        return sum(1 for i in ids if db.execute(q, (i,)).fetchone())
    if shape == "temp_join":
        db.execute("CREATE TEMP TABLE IF NOT EXISTS q (id BLOB PRIMARY KEY) WITHOUT ROWID")
        db.execute("DELETE FROM q")
        db.executemany("INSERT OR IGNORE INTO q VALUES (?)", ((i,) for i in ids))
        return len(db.execute("SELECT c.id, c.pack, c.off FROM q JOIN chunks c ON c.id = q.id").fetchall())
    raise ValueError(shape)


def probe(a):
    res = {"rows": a.rows, "file_bytes": os.path.getsize(a.db), "lookups": [], "inserts": {}}
    for state in ("cold", "warm", "warm"):
        for shape in ("in999", "point", "temp_join"):
            if state == "cold":
                drop_caches()
            db = open_db(a.db)
            ids = batch_ids(a.rows, 30000, hash((state, shape, len(res["lookups"]))) & 0xffff)
            t0 = time.time()
            found = lookup(db, ids, shape)
            dt = time.time() - t0
            db.close()
            r = {"state": state, "shape": shape, "ids": len(ids), "found": found, "s": dt}
            res["lookups"].append(r)
            print(json.dumps(r), flush=True)
    # Insert rate: new random ids, 7.5k per transaction (one pack commit), WAL + synchronous=NORMAL.
    db = open_db(a.db)
    total, txn = 1_000_000, 7500
    t0 = time.time()
    n = 0
    while n < total:
        db.execute("BEGIN IMMEDIATE")
        db.executemany("INSERT OR IGNORE INTO chunks (id, pack, off, clen, ulen, flags) VALUES (?,?,?,?,?,?)",
                       ((os.urandom(32), 10 ** 7, k * 19000, 19000, 65536, 1) for k in range(txn)))
        db.execute("COMMIT")
        n += txn
    dt = time.time() - t0
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    db.close()
    res["inserts"] = {"rows": n, "txn_rows": txn, "s": dt, "rows_per_s": n / dt, "file_bytes_after": os.path.getsize(a.db)}
    print(json.dumps(res["inserts"]), flush=True)
    json.dump(res, open(f"/data/results/index-probe-{a.rows}.json", "w"), indent=1)


def probe2(a):
    """What it takes to meet 1 s: sorted ids, parallel connections (queue depth), and a fully cached file."""
    from concurrent.futures import ThreadPoolExecutor
    res = {"rows": a.rows, "runs": []}

    def par_lookup(ids, threads):
        ids = sorted(ids)
        parts = [ids[k::threads] for k in range(threads)]

        def one(part):
            db = open_db(a.db)
            q = "SELECT pack, off, clen FROM chunks WHERE id = ?"
            n = sum(1 for i in part if db.execute(q, (i,)).fetchone())
            db.close()
            return n
        with ThreadPoolExecutor(threads) as ex:
            return sum(ex.map(one, parts))
    seed = 100
    for cache in ("cold", "full"):
        if cache == "full":
            t0 = time.time()
            with open(a.db, "rb") as f:
                while f.read(64 << 20):
                    pass
            res["full_cache_load_s"] = time.time() - t0
        for threads in (1, 4, 16, 64):
            if cache == "cold":
                drop_caches()
            seed += 1
            ids = batch_ids(a.rows, 30000, seed)
            t0 = time.time()
            found = par_lookup(ids, threads)
            r = {"cache": cache, "threads": threads, "sorted": True, "found": found, "s": time.time() - t0}
            res["runs"].append(r)
            print(json.dumps(r), flush=True)
    json.dump(res, open(f"/data/results/index-probe2-{a.rows}.json", "w"), indent=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["build", "probe", "fill", "probe2"])
    ap.add_argument("--rows", type=int, default=500_000_000)
    ap.add_argument("--db", default="/data/idx/chunks.sqlite")
    a = ap.parse_args()
    os.makedirs("/data/idx", exist_ok=True)
    os.makedirs("/data/results", exist_ok=True)
    {"build": build, "probe": probe, "fill": fill, "probe2": probe2}[a.what](a)


main()
