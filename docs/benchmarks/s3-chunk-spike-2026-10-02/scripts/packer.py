#!/usr/bin/env python3
"""S12 (a): prototype packer over S10-style per-layer conversions (nydus-image v2.4.5, 256 KiB, no dict).

For each conversion (one OCI layer, in the S10 shuffled image order): every chunk id not yet in the
index is read from the conversion's local Nydus blob, decompressed and verified (len == ulen and
sha256 == id), then appended in blob order to packs of at most 64 MiB with a sorted footer. Packs are
written locally (/data/s12/packs, the loopback baseline) and, with --upload, PUT to spike-s12/packs/.
Then every image's (and layer's) chunk map + locator is derived from its bootstrap.

Usage: packer.py --rundir /data/work/nodict-256k --sample /data/manifests/sample-manifests.json
                 [--upload] [--tag main] [--maps-only]
"""
import argparse
import collections
import hashlib
import json
import os
import pickle
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from compression import zstd

sys.path.insert(0, "/root/s12")
import packfmt  # noqa: E402
import rafs  # noqa: E402

S12 = os.environ.get("S12_DIR", "/data/s12")
PACKS = f"{S12}/packs"
MAPS = f"{S12}/maps"
STATE = f"{S12}/index.pkl"
CFG = "/root/s12/s3.json"  # endpoint/bucket/region (no secrets)
ENV = "/root/s12/.s3env"   # root-only credentials file


def s3client():
    import s3lib
    c = json.load(open(CFG))
    return s3lib.S3(c["endpoint"], c["bucket"], c["region"], ENV)


def load_state():
    if os.path.exists(STATE):
        return pickle.load(open(STATE, "rb"))
    return {"index": {}, "packs": [], "layers_done": {}, "conversions": []}


def encode(raw, data, nydus_flags):
    """Keep nydus-image's zstd bytes; store raw when zstd saves < 3% or the chunk is < 4 KiB."""
    if not nydus_flags & 1:
        return data, 0
    if len(data) < 4096 or len(raw) > 0.97 * len(data):
        return data, 0
    return raw, packfmt.F_ZSTD


def pack_layer(st, rundir, digest, pos, upload_pool, s3, stats):
    boot = f"{rundir}/layers/{digest.split(':')[1]}.boot"
    info = rafs.read(boot)
    conv = {"layer": digest, "pos": pos, "chunks": len(info["chunks"]), "refs_bytes_c": 0,
            "dup_known": 0, "dup_known_bytes_c": 0, "dup_within": 0, "new": 0, "new_bytes_c": 0,
            "new_bytes_u": 0, "packs": 0, "stored_raw": 0, "zero_chunks": 0}
    if not info["devices"]:
        st["layers_done"][digest] = []
        st["conversions"].append(conv)
        return conv
    blob_ids = [d["blob_id"] for d in info["devices"]]
    fds = [os.open(f"{rundir}/blobs/{b}", os.O_RDONLY) for b in blob_ids]
    t0 = time.time()
    seen = set()
    w = packfmt.PackWriter()
    produced = []

    def flush():
        nonlocal w
        if not len(w):
            return
        sha, data = w.finish()
        pno = len(st["packs"])
        for cid, off, clen, ulen, fl in w.entries:
            st["index"][cid] = (pno, off, clen, ulen, fl)
        st["packs"].append({"sha": sha, "bytes": len(data), "chunks": len(w.entries), "layer": digest, "pos": pos})
        path = f"{PACKS}/{sha}.pack"
        with open(path + ".tmp", "wb") as f:
            f.write(data)
        os.rename(path + ".tmp", path)
        if s3 is not None:
            key = f"spike-s12/packs/{sha[:2]}/{sha}.pack"
            upload_pool.append(UPLOADER.submit(s3.put, key, data))
        produced.append(pno)
        conv["packs"] += 1
        w = packfmt.PackWriter()

    for (dg, bi, fl, cs, us, co, uo) in sorted(info["chunks"], key=lambda c: (c[1], c[5])):
        cid = bytes.fromhex(dg)
        conv["refs_bytes_c"] += cs
        if cid in st["index"]:
            conv["dup_known"] += 1; conv["dup_known_bytes_c"] += cs
            continue
        if cid in seen:
            conv["dup_within"] += 1
            continue
        seen.add(cid)
        raw = os.pread(fds[bi], cs, co)
        data = zstd.decompress(raw) if fl & 1 else raw
        if len(data) != us or hashlib.sha256(data).digest() != cid:
            raise RuntimeError(f"chunk {dg} of {digest} failed verification")
        if data.count(0) == len(data):
            conv["zero_chunks"] += 1
        payload, pflags = encode(raw, data, fl)
        conv["stored_raw"] += pflags == 0
        if not w.fits(len(payload)):
            flush()
        w.add(cid, payload, us, pflags)
        conv["new"] += 1; conv["new_bytes_c"] += len(payload); conv["new_bytes_u"] += us
    flush()
    for fd in fds:
        os.close(fd)
    conv["pack_s"] = time.time() - t0
    st["layers_done"][digest] = produced
    st["conversions"].append(conv)
    return conv


def device_entries(boot_path, index, packs_used=None):
    info = rafs.read(boot_path)
    base = [d["mapped_blkaddr"] * 4096 for d in info["devices"]]
    ents = []
    for (dg, bi, fl, cs, us, co, uo) in info["chunks"]:
        cid = bytes.fromhex(dg)
        pno, off, clen, ulen, pfl = index[cid]
        assert ulen == us
        ents.append((base[bi] + uo, us, cid, pno, off, clen, pfl))
    ents.sort()
    for a, b in zip(ents, ents[1:]):
        assert a[0] + a[1] <= b[0], "overlapping chunk map entries"
    size = max([info["size"]] + [b + d["blocks"] * 4096 for b, d in zip(base, info["devices"])])
    return info, ents, (size + 4095) // 4096 * 4096


def write_maps(st, rundir, sample, order, s3, which, prefix="img"):
    os.makedirs(MAPS, exist_ok=True)
    out = []
    jobs = []
    for i in order:
        jobs.append(("img", i, f"{rundir}/images/{i:03d}.boot"))
    if which == "all":
        for d in st["layers_done"]:
            p = f"{rundir}/layers/{d.split(':')[1]}.boot"
            if os.path.exists(p):
                jobs.append(("layer", d.split(":")[1], p))
    for kind, name, boot in jobs:
        if not os.path.exists(boot):
            continue
        label = f"{prefix}-{name:03d}" if kind == "img" else f"layer-{name}"
        info, ents, dev_size = device_entries(boot, st["index"])
        braw = open(boot, "rb").read()
        bsha = hashlib.sha256(braw).hexdigest()
        packs_used = sorted({e[3] for e in ents})
        header = {"kind": kind, "name": name, "bootstrap": boot, "bootstrap_sha256": bsha,
                  "bootstrap_bytes": len(braw), "device_size": dev_size, "entries": len(ents)}
        mpath = f"{MAPS}/{label}.map"
        nbytes = packfmt.write_map(mpath, header, ents)
        rec = {"kind": kind, "name": label, "entries": len(ents), "packs": len(packs_used),
               "bytes_c": sum(e[5] for e in ents), "bytes_u": sum(e[1] for e in ents),
               "bootstrap_bytes": len(braw), "chunk_map_bytes": 44 * len(ents) + 64,
               "locator_bytes_raw": 13 * len(ents), "map_file": mpath}
        bz = zstd.compress(braw, 3)
        mz = zstd.compress(open(mpath, "rb").read(), 3)
        rec.update(bootstrap_zst_bytes=len(bz), map_zst_bytes=len(mz))
        with open(f"{MAPS}/{label}.boot.zst", "wb") as f:
            f.write(bz)
        with open(f"{MAPS}/{label}.map.zst", "wb") as f:
            f.write(mz)
        if kind == "img":
            s = sample[name]
            rec.update(family=s["family"], image=s["image"])
            # Per-pack bytes, to see how concentrated an image is across packs.
            per = collections.Counter()
            for e in ents:
                per[e[3]] += e[5]
            top = sorted(per.values(), reverse=True)
            rec["packs_for_90pct_bytes"] = next((k + 1 for k in range(len(top)) if sum(top[:k + 1]) >= 0.9 * sum(top)), 0)
            if s3 is not None:
                UPLOADER.submit(s3.put, f"spike-s12/meta/{label}.boot.zst", bz).result()
                UPLOADER.submit(s3.put, f"spike-s12/meta/{label}.map.zst", mz).result()
        out.append(rec)
    return out


def q(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else None


UPLOADER = ThreadPoolExecutor(16)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rundir", required=True)
    ap.add_argument("--sample", required=True)
    ap.add_argument("--upload", action="store_true")
    ap.add_argument("--tag", default="main")
    ap.add_argument("--maps", default="all", choices=["all", "images"])
    ap.add_argument("--maps-only", action="store_true")
    ap.add_argument("--prefix", default="img")
    args = ap.parse_args()
    os.makedirs(PACKS, exist_ok=True)
    sample = json.load(open(args.sample))
    recs = [json.loads(l) for l in open(f"{args.rundir}/images.jsonl")]
    order = [r["idx"] for r in sorted(recs, key=lambda r: r["pos"])]
    st = load_state()
    s3 = s3client() if args.upload else None
    t0 = time.time()
    pending = []
    per_image = []
    if not args.maps_only:
        for pos, i in enumerate(order):
            s = sample[i]
            img = {"pos": pos, "idx": i, "family": s["family"], "layers": 0, "layers_cached": 0, "new_bytes_c": 0,
                   "new_chunks": 0, "packs_written": 0}
            for l in s["manifest"]["layers"]:
                img["layers"] += 1
                if l["digest"] in st["layers_done"]:
                    img["layers_cached"] += 1
                    continue
                c = pack_layer(st, args.rundir, l["digest"], pos, pending, s3, None)
                img["new_bytes_c"] += c["new_bytes_c"]; img["new_chunks"] += c["new"]; img["packs_written"] += c["packs"]
            per_image.append(img)
            print(pos, s["family"], img, flush=True)
        for f in pending:
            f.result()
        pickle.dump(st, open(STATE + ".tmp", "wb"))
        os.rename(STATE + ".tmp", STATE)
    t_pack = time.time() - t0
    json.dump(st["packs"], open(f"{S12}/packs.json", "w"))
    maps = write_maps(st, args.rundir, sample, order, s3, args.maps, args.prefix)
    conv = st["conversions"]
    refs = sum(c["chunks"] for c in conv)
    new = sum(c["new"] for c in conv)
    stored = sum(p["bytes"] for p in st["packs"])
    imgs = [m for m in maps if m["kind"] == "img"]
    summary = {
        "tag": args.tag, "pack_wall_s": t_pack, "conversions": len(conv),
        "packs": len(st["packs"]), "pack_bytes": stored,
        "pack_payload_bytes": sum(c["new_bytes_c"] for c in conv),
        "pack_size_median": q([p["bytes"] for p in st["packs"]], 0.5),
        "pack_size_p90": q([p["bytes"] for p in st["packs"]], 0.9),
        "packs_at_cap": sum(p["bytes"] > (63 << 20) for p in st["packs"]),
        "chunk_refs_in_conversions": refs, "unique_chunks": new,
        "dup_ratio_by_count": 1 - new / refs if refs else None,
        "refs_bytes_c": sum(c["refs_bytes_c"] for c in conv),
        "dup_ratio_by_bytes": 1 - sum(c["new_bytes_c"] for c in conv) / max(1, sum(c["refs_bytes_c"] for c in conv)),
        "dedupe_factor_bytes": sum(c["refs_bytes_c"] for c in conv) / max(1, sum(c["new_bytes_c"] for c in conv)),
        "stored_raw_chunks": sum(c["stored_raw"] for c in conv), "zero_chunks": sum(c["zero_chunks"] for c in conv),
        "bootstrap_bytes": sum(m["bootstrap_bytes"] for m in imgs),
        "bootstrap_zst_bytes": sum(m.get("bootstrap_zst_bytes", 0) for m in imgs),
        "map_zst_bytes": sum(m.get("map_zst_bytes", 0) for m in imgs),
        "fanout_packs_per_image": {"median": q([m["packs"] for m in imgs], 0.5), "p90": q([m["packs"] for m in imgs], 0.9),
                                   "max": max(m["packs"] for m in imgs), "mean": statistics.mean(m["packs"] for m in imgs)},
        "packs_for_90pct_bytes": {"median": q([m["packs_for_90pct_bytes"] for m in imgs], 0.5),
                                  "p90": q([m["packs_for_90pct_bytes"] for m in imgs], 0.9)},
        "fanout_by_family": {f: {"n": len(v), "median": q([m["packs"] for m in v], 0.5), "max": max(m["packs"] for m in v)}
                             for f in sorted({m["family"] for m in imgs})
                             for v in [[m for m in imgs if m["family"] == f]]},
    }
    print(json.dumps(summary, indent=1), flush=True)
    os.makedirs("/data/results", exist_ok=True)
    json.dump({"summary": summary, "per_image": per_image, "maps": maps, "packs": st["packs"],
               "conversions": conv}, open(f"/data/results/packer-{args.tag}.json", "w"), indent=1)


main()
