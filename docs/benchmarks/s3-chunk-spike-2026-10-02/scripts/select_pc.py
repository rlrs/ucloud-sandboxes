#!/usr/bin/env python3
"""S12 (e): choose ~60 distinct prepared images (TMax / Terminal-Lego / OpenSWE foundations) that share
few base layer stacks, and fetch them read-only from our registry.

Reads every candidate's manifest, groups images by their first `--depth` layer digests (the shared
foundation base), keeps the largest groups until `--want` images, and writes
/data/manifests/pc-manifests.json in fetch.py's format, then fetches the layers and configs.
"""
import argparse
import collections
import hashlib
import json
import os
import random
import urllib.request
from concurrent.futures import ThreadPoolExecutor

REG = "http://10.42.0.2:5000"
ACCEPT = "application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.v2+json"


def get(path):
    req = urllib.request.Request(REG + path, headers={"Accept": ACCEPT})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def split(ref):
    name, digest = ref.split("@")
    return name.split("/", 1)[1].rsplit(":", 1)[0], digest


def manifest(c):
    repo, digest = split(c["prepared_reference"])
    try:
        raw = get(f"/v2/{repo}/manifests/{digest}")
    except Exception as e:  # noqa: BLE001
        return None
    if "sha256:" + hashlib.sha256(raw).hexdigest() != digest:
        return None
    m = json.loads(raw)
    if "layers" not in m:
        return None
    return dict(c, repo=repo, digest=digest, manifest=m, env=None)


def fetch_blob(repo, d):
    path = f"/data/oci/blobs/sha256/{d.split(':')[1]}"
    if os.path.exists(path):
        return 0
    h = hashlib.sha256()
    with urllib.request.urlopen(f"{REG}/v2/{repo}/blobs/{d}", timeout=600) as r, open(path + ".part", "wb") as f:
        while b := r.read(4 << 20):
            h.update(b)
            f.write(b)
    assert "sha256:" + h.hexdigest() == d
    os.rename(path + ".part", path)
    return os.path.getsize(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidates", default="/root/s12/pc-candidates.json")
    ap.add_argument("--want", type=int, default=64)
    ap.add_argument("--groups", type=int, default=4)
    ap.add_argument("--depth", type=int, default=1)
    a = ap.parse_args()
    cands = json.load(open(a.candidates))
    with ThreadPoolExecutor(64) as ex:
        ms = [m for m in ex.map(manifest, cands) if m]
    by = collections.defaultdict(list)
    for m in ms:
        by[tuple(l["digest"] for l in m["manifest"]["layers"][:a.depth])].append(m)
    groups = sorted(by.values(), key=len, reverse=True)
    stats = [{"base": g[0]["manifest"]["layers"][0]["digest"], "n": len(g),
              "families": dict(collections.Counter(x["family"] for x in g))} for g in groups[:12]]
    print(json.dumps({"manifests": len(ms), "groups": len(groups), "top": stats}, indent=1), flush=True)
    rnd = random.Random(20261002)
    per = a.want // a.groups
    chosen = []
    for g in groups[:a.groups]:
        rnd.shuffle(g)
        chosen += g[:per]
    rnd.shuffle(chosen)
    for i, c in enumerate(chosen):
        c["idx"] = i
    os.makedirs("/data/manifests", exist_ok=True)
    json.dump(chosen, open("/data/manifests/pc-manifests.json", "w"), indent=1)
    blobs = {}
    for c in chosen:
        blobs.setdefault(c["manifest"]["config"]["digest"], c["repo"])
        for l in c["manifest"]["layers"]:
            blobs.setdefault(l["digest"], c["repo"])
    layers = {l["digest"]: l["size"] for c in chosen for l in c["manifest"]["layers"]}
    summed = sum(l["size"] for c in chosen for l in c["manifest"]["layers"])
    with ThreadPoolExecutor(16) as ex:
        n = sum(ex.map(lambda kv: fetch_blob(kv[1], kv[0]), blobs.items()))
    out = {"chosen": len(chosen), "unique_layers": len(layers), "unique_layer_bytes": sum(layers.values()),
           "summed_layer_bytes": summed, "fetched_bytes": n, "groups": stats[:a.groups],
           "images": [{"idx": c["idx"], "family": c["family"], "image": c["image"], "layers": len(c["manifest"]["layers"]),
                       "base": c["manifest"]["layers"][0]["digest"]} for c in chosen]}
    json.dump(out, open("/data/results/pc-selection.json", "w"), indent=1)
    print(json.dumps({k: v for k, v in out.items() if k != "images"}), flush=True)


main()
