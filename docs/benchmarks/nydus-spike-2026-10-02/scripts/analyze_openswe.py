#!/usr/bin/env python3
"""Marginal storage of locally built OpenSWE task images on top of the 181-image sample corpus."""
import gzip, json, os, glob, sys
run, base = sys.argv[1], sys.argv[2]           # openswe nodict run dir, sample nodict run dir
sample = json.load(open("/data/manifests/sample-manifests.json"))
tasks = json.load(open("/data/manifests/openswe-manifests.json"))
seen_oci = {l["digest"] for s in sample for l in s["manifest"]["layers"]}
seen_chunks = set()
for f in glob.glob(f"{base}/images/*.boot.chunks.json"):
    seen_chunks.update(c[0] for c in json.load(open(f))["chunks"])
base_blobs = set(os.listdir(f"{base}/blobs"))
recs = {json.loads(l)["idx"]: json.loads(l) for l in open(f"{run}/images.jsonl")}
out = []
for i, t in enumerate(tasks):
    r = recs[i]
    new_layers = [l for l in t["manifest"]["layers"] if l["digest"] not in seen_oci]
    unc = 0
    for l in new_layers:
        with gzip.open(f"/data/oci/blobs/sha256/{l['digest'].split(':')[1]}") as g:
            while b := g.read(8 << 20):
                unc += len(b)
    seen_oci.update(l["digest"] for l in t["manifest"]["layers"])
    blobs = []
    for l in t["manifest"]["layers"]:
        j = f"{run}/layers/{l['digest'].split(':')[1]}.boot.json"
        b = json.load(open(j))["blobs"]
        if b and b[0] not in base_blobs:
            blobs.append(b[0]); base_blobs.add(b[0])
    nodict_new = sum(os.path.getsize(f"{run}/blobs/{b}") for b in blobs)
    ch = {}
    for c in json.load(open(f"{run}/images/{i:03d}.boot.chunks.json"))["chunks"]:
        ch.setdefault(c[0], (c[3], c[4]))
    new = {d: v for d, v in ch.items() if d not in seen_chunks}
    seen_chunks.update(ch)
    rec = {"task": t["image"], "group": t["group"], "layers": len(t["manifest"]["layers"]), "new_oci_layers": len(new_layers),
           "oci_compressed_bytes": sum(l["size"] for l in t["manifest"]["layers"]),
           "oci_new_bytes": sum(l["size"] for l in new_layers), "new_layers_uncompressed_bytes": unc,
           "bootstrap_bytes": r["bootstrap_bytes"], "nodict_new_blob_bytes": nodict_new,
           "cas_new_compressed": sum(v[0] for v in new.values()), "cas_new_uncompressed": sum(v[1] for v in new.values()),
           "image_unique_uncompressed": sum(v[1] for v in ch.values()),
           "convert_wall": r["create_wall"] + r["merge_wall"], "convert_cpu": r["create_cpu"] + r["merge_cpu"],
           "build_seconds": t["build_seconds"]}
    out.append(rec)
    print(json.dumps(rec))
json.dump(out, open("/data/results/openswe-tasks.json", "w"), indent=1)
