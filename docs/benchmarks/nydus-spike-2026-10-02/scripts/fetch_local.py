#!/usr/bin/env python3
"""Fetch the locally built OpenSWE task images from the VM registry (localhost:5001) into the blob store."""
import gzip, hashlib, json, os, urllib.request, zlib
REG = "http://127.0.0.1:5001"
ACCEPT = "application/vnd.docker.distribution.manifest.v2+json,application/vnd.oci.image.manifest.v1+json"
out = []
for l in open("/data/logs/openswe-builds.jsonl"):
    r = json.loads(l)
    if r.get("status") != "ok":
        continue
    repo, tag = r["tag"].split("/", 1)[1], "latest"
    req = urllib.request.Request(f"{REG}/v2/{repo}/manifests/{tag}", headers={"Accept": ACCEPT})
    raw = urllib.request.urlopen(req).read(); m = json.loads(raw)
    for b in [m["config"]] + m["layers"]:
        p = f"/data/oci/blobs/sha256/{b['digest'].split(':')[1]}"
        if not os.path.exists(p):
            data = urllib.request.urlopen(f"{REG}/v2/{repo}/blobs/{b['digest']}").read()
            assert "sha256:" + hashlib.sha256(data).hexdigest() == b["digest"]
            open(p, "wb").write(data)
    cfg = json.load(open(f"/data/oci/blobs/sha256/{m['config']['digest'].split(':')[1]}"))
    out.append({"group": "openswe-task-pandas" if "pandas-dev" in r["task"] else "openswe-task-other",
                "family": "OpenSWE-task", "cached_kind": "built-locally", "image": r["task"],
                "prepared_reference": f"localhost:5001/{repo}:{tag}@sha256:{hashlib.sha256(raw).hexdigest()}",
                "repo": repo, "digest": "sha256:" + hashlib.sha256(raw).hexdigest(), "manifest": m, "env": None,
                "erofs_bytes": 0, "diff_ids": cfg["rootfs"]["diff_ids"], "build_seconds": r["build_seconds"]})
    print(r["task"], len(m["layers"]), sum(x["size"] for x in m["layers"]))
json.dump(out, open("/data/manifests/openswe-manifests.json", "w"), indent=1)
