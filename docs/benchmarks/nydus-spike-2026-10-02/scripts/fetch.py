#!/usr/bin/env python3
"""Read-only fetch of the sample's OCI manifests, blobs and environment metadata from 10.42.0.2:5000."""
import hashlib, json, os, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
REG = "http://10.42.0.2:5000"
ROOT = "/data"
ACCEPT = ",".join(["application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json",
                   "application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json"])

def get(path, accept=ACCEPT):
    req = urllib.request.Request(REG + path, headers={"Accept": accept})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()

def split(ref):
    name, digest = ref.split("@")
    repo = name.split("/", 1)[1].rsplit(":", 1)[0]
    return repo, digest

def blob_path(d):
    return f"{ROOT}/oci/blobs/sha256/{d.split(':')[1]}"

def fetch_blob(repo, d):
    path = blob_path(d)
    if os.path.exists(path):
        return 0
    tmp = path + ".part"
    h = hashlib.sha256()
    req = urllib.request.Request(f"{REG}/v2/{repo}/blobs/{d}")
    with urllib.request.urlopen(req, timeout=600) as r, open(tmp, "wb") as f:
        while True:
            b = r.read(4 << 20)
            if not b:
                break
            h.update(b); f.write(b)
    if "sha256:" + h.hexdigest() != d:
        os.unlink(tmp); raise ValueError(f"digest mismatch {d}")
    os.rename(tmp, path)
    return os.path.getsize(path)

def env_info(annot):
    root = annot.get("org.ucloud.immutable-environment.v1")
    if not root:
        return None
    m = json.loads(get(f"/v2/environments/manifests/{root}"))
    cfg = json.loads(get(f"/v2/environments/blobs/{m['config']['digest']}"))
    env = cfg["environment"]
    comps = [env["base"], *([env["workspace"]] if env.get("workspace") else []), *env.get("toolkits", [])]
    out = []
    for c in comps:
        cm = json.loads(get(f"/v2/environments/manifests/{c}"))
        out.append({"component": c, "layers": cm["layers"], "config": cm["config"]})
    return {"root": root, "components": out}

def main():
    sample = json.load(open(sys.argv[1]))
    jobs = []
    results = []
    for i, s in enumerate(sample):
        repo, digest = split(s["prepared_reference"])
        raw = get(f"/v2/{repo}/manifests/{digest}")
        assert "sha256:" + hashlib.sha256(raw).hexdigest() == digest
        m = json.loads(raw)
        if "manifests" in m:
            raise SystemExit(f"index manifest for {s['image']}")
        rec = dict(s, idx=i, repo=repo, digest=digest, manifest=m, env=env_info(m.get("annotations", {})))
        results.append(rec)
        jobs.append((repo, m["config"]["digest"]))
        jobs += [(repo, l["digest"]) for l in m["layers"]]
    json.dump(results, open(f"{ROOT}/manifests/sample-manifests.json", "w"), indent=1)
    uniq = {}
    for repo, d in jobs:
        uniq.setdefault(d, repo)
    print("blobs", len(uniq), "bytes", sum(l["size"] for r in results for l in r["manifest"]["layers"]), flush=True)
    t = time.time(); done = 0
    with ThreadPoolExecutor(12) as ex:
        for n in ex.map(lambda kv: fetch_blob(kv[1], kv[0]), uniq.items()):
            done += n
    print("fetched", done, "bytes in", round(time.time() - t, 1), "s", flush=True)

main()
