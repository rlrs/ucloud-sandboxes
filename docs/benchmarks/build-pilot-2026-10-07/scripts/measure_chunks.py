"""Chunk-store bytes the pilot's built images add (C2.14 storage question).
  measure_chunks.py PILOT_DIR FAMILY... (run on the gateway, as root, with the SDK on PYTHONPATH)

For each built image: the layers it adds over its base (the regenerated foundation),
every regular file cut into fixed 256 KiB chunks, each chunk identified by sha256 and
charged its zstd level-3 size, the chunk store's unit. An image is charged only the
chunks no earlier image of its family (in build order) already added. The bases' own
chunks are not loaded, so a task that rewrites base files is overcharged: an upper bound.
Writes PILOT_DIR/chunks.json.
"""
from compression import zstd
from concurrent.futures import ProcessPoolExecutor
import hashlib
import json
import re
import sys
import tarfile
from pathlib import Path
from urllib import request


REGISTRY = "10.36.101.16:5000"
CHUNK = 256 * 1024
ACCEPT = "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"
# REFERENCE unused since build records expire with their builders.
REFERENCE = re.compile(re.escape(REGISTRY) + r"/([a-z0-9._/-]+)(?::[A-Za-z0-9._-]+)?(?:@(sha256:[0-9a-f]{64}))?")


def manifest_layers(repository, reference):
    req = request.Request(f"http://{REGISTRY}/v2/{repository}/manifests/{reference}", headers={"Accept": ACCEPT})
    with request.urlopen(req, timeout=60) as response:
        return [layer["digest"] for layer in json.load(response)["layers"]]


def registry_json(path):
    with request.urlopen(f"http://{REGISTRY}{path}", timeout=120) as response:
        return json.load(response)


def base_layers():
    """Every layer of every regenerated foundation copy: what builds start from."""
    tags = registry_json("/v2/ucloud-regenerated/tags/list").get("tags") or []
    return {digest for tag in tags for digest in manifest_layers("ucloud-regenerated", tag)}


def pilot_repositories():
    """pilot image name -> its repository (the builder appends a hash to the name)."""
    names, last = [], ""
    while True:
        page = registry_json("/v2/_catalog?n=1000" + (f"&last={last}" if last else "")).get("repositories") or []
        names += page
        if len(page) < 1000:
            break
        last = page[-1]
    return {name.removeprefix("ucloud-managed/").rsplit("-", 1)[0]: name
            for name in names if name.startswith("ucloud-managed/pilot-")}


def chunk_layers(job):
    """[(sha256 hex, zstd size)] of every file chunk in the image's added layers."""
    repository, layers = job
    chunks = []
    for digest in layers:
        with request.urlopen(f"http://{REGISTRY}/v2/{repository}/blobs/{digest}", timeout=600) as blob, \
                tarfile.open(fileobj=blob, mode="r|*") as archive:
            for member in archive:
                if not member.isfile():
                    continue
                source = archive.extractfile(member)
                while piece := source.read(CHUNK):
                    chunks.append((hashlib.sha256(piece).hexdigest(), len(zstd.compress(piece, level=3))))
    return chunks


def main():
    root, families = Path(sys.argv[1]), set(sys.argv[2:])
    base, repositories = base_layers(), pilot_repositories()
    rows = [json.loads(line) for line in (root / "results.jsonl").read_text().splitlines()]
    rows = sorted((r for r in rows if r["status"] == "succeeded" and r["family"] in families),
                  key=lambda r: r.get("finished_at") or "")
    jobs = []
    for row in rows:
        repository = repositories[row["name"].replace("_", "-").lower()]
        jobs.append((repository, [d for d in manifest_layers(repository, "latest") if d not in base]))
    with ProcessPoolExecutor(6) as pool:
        chunked = list(pool.map(chunk_layers, jobs))
    seen, out = {family: set() for family in families}, []
    for row, chunks, (_, layers) in zip(rows, chunked, jobs):
        own = dict(chunks)
        new = {digest: size for digest, size in own.items() if digest not in seen[row["family"]]}
        seen[row["family"]].update(own)
        out.append({"id": row["id"], "family": row["family"], "layers": len(layers),
                    "raw_bytes": len(chunks) * CHUNK, "own_zstd": sum(own.values()), "new_zstd": sum(new.values())})
        print(json.dumps(out[-1]), flush=True)
    (root / "chunks.json").write_text(json.dumps(out, indent=1) + "\n")


if __name__ == "__main__":
    main()
