"""Per-image storage facts: OCI, our EROFS components, Nydus bootstrap + blobs; conversion times."""
import json, sys
sys.path.insert(0, "/root/s11")
import s11lib as L
envs = json.load(open("/root/s11/out/environments.json"))
conv = {}
for f in ("results.jsonl", "results-extra.jsonl"):
    for line in open(f"/root/s11/convert/{f}"):
        r = json.loads(line); conv[r["name"]] = r
out = {"images": {}}
all_blobs = {}
names = [i["name"] for i in json.load(open("/root/s11/images.json"))["images"]] + \
        ["scaleswe-responses-1-dict", "scaleswe-responses-2-dict", "scaleswe-responses-3-dict", "scaleswe-oauthlib-1-dict", "tlego-3-raw"]
for n in names:
    m, boot, blobs = L.nydus_layers(n)
    base = n.replace("-dict", "").replace("-raw", "")
    e = envs.get(base, {})
    r = {"nydus_layers": len(m["layers"]), "nydus_bootstrap_layer_bytes": boot["size"],
         "nydus_blobs": len(blobs), "nydus_blob_bytes": sum(b["size"] for b in blobs),
         "nydus_blob_digests": [b["digest"][7:19] for b in blobs],
         "oci_layers": len(e.get("oci_layers", [])), "oci_layer_bytes": e.get("oci_layer_bytes"),
         "erofs_components": len(e.get("components", [])),
         "erofs_component_bytes": sum(c["image_size"] for c in e.get("components", [])),
         "convert_seconds": conv.get(n, {}).get("seconds"), "convert_rc": conv.get(n, {}).get("rc")}
    try:
        r["nydusify_metrics"] = json.load(open(f"/root/s11/convert/{n}.metrics.json"))
    except Exception as exc:
        r["nydusify_metrics"] = repr(exc)
    if not n.endswith(("-dict", "-raw")):
        for b in blobs:
            all_blobs.setdefault(b["digest"], b["size"])
    out["images"][n] = r
base20 = [n for n in names if not n.endswith(("-dict", "-raw"))]
out["totals_20"] = {
    "oci_layer_bytes": sum(out["images"][n]["oci_layer_bytes"] or 0 for n in base20),
    "erofs_component_bytes_sum": sum(out["images"][n]["erofs_component_bytes"] for n in base20),
    "erofs_component_bytes_distinct": sum({c["component"]: c["image_size"] for n in base20 for c in envs[n]["components"]}.values()),
    "nydus_blob_bytes_sum": sum(out["images"][n]["nydus_blob_bytes"] for n in base20),
    "nydus_blob_bytes_distinct": sum(all_blobs.values()),
    "nydus_bootstrap_bytes": sum(out["images"][n]["nydus_bootstrap_layer_bytes"] for n in base20),
}
json.dump(out, open("/root/s11/out/storage.json", "w"), indent=1)
print(json.dumps(out["totals_20"], indent=1))
for n in names:
    r = out["images"][n]; print(n, r["oci_layers"], r["erofs_components"], r["nydus_blobs"], r["nydus_bootstrap_layer_bytes"], r["nydus_blob_bytes"], r["erofs_component_bytes"], r["convert_seconds"])
