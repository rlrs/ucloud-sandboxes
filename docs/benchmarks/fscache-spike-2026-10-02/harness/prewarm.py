"""Fill the local pull-through cache (5002) with every environment blob the selection reads."""
import json, sys, time
sys.path.insert(0, "/root/s11")
import s11lib
from ucloud_sandboxes.environment_artifact import load_image_environment
reg = s11lib.env_registry()
out = {}
for img in json.load(open("/root/s11/images.json"))["images"]:
    repo, digest = s11lib.split_ref(img["prepared_reference"])
    root, env = load_image_environment(reg, repo, digest)
    comps = []
    for c in env.components:
        comp = reg.load(c)
        whole = reg.whole_image(comp)
        t0 = time.monotonic(); n = 0
        if whole:
            data, _ = s11lib.reg_get(s11lib.PROXY_REG, f"/v2/environments/blobs/{comp.image_digest}"); n = len(data)
        else:
            for ch in comp.chunks:
                data, _ = s11lib.reg_get(s11lib.PROXY_REG, f"/v2/environments/blobs/{ch.digest}"); n += len(data)
        comps.append({"component": c, "type": type(comp).__name__, "image_size": comp.image_size, "whole_image": whole,
                      "chunks": len(comp.chunks), "fetched": n, "seconds": round(time.monotonic() - t0, 2)})
    cfg, m = s11lib.image_config(img["prepared_reference"])
    out[img["name"]] = {"root": root, "components": comps, "oci_layers": [l["digest"] for l in m["layers"]],
                        "oci_layer_bytes": sum(l["size"] for l in m["layers"]),
                        "workdir": (cfg.get("config") or {}).get("WorkingDir")}
    print(img["name"], len(comps), sum(c["image_size"] for c in comps), flush=True)
json.dump(out, open("/root/s11/out/environments.json", "w"), indent=1)
