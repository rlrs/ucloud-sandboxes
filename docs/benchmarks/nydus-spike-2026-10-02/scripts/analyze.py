#!/usr/bin/env python3
"""Storage accounting for the spike. Writes /data/results/storage-<chunk>.json.

today:        unique OCI layer blobs + unique EROFS component blobs (registry stores each digest once).
nydus_nodict: unique nydus blobs (one per unique OCI layer) + per-image bootstraps.
nydus_dict:   blobs added by the stock nydus-image incremental chunk dict run + bootstraps.
cas_oracle:   chunk-level content addressing over the converted images: a chunk (sha256 of its
              uncompressed bytes) is stored once, compressed; chunks shadowed by upper layers are
              never stored. Plus bootstraps. This is what our own chunk store would hold.
All in the dict run's (shuffled) order, so "new bytes" are comparable per image.
"""
import json, os, sys, collections
FAM = lambda f: {"SWE-smith": "SWE-smith/R2E/rebench/Lego", "R2E-Gym": "SWE-smith/R2E/rebench/Lego",
                 "SWE-rebench v2": "SWE-smith/R2E/rebench/Lego", "SWE-Lego": "SWE-smith/R2E/rebench/Lego"}.get(f, f)


def load(path):
    return [json.loads(l) for l in open(path)] if os.path.exists(path) else []


def main(tag, nodict_dir, dict_dir):
    sample = json.load(open("/data/manifests/sample-manifests.json"))
    nod = {r["idx"]: r for r in load(f"{nodict_dir}/images.jsonl")}
    dic = {r["idx"]: r for r in load(f"{dict_dir}/images.jsonl")} if dict_dir else {}
    order = [r["idx"] for r in load(f"{nodict_dir}/images.jsonl")]
    seen_oci, seen_erofs, seen_chunks, seen_nblobs = set(), set(), set(), set()
    per_image, fam = [], collections.defaultdict(lambda: collections.Counter())
    for i in order:
        s = sample[i]; n = nod[i]; f = FAM(s["family"])
        rec = {"idx": i, "family": s["family"], "group": s["group"], "image": s["image"],
               "oci_compressed_bytes": sum(l["size"] for l in s["manifest"]["layers"]),
               "erofs_bytes_selection": s["erofs_bytes"]}
        rec["oci_new_bytes"] = sum(l["size"] for l in s["manifest"]["layers"] if l["digest"] not in seen_oci)
        seen_oci.update(l["digest"] for l in s["manifest"]["layers"])
        comps = [(c["layers"][0]["digest"], c["layers"][0]["size"]) for c in s["env"]["components"]]
        rec["erofs_component_bytes"] = sum(sz for _, sz in comps)
        rec["erofs_new_bytes"] = sum(sz for d, sz in dict(comps).items() if d not in seen_erofs)
        seen_erofs.update(d for d, _ in comps)
        rec["bootstrap_bytes"] = n.get("bootstrap_bytes", 0)
        rec["nodict_new_blob_bytes"] = n["new_blob_bytes"]
        rec["nodict_convert_wall"] = n["create_wall"] + n["merge_wall"]; rec["nodict_convert_cpu"] = n["create_cpu"] + n["merge_cpu"]
        ch = json.load(open(f"{nodict_dir}/images/{i:03d}.boot.chunks.json"))["chunks"]
        uniq = {}
        for (dg, bi, fl, cs, us, co, uo) in ch:
            uniq.setdefault(dg, (cs, us))
        rec["image_chunks"] = len(uniq)
        rec["image_unique_compressed"] = sum(cs for cs, _ in uniq.values())
        rec["image_unique_uncompressed"] = sum(us for _, us in uniq.values())
        new = [v for d, v in uniq.items() if d not in seen_chunks]
        seen_chunks.update(uniq)
        rec["cas_new_compressed"] = sum(cs for cs, _ in new); rec["cas_new_uncompressed"] = sum(us for _, us in new)
        rec["cas_new_chunks"] = len(new)
        if i in dic:
            d = dic[i]
            rec.update(dict_new_blob_bytes=d["new_blob_bytes"], dict_bootstrap_bytes=d.get("bootstrap_bytes", 0),
                       dict_convert_wall=d["create_wall"] + d["merge_wall"] + d.get("dict_merge_wall", 0),
                       dict_convert_cpu=d["create_cpu"] + d["merge_cpu"] + d.get("dict_merge_cpu", 0),
                       dict_image_blobs=d.get("image_blobs"), dict_blobs=d.get("dict_blobs"), dict_frozen=d["dict_frozen"],
                       dict_bytes=d.get("dict_bytes"), merge_rc=d["merge_rc"])
        per_image.append(rec)
        c = fam[f]; c["images"] += 1
        for k in ("oci_compressed_bytes", "oci_new_bytes", "erofs_new_bytes", "erofs_component_bytes", "bootstrap_bytes",
                  "nodict_new_blob_bytes", "cas_new_compressed", "cas_new_uncompressed", "image_unique_compressed",
                  "image_unique_uncompressed", "dict_new_blob_bytes", "dict_bootstrap_bytes", "nodict_convert_wall",
                  "nodict_convert_cpu", "dict_convert_wall", "dict_convert_cpu", "erofs_bytes_selection"):
            c[k] += rec.get(k) or 0
        c["dict_images"] += i in dic
    tot = collections.Counter()
    for c in fam.values():
        tot.update(c)
    def views(c):
        return {"today_oci_plus_erofs": c["oci_new_bytes"] + c["erofs_new_bytes"],
                "today_oci_only": c["oci_new_bytes"], "today_erofs_only": c["erofs_new_bytes"],
                "nydus_nodict": c["nodict_new_blob_bytes"] + c["bootstrap_bytes"],
                "nydus_dict_stock": (c["dict_new_blob_bytes"] + c["dict_bootstrap_bytes"]) if c["dict_images"] == c["images"] else None,
                "nydus_cas": c["cas_new_compressed"] + c["bootstrap_bytes"],
                "bootstraps": c["bootstrap_bytes"]}
    out = {"tag": tag, "images": len(per_image), "chunks_seen": len(seen_chunks),
           "families": {f: dict(c) | {"views": views(c)} for f, c in fam.items()},
           "total": dict(tot) | {"views": views(tot)}, "per_image": per_image}
    os.makedirs("/data/results", exist_ok=True)
    json.dump(out, open(f"/data/results/storage-{tag}.json", "w"), indent=1)
    G = 1e9
    print(tag, "images", len(per_image))
    for f, c in sorted(fam.items()) + [("TOTAL", tot)]:
        v = views(c)
        print(f"{f:28s} n={c['images']:3d} OCI-new {c['oci_new_bytes']/G:7.2f} EROFS-new {c['erofs_new_bytes']/G:7.2f} "
              f"today {v['today_oci_plus_erofs']/G:7.2f} | nodict {v['nydus_nodict']/G:7.2f} "
              f"dict {(v['nydus_dict_stock'] or 0)/G:7.2f} ({c['dict_images']}) cas {v['nydus_cas']/G:7.2f} boot {c['bootstrap_bytes']/G:6.3f}")


main(*sys.argv[1:3], sys.argv[3] if len(sys.argv) > 3 else None)
