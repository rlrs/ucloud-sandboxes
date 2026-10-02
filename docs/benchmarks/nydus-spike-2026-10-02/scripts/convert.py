#!/usr/bin/env python3
"""Convert the sample's OCI layers to RAFS v6 with nydus-image, with or without an incremental chunk dict.

nodict: every unique OCI layer -> one bootstrap + blob (parallel); images merged from their layers.
dict:   images in a fixed shuffled order; each layer built with --chunk-dict <dict.boot>, where the dict
        is merge(previous dict, previous image bootstraps). Strictly sequential.
"""
import argparse, json, os, random, shutil, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "/root")
import rafs

BLOBS = "/data/oci/blobs/sha256"


def run(cmd):
    t = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out = p.stdout.read()
    _, status, ru = os.wait4(p.pid, 0)
    p.returncode = os.waitstatus_to_exitcode(status)
    return {"rc": p.returncode, "wall": time.time() - t, "cpu": ru.ru_utime + ru.ru_stime,
            "maxrss_kb": ru.ru_maxrss, "out": out.decode(errors="replace")[-2000:]}


def create(args, layer, boot, dict_boot=None):
    cmd = ["nydus-image", "create", "-t", "targz-rafs", "--fs-version", "6", "--digester", "sha256",
           "--compressor", args.compressor, "--chunk-size", args.chunk_size, "-D", args.out + "/blobs",
           "-B", boot, "-J", boot + ".json", "--repeatable", f"{BLOBS}/{layer.split(':')[1]}"]
    if dict_boot:
        cmd[2:2] = ["--chunk-dict", "bootstrap=" + dict_boot]
    r = run(cmd)
    if r["rc"] != 0:
        raise RuntimeError(f"create {layer}: {r['out']}")
    r["blobs"] = json.load(open(boot + ".json"))["blobs"]
    dict_ids = {d["blob_id"] for d in rafs.read(dict_boot)["devices"]} if dict_boot else set()
    own = [b for b in r["blobs"] if b not in dict_ids]
    assert len(own) <= 1, own
    r["own_blob"] = own[0] if own else "0" * 64
    return r


def merge(args, out, sources, dict_boot=None, ids=None):
    # merge names each layer's blob after its bootstrap file name unless told the real blob ids.
    extra = ["--chunk-dict", "bootstrap=" + dict_boot] if dict_boot else []
    if ids:
        extra += ["--original-blob-ids", ",".join(ids)]
    r = run(["nydus-image", "merge", *extra, "-B", out, "-J", out + ".json", *sources])
    return r


def blob_size(args, blob_id):
    p = f"{args.out}/blobs/{blob_id}"
    return os.path.getsize(p) if os.path.exists(p) else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["nodict", "dict"], required=True)
    ap.add_argument("--chunk-size", default="0x100000")
    ap.add_argument("--compressor", default="zstd")
    ap.add_argument("--out", required=True)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--seed", type=int, default=13)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    os.makedirs(args.out + "/blobs", exist_ok=True)
    os.makedirs(args.out + "/layers", exist_ok=True)
    os.makedirs(args.out + "/images", exist_ok=True)
    sample = json.load(open(os.environ.get("SAMPLE_MANIFESTS", "/data/manifests/sample-manifests.json")))
    order = list(range(len(sample)))
    if args.seed >= 0:
        random.Random(args.seed).shuffle(order)
    if args.limit:
        order = order[:args.limit]
    log = open(args.out + "/images.jsonl", "a")
    seen_blobs, layer_done = set(), {}
    if args.mode == "nodict":
        uniq = {}
        for i in order:
            for l in sample[i]["manifest"]["layers"]:
                uniq.setdefault(l["digest"], l["size"])
        t0 = time.time()

        def one(d):
            boot = f"{args.out}/layers/{d.split(':')[1]}.boot"
            return d, create(args, d, boot)
        with ThreadPoolExecutor(args.workers) as ex:
            for d, r in ex.map(one, sorted(uniq, key=lambda d: -uniq[d])):
                layer_done[d] = r
        print("layers", len(uniq), "wall", round(time.time() - t0, 1), flush=True)
    dict_boot = None
    dict_frozen_at = None
    for pos, i in enumerate(order):
        s = sample[i]
        rec = {"pos": pos, "idx": i, "group": s["group"], "family": s["family"], "image": s["image"],
               "oci_bytes": sum(l["size"] for l in s["manifest"]["layers"]), "erofs_bytes": s["erofs_bytes"],
               "layers": len(s["manifest"]["layers"]), "create_wall": 0.0, "create_cpu": 0.0, "layers_reused": 0}
        boots, new_bytes, new_blobs, ids = [], 0, [], []
        for l in s["manifest"]["layers"]:
            d = l["digest"]
            boot = f"{args.out}/layers/{d.split(':')[1]}.boot"
            if args.mode == "dict":
                if d in layer_done:
                    rec["layers_reused"] += 1
                else:
                    layer_done[d] = create(args, d, boot, dict_boot)
                    r = layer_done[d]
                    rec["create_wall"] += r["wall"]; rec["create_cpu"] += r["cpu"]
            else:
                r = layer_done[d]
                if d in seen_blobs:
                    rec["layers_reused"] += 1
                else:
                    rec["create_wall"] += r["wall"]; rec["create_cpu"] += r["cpu"]
            seen_blobs.add(d)
            for b in layer_done[d]["blobs"]:
                if b not in seen_blobs:
                    seen_blobs.add(b)
                    new_blobs.append(b)
                    new_bytes += blob_size(args, b)
            boots.append(boot)
            ids.append(layer_done[d]["own_blob"])
        img = f"{args.out}/images/{i:03d}.boot"
        m = merge(args, img, boots, dict_boot, ids)
        if m["rc"] != 0 and args.mode == "dict" and rec["layers_reused"]:
            # A reused layer bootstrap can name a blob the current dict no longer lists
            # (merge drops blobs whose chunks are all shadowed): rebuild those layers.
            rec["rebuilt_layers"] = 0
            for k, l in enumerate(s["manifest"]["layers"]):
                d = l["digest"]
                r = layer_done[d]
                fresh = f"{args.out}/layers/{d.split(':')[1]}.{pos}.boot"
                r2 = create(args, d, fresh, dict_boot)
                rec["create_wall"] += r2["wall"]; rec["create_cpu"] += r2["cpu"]; rec["rebuilt_layers"] += 1
                for b in r2["blobs"]:
                    if b not in seen_blobs:
                        seen_blobs.add(b); new_blobs.append(b); new_bytes += blob_size(args, b)
                boots[k] = fresh; ids[k] = r2["own_blob"]
            m = merge(args, img, boots, dict_boot, ids)
        rec.update(merge_wall=m["wall"], merge_cpu=m["cpu"], merge_rc=m["rc"])
        if m["rc"] != 0:
            rec["merge_error"] = m["out"][-500:]
        else:
            info = rafs.read(img)
            rec.update(bootstrap_bytes=info["size"], image_blobs=len(info["devices"]),
                       chunks=len(info["chunks"]), build_time=info["build_time"])
            json.dump({"devices": info["devices"], "chunks": info["chunks"]}, open(img + ".chunks.json", "w"))
        rec.update(new_blob_bytes=new_bytes, new_blobs=len(new_blobs))
        if args.mode == "dict" and m["rc"] == 0 and dict_frozen_at is None:
            nd = f"{args.out}/dict.next.boot"
            dm = merge(args, nd, ([dict_boot] if dict_boot else []) + boots, dict_boot,
                       (["0" * 64] if dict_boot else []) + ids)
            rec.update(dict_merge_wall=dm["wall"], dict_merge_cpu=dm["cpu"])
            if dm["rc"] == 0:
                os.replace(nd, f"{args.out}/dict.boot")
                dict_boot = f"{args.out}/dict.boot"
                di = rafs.read(dict_boot)
                rec.update(dict_bytes=di["size"], dict_blobs=len(di["devices"]), dict_chunks=len(di["chunks"]))
            else:
                dict_frozen_at = pos
                rec["dict_merge_error"] = dm["out"][-800:]
                print("dict frozen at", pos, dm["out"][-300:], flush=True)
        rec["dict_frozen"] = dict_frozen_at is not None
        log.write(json.dumps(rec) + "\n"); log.flush()
        print(pos, s["family"], rec.get("bootstrap_bytes"), new_bytes, round(rec["create_wall"], 1),
              rec.get("image_blobs"), rec.get("dict_blobs"), flush=True)


main()
