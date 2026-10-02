"""Compare nydus's own export --block of stock-dict images with the no-dict conversion, at several positions."""
import hashlib, json, os, subprocess, sys
pos2idx = {json.loads(l)["pos"]: json.loads(l) for l in open("/data/work/dict-1m/images.jsonl")}
os.makedirs("/data/export", exist_ok=True)
out = []
for pos in [int(x) for x in sys.argv[1:]]:
    r = pos2idx[pos]; idx = r["idx"]
    res = {"pos": pos, "idx": idx, "family": r["family"], "image_blobs": r.get("image_blobs"), "rebuilt_layers": r.get("rebuilt_layers"), "layers_reused": r["layers_reused"]}
    for run in ("dict-1m", "nodict-1m"):
        img = f"/data/export/{run}-{idx}.img"; mnt = f"/mnt/x-{run}"
        os.makedirs(mnt, exist_ok=True)
        p = subprocess.run(["nydus-image", "export", "--block", "--bootstrap", f"/data/work/{run}/images/{idx:03d}.boot",
                            "--localfs-dir", f"/data/work/{run}/blobs", "--output", img], capture_output=True, text=True)
        res[run + "_export_rc"] = p.returncode
        if p.returncode or not os.path.exists(img):
            res[run + "_export_error"] = (p.stdout + p.stderr)[-400:]
            continue
        subprocess.run(["mount", "-t", "erofs", "-o", "ro", img, mnt], check=True)
    if any(k.endswith("_export_error") for k in res):
        for run in ("dict-1m", "nodict-1m"):
            subprocess.run(["umount", f"/mnt/x-{run}"], capture_output=True)
            if os.path.exists(f"/data/export/{run}-{idx}.img"): os.unlink(f"/data/export/{run}-{idx}.img")
        print(json.dumps(res), flush=True); out.append(res); continue
    n = bad = eio = 0
    for dp, dn, fn in os.walk("/mnt/x-nodict-1m"):
        for f in fn:
            a = os.path.join(dp, f)
            if os.path.islink(a) or not os.path.isfile(a): continue
            b = "/mnt/x-dict-1m" + a[len("/mnt/x-nodict-1m"):]
            n += 1
            try:
                ok = hashlib.sha256(open(a, "rb").read()).digest() == hashlib.sha256(open(b, "rb").read()).digest()
            except OSError:
                ok = False; eio += 1
            bad += not ok
    res.update(files=n, differ=bad, eio=eio)
    for run in ("dict-1m", "nodict-1m"):
        subprocess.run(["umount", f"/mnt/x-{run}"]); os.unlink(f"/data/export/{run}-{idx}.img")
    print(json.dumps(res), flush=True); out.append(res)
json.dump(out, open("/data/results/dict-integrity.json", "w"), indent=1)
