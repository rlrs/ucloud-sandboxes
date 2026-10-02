#!/usr/bin/env python3
"""Per-class delta and new-chunk accounting for OpenSWE task trees over their foundations.

For every variant separately, chunks are deduplicated in processing order against the chunk set of
all foundations plus every task tree of that variant processed before (the S10 chunk-store method:
a 256 KiB chunk costs its zstd size the first time its sha256 is seen; files deleted or shadowed in the
final tree cost nothing). Within one image, a chunk shared by several files is charged to the most
essential class that holds it (source > git > installed > build > other > cache > tmp).
"""
import collections, gzip, json, os, re, sys

W = sys.argv[1]                       # work dir with found/, trees/, tasks.json
ORDER = json.load(open(f"{W}/order.json"))
VARIANTS = sys.argv[2].split(",")

CLASSES = ["source", "git", "installed", "build", "other", "cache", "tmp"]
PRIO = {c: i for i, c in enumerate(CLASSES)}
SO = re.compile(r"\.so(\.[\d.]+)?$|\.pyd$")


def classify(p, tracked):
    if p == "/testbed/.git" or p.startswith("/testbed/.git/"):
        r = p[len("/testbed/.git/"):]
        return ("git", "pack" if r.startswith("objects/pack/") else "loose" if r.startswith("objects/") else "other")
    parts = p.split("/")
    base = parts[-1]
    if p.startswith("/testbed/"):
        rel = p[len("/testbed/"):]
        if "__pycache__" in parts or base.endswith((".pyc", ".pyo")):
            return ("build", "pyc")
        if any(x.endswith((".egg-info", ".dist-info")) for x in parts) or ".eggs" in parts:
            return ("build", "egg-info")
        if rel in tracked:
            return ("source", "tracked")
        if parts[2] in ("build", "builddir", "_build") or rel.startswith("build"):
            return ("build", "build-dir")
        if base.endswith((".o", ".a", ".obj")):
            return ("build", "obj")
        if SO.search(base):
            return ("build", "so")
        if base.endswith((".c", ".cpp", ".cxx", ".h", ".html")) :
            return ("build", "generated-src")
        if base.endswith(".log"):
            return ("tmp", "log")
        return ("build", "untracked-other")
    if p.startswith(("/tmp/", "/var/tmp/")):
        return ("tmp", "tmp")
    if p.startswith("/var/log/") or base.endswith(".log"):
        return ("tmp", "log")
    if "/.cache/" in p or p.startswith(("/root/.cache", "/root/.npm", "/root/.cargo/registry", "/root/.conda/pkgs")):
        sub = "pip" if "/.cache/pip" in p else "uv" if "/.cache/uv" in p else "other"
        return ("cache", sub)
    if p.startswith("/opt/conda/pkgs/"):
        r = p[len("/opt/conda/pkgs/"):]
        if "/" not in r or r.startswith("cache/"):
            return ("cache", "conda-tarballs")
        return ("installed", "conda-pkgs-extracted")
    if p.startswith(("/var/lib/apt/lists", "/var/cache/apt", "/var/cache/debconf")):
        return ("cache", "apt")
    if "site-packages/" in p or "dist-packages/" in p:
        if "__pycache__" in parts or base.endswith(".pyc"):
            return ("installed", "pyc")
        if SO.search(base):
            return ("installed", "so")
        return ("installed", "site-packages")
    if p.startswith("/opt/conda/envs/") or p.startswith("/opt/conda/"):
        if "__pycache__" in parts or base.endswith(".pyc"):
            return ("installed", "pyc")
        return ("installed", "conda-env-other")
    if p.startswith("/root/.local/") or p.startswith("/usr/local/lib/python"):
        return ("installed", "site-packages")
    if p.startswith(("/usr/", "/lib", "/bin", "/sbin")):
        return ("other", "system-usr")
    if p.startswith(("/var/lib/dpkg", "/etc/")):
        return ("other", "dpkg-etc")
    if p.startswith("/root/") or p.startswith("/home/"):
        return ("other", "home")
    return ("other", "other")


def load(path):
    recs, tail = [], {}
    with gzip.open(path, "rt") as f:
        for line in f:
            r = json.loads(line)
            if isinstance(r, list):
                recs.append(r)
            else:
                tail = r
    return recs, tail


def main():
    seen0 = set()
    found = {}
    for fn in sorted(os.listdir(f"{W}/found")):
        recs, _ = load(f"{W}/found/{fn}")
        found[fn.split(".")[0]] = {r[0]: r for r in recs}
        for r in recs:
            seen0.update(c[0] for c in r[3])
    print("foundation chunks", len(seen0), file=sys.stderr)
    results = []
    for v in VARIANTS:
        seen = set(seen0)
        for o in ORDER:
            task = o["task"]
            fn = f"{W}/trees/{task}/{v}.jsonl.gz"
            if not os.path.exists(fn):
                continue
            recs, tail = load(fn)
            tr = f"{W}/trees/{task}/tracked.txt"
            tracked = set(open(tr).read().split("\0")) if os.path.exists(tr) else set()
            fmap = found.get(o["foundation_key"], {})
            byp = {r[0]: r for r in recs}
            items = []
            for r in recs:
                p, t, size, chunks, link = r[0], r[1], r[2], r[3], r[4]
                if t == "h":   # hard link: the data lives at the link target (this tree or foundation)
                    tgt = "/" + link.lstrip("./")
                    src = byp.get(tgt) or fmap.get(tgt)
                    chunks = src[3] if src else []
                    size = 0
                cls, sub = classify(p, tracked)
                items.append((PRIO[cls], p, cls, sub, size, chunks))
            items.sort()
            agg = collections.defaultdict(lambda: [0, 0, 0, 0])   # bytes, files, new_u, new_c
            mine = set()
            topdirs = collections.Counter()
            for _, p, cls, sub, size, chunks in items:
                a = agg[(cls, sub)]
                a[0] += size; a[1] += 1
                for d, u, c in chunks:
                    if d in seen or d in mine:
                        continue
                    mine.add(d); a[2] += u; a[3] += c
                    topdirs["/".join(p.split("/")[:5])] += c
            seen |= mine
            rec = {"variant": v, "task": task, "group": o["group"], "project": o["project"], "pos": o["pos"],
                   "first_of_project": o["first_of_project"], "deleted_foundation_paths": len(tail.get("deleted", [])),
                   "classes": {f"{c}/{s}": dict(zip(["bytes", "files", "new_u", "new_c"], a)) for (c, s), a in sorted(agg.items())},
                   "top_dirs_new_c": topdirs.most_common(12)}
            for k in ("bytes", "new_u", "new_c"):
                rec[k] = sum(x[k] for x in rec["classes"].values())
            results.append(rec)
            print(v, task, round(rec["bytes"] / 1e6), round(rec["new_u"] / 1e6), round(rec["new_c"] / 1e6), file=sys.stderr)
    json.dump(results, open(f"{W}/results.json", "w"), indent=1)


main()
