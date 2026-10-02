#!/usr/bin/env python3
"""Where the installed-package new chunks of one variant go: by package directory, and repeat installs.

Same sequential dedupe as analyze.py. For each site-packages top-level entry (or conda env lib/bin entry)
it reports new zstd bytes; it also counts how often a *.dist-info name (package==version) recurs across
tasks and how many new bytes those repeat installs still cost.
"""
import collections, gzip, json, os, re, sys
W, V = sys.argv[1], sys.argv[2]
ORDER = json.load(open(f"{W}/order.json"))
seen = set()
for fn in os.listdir(f"{W}/found"):
    with gzip.open(f"{W}/found/{fn}", "rt") as f:
        for line in f:
            r = json.loads(line)
            if isinstance(r, list):
                seen.update(c[0] for c in r[3])
pk = collections.Counter(); dist_seen = collections.Counter(); repeat_cost = collections.Counter(); first_cost = collections.Counter()
SP = re.compile(r"^/opt/conda/envs/testbed/lib/python[\d.]+/site-packages/([^/]+)")
for o in ORDER:
    fn = f"{W}/trees/{o['task']}/{V}.jsonl.gz"
    if not os.path.exists(fn):
        continue
    recs = [json.loads(l) for l in gzip.open(fn, "rt")]
    recs = [r for r in recs if isinstance(r, list)]
    dists = {m.group(1) for r in recs if (m := SP.match(r[0])) and m.group(1).endswith(".dist-info")}
    norm = lambda s: re.sub(r"[-_.]+", "_", s.lower())
    owner = {}
    for d in dists:
        name = d[:-10].rsplit("-", 1)[0]
        owner[norm(name)] = d
    new = set()
    per_dist = collections.Counter()
    for r in recs:
        p = r[0]
        m = SP.match(p)
        key = ("site:" + m.group(1)) if m else ("env:" + "/".join(p.split("/")[4:6])) if p.startswith("/opt/conda/envs/testbed/") else None
        for d, u, c in r[3]:
            if d in seen or d in new:
                continue
            new.add(d)
            if key:
                pk[key] += c
                if m:
                    top = norm(m.group(1).split(".")[0])
                    if top in owner:
                        per_dist[owner[top]] += c
    for d in dists:
        (repeat_cost if dist_seen[d] else first_cost)[d] += per_dist[d]
        dist_seen[d] += 1
    seen |= new
print("top new-chunk dirs (MB):")
for k, c in pk.most_common(40):
    print(f"  {c/1e6:7.1f}  {k}")
rep = sum(repeat_cost.values()); fst = sum(first_cost.values())
print(f"dist-info names: {len(dist_seen)} distinct, {sum(dist_seen.values())} installs; repeats {sum(v-1 for v in dist_seen.values())}")
print(f"new MB attributable to first install of a package==version: {fst/1e6:.0f}; to repeat installs of the same package==version: {rep/1e6:.0f}")
for d, c in repeat_cost.most_common(15):
    print(f"   repeat {c/1e6:6.1f} MB {d} (x{dist_seen[d]})")
