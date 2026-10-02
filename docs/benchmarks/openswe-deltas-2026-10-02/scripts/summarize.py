#!/usr/bin/env python3
"""Tables for the README from results.json (+ verify/eval key=value files): per class, per variant, extrapolation."""
import json, os, statistics, sys, collections
W = sys.argv[1]
R = json.load(open(f"{W}/results.json"))
CLASSES = ["source", "git", "installed", "build", "other", "cache", "tmp"]
MB = 1e6


def q(xs, p):
    xs = sorted(xs)
    if not xs:
        return 0
    k = (len(xs) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def bucket(r):
    pandas = r["project"] == "pandas-dev__pandas"
    return ("pandas" if pandas else "other") + ("-first" if r["first_of_project"] else "-later")


def cls_sum(r, key):
    out = collections.Counter()
    for k, v in r["classes"].items():
        out[k.split("/")[0]] += v[key]
    return out


by_v = collections.defaultdict(list)
for r in R:
    by_v[r["variant"]].append(r)
tasks_all = [r["task"] for r in by_v["base"]]
summary = {"variants": {}, "per_task": {}}
for v, rs in by_v.items():
    rs = [r for r in rs if r["task"] in tasks_all]
    s = {}
    for b in ["pandas-first", "pandas-later", "other-first", "other-later", "all"]:
        sel = [r for r in rs if b == "all" or bucket(r) == b]
        if not sel:
            continue
        e = {"n": len(sel)}
        for key in ("bytes", "new_u", "new_c"):
            xs = [r[key] / MB for r in sel]
            e[key] = {"mean": statistics.mean(xs), "median": statistics.median(xs), "p90": q(xs, 0.9)}
        for key in ("bytes", "new_u", "new_c"):
            e[key + "_by_class"] = {c: statistics.mean(cls_sum(r, key)[c] / MB for r in sel) for c in CLASSES}
        s[b] = e
    summary["variants"][v] = s
for r in R:
    summary["per_task"].setdefault(r["task"], {})[r["variant"]] = {
        "bucket": bucket(r), "bytes": r["bytes"], "new_u": r["new_u"], "new_c": r["new_c"],
        "by_class_new_c": dict(cls_sum(r, "new_c")), "by_class_bytes": dict(cls_sum(r, "bytes")),
        "by_class_new_u": dict(cls_sum(r, "new_u"))}
json.dump(summary, open(f"{W}/summary.json", "w"), indent=1)
for v in summary["variants"]:
    print(v, {b: (e["n"], round(e["new_c"]["mean"]), round(e["new_c"]["median"])) for b, e in summary["variants"][v].items()})
