#!/usr/bin/env python3
"""Markdown tables for the README from results.json."""
import collections, json, statistics, sys
R = json.load(open(sys.argv[1]))
CL = ["source", "git", "installed", "build", "other", "cache", "tmp"]
V = ["base", "a", "a_gc", "a_shallow", "a_shallow_loose", "a_placeholder", "a_sharedpack", "c"]
MB = 1e6
def q(xs, p):
    xs = sorted(xs); k = (len(xs) - 1) * p; lo = int(k); hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)
def cls(r, key):
    c = collections.Counter()
    for k, v in r["classes"].items(): c[k.split("/")[0]] += v[key]
    return c
by = {(r["variant"], r["task"]): r for r in R}
base = [r for r in R if r["variant"] == "base"]
out = []
out.append("| # | Task | Kind | Delta MB | New chunks MB (unc.) | **New chunks MB (zstd)** | source | git | installed | build | other | cache | tmp | (c) zstd MB |")
out.append("|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for i, r in enumerate(sorted(base, key=lambda r: r["pos"])):
    c = cls(r, "new_c"); kind = ("first" if r["first_of_project"] else "later")
    cv = by.get(("c", r["task"]))
    out.append(f"| {i+1} | {r['task'][9:]} | {kind} | {r['bytes']/MB:,.0f} | {r['new_u']/MB:,.0f} | **{r['new_c']/MB:,.0f}** | " +
               " | ".join(f"{c[x]/MB:,.0f}" for x in CL) + f" | {cv['new_c']/MB:,.0f} |")
out.append("")
# per-class aggregate for base: bytes and new_c and new_u
out.append("| Class | Delta MB mean | median | p90 | New chunks (unc.) MB mean | **New chunks (zstd) MB mean** | median | p90 | share of new zstd |")
out.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
tot = sum(r["new_c"] for r in base)
for x in CL + ["total"]:
    b = [(cls(r, "bytes")[x] if x != "total" else r["bytes"]) / MB for r in base]
    u = [(cls(r, "new_u")[x] if x != "total" else r["new_u"]) / MB for r in base]
    c = [(cls(r, "new_c")[x] if x != "total" else r["new_c"]) / MB for r in base]
    out.append(f"| {x} | {statistics.mean(b):,.0f} | {statistics.median(b):,.0f} | {q(b,.9):,.0f} | {statistics.mean(u):,.0f} | **{statistics.mean(c):,.0f}** | {statistics.median(c):,.0f} | {q(c,.9):,.0f} | {100*sum(c)*MB/tot:.0f}% |")
out.append("")
# subclasses for base and c
sub = collections.defaultdict(lambda: collections.Counter())
for r in R:
    for k, v in r["classes"].items():
        sub[r["variant"]][k] += v["new_c"]
        sub[r["variant"] + "#b"][k] += v["bytes"]
n = len(base)
keys = sorted(set(sub["base"]) | set(sub["c"]), key=lambda k: -sub["base"][k])
out.append("| Subclass | Delta MB/task (base) | New zstd MB/task: base | (a) | (a)+shallow | (c) |")
out.append("|---|---:|---:|---:|---:|---:|")
for k in keys:
    if sub["base"][k] / n < 0.5 and sub["c"][k] / n < 0.5: continue
    out.append(f"| {k} | {sub['base#b'][k]/n/MB:,.0f} | {sub['base'][k]/n/MB:,.1f} | {sub['a'][k]/n/MB:,.1f} | {sub['a_shallow'][k]/n/MB:,.1f} | {sub['c'][k]/n/MB:,.1f} |")
out.append("")
# variants
def grp(r):
    return ("pandas " if r["project"] == "pandas-dev__pandas" else "other ") + ("first" if r["first_of_project"] else "later")
out.append("| Variant | n | All: mean | median | p90 | First of project: mean (median) | Later commit: mean (median) | pandas later: mean | git | installed | build | cache |")
out.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for v in V:
    rs = [r for r in R if r["variant"] == v]
    if v == "a_sharedpack": pass
    a = [r["new_c"] / MB for r in rs]
    f = [r["new_c"] / MB for r in rs if r["first_of_project"]]
    l = [r["new_c"] / MB for r in rs if not r["first_of_project"]]
    pl = [r["new_c"] / MB for r in rs if not r["first_of_project"] and r["project"] == "pandas-dev__pandas"]
    m = lambda x: statistics.mean(cls(r, "new_c")[x] for r in rs) / MB
    out.append(f"| {v} | {len(rs)} | **{statistics.mean(a):,.0f}** | {statistics.median(a):,.0f} | {q(a,.9):,.0f} | {statistics.mean(f):,.0f} ({statistics.median(f):,.0f}) | {statistics.mean(l):,.0f} ({statistics.median(l):,.0f}) | {statistics.mean(pl):,.0f} | {m('git'):,.0f} | {m('installed'):,.0f} | {m('build'):,.0f} | {m('cache'):,.0f} |")
out.append("")
# extrapolation
FIRST, LATER = 10326, 35549 - 10326
out.append("| Variant | First-of-project mean MB | Later-commit mean MB | **OpenSWE TB (means)** | TB at medians | TB, later = pandas-later mean | TB, later = non-pandas later mean |")
out.append("|---|---:|---:|---:|---:|---:|---:|")
for v in V:
    if v == "a_sharedpack": continue
    rs = [r for r in R if r["variant"] == v]
    f = [r["new_c"] for r in rs if r["first_of_project"]]
    l = [r["new_c"] for r in rs if not r["first_of_project"]]
    pl = [r["new_c"] for r in rs if not r["first_of_project"] and r["project"] == "pandas-dev__pandas"]
    ol = [r["new_c"] for r in rs if not r["first_of_project"] and r["project"] != "pandas-dev__pandas"]
    tb = lambda a, b: (FIRST * a + LATER * b) / 1e12
    out.append(f"| {v} | {statistics.mean(f)/MB:,.0f} | {statistics.mean(l)/MB:,.0f} | **{tb(statistics.mean(f), statistics.mean(l)):.1f}** | {tb(statistics.median(f), statistics.median(l)):.1f} | {tb(statistics.mean(f), statistics.mean(pl)):.1f} | {tb(statistics.mean(f), statistics.mean(ol)):.1f} |")
print("\n".join(out))
