#!/usr/bin/env python3
"""Collect verify/*.txt and eval/*.txt key=value outputs into one JSON."""
import glob, json, os, sys
W = sys.argv[1]
def kv(p):
    d = {}
    for l in open(p, errors="replace"):
        if "=" in l:
            k, v = l.rstrip("\n").split("=", 1); d.setdefault(k, v)
    return d
out = {}
for p in sorted(glob.glob(f"{W}/verify/*/*.txt")):
    t, v = p.split("/")[-2], os.path.basename(p)[:-4]
    d = kv(p)
    out.setdefault(t, {}).setdefault(v, {})["verify"] = {k: d.get(k) for k in (
        "git_status_rc", "git_status_ms", "git_status_lines", "git_status_sha", "head_is_base", "base_commit_present",
        "commits", "describe", "git_diff_rc", "fsck_rc", "version", "test_patch_check_rc", "collect_rc", "collected")}
for p in sorted(glob.glob(f"{W}/eval/*/*.txt")):
    t, name = p.split("/")[-2], os.path.basename(p)[:-4]
    v, mode = (name[:-5], "gold") if name.endswith(".gold") else (name, "unpatched")
    d = kv(p)
    out.setdefault(t, {}).setdefault(v, {})["eval_" + mode] = {k: d.get(k) for k in ("OPENSWE_EXIT_CODE", "gold_apply", "run_s")}
json.dump(out, open(sys.argv[2], "w"), indent=1)
print(len(out))
