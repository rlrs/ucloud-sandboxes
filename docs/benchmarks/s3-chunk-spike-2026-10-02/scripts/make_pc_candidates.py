#!/usr/bin/env python3
"""Write pc-candidates.json for select_pc.py: every distinct TMax / Terminal-Lego / OpenSWE foundation
prepared reference in all-cached-training-tasks-with-terminal-lego-2026-10-01.zip (3,546 in S12)."""
import json
import sys
import zipfile

z = zipfile.ZipFile(sys.argv[1])
images = json.load(z.open("all-cached-training-tasks/all-image-selectors.json"))["images"]
seen = {}
for x in images:
    if x["family"] in ("TMax", "Terminal-Lego", "OpenSWE") and x.get("foundation_key") and x["cached_kind"] == "foundation":
        seen.setdefault(x["prepared_reference"], {
            "prepared_reference": x["prepared_reference"], "family": x["family"], "foundation_key": x["foundation_key"],
            "image": x["image"], "erofs_bytes": x.get("erofs_bytes"), "group": "pagecache", "cached_kind": "foundation"})
json.dump(list(seen.values()), open(sys.argv[2] if len(sys.argv) > 2 else "pc-candidates.json", "w"))
print(len(seen))
