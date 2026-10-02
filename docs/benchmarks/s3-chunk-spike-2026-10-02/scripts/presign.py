#!/usr/bin/env python3
"""Presign 24 h GET URLs for every spike pack and image meta object (what the locator would carry).
Writes a root-only /root/s12/presigned.json: {"host", "urls": [{url, bytes}], "by_sha": {sha|meta key: url}}.
The URLs are bearer tokens: the file is deleted with the VM."""
import json
import os
import sys

sys.path.insert(0, "/root/s12")
import s3lib  # noqa: E402

c = json.load(open("/root/s12/s3.json"))
s = s3lib.S3(c["endpoint"], c["bucket"], c["region"], "/root/s12/.s3env")
packs = json.load(open("/data/s12/packs.json"))
by = {}
urls = []
for p in packs:
    u = s.presign(f"spike-s12/packs/{p['sha'][:2]}/{p['sha']}.pack")
    by[p["sha"]] = u
    urls.append({"url": u, "bytes": p["bytes"]})
for key, size in s.list("spike-s12/meta/"):
    by["meta/" + key.split("/", 2)[2]] = s.presign(key)
os.umask(0o077)
json.dump({"host": s.host, "urls": urls, "by_sha": by}, open("/root/s12/presigned.json", "w"))
print("packs", len(urls), "meta", len(by) - len(urls))
