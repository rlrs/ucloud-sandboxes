#!/usr/bin/env python3
"""Check the walker's chunking against nydus-image v2.4.5 on one exported tree.

nydus-image create -t dir-rafs --fs-version 6 --chunk-size 0x40000 --compressor zstd --digester sha256
over the unpacked `docker export` of an image, then compare the bootstrap's chunk table (rafs.py) with
the walker's --full output for the same export: digest sets, uncompressed and compressed byte sums.
"""
import gzip, json, subprocess, sys
sys.path.insert(0, "/data/w/scripts")
import rafs
tree, walk_out, boot = sys.argv[1], sys.argv[2], sys.argv[3]
info = rafs.read(boot)
nyd = {}
for d, bi, fl, cs, us, co, uo in info["chunks"]:
    nyd.setdefault(d[:32], (cs, us))
mine = {}
with gzip.open(walk_out, "rt") as f:
    for line in f:
        r = json.loads(line)
        if isinstance(r, list):
            for d, u, c in r[3]:
                mine.setdefault(d, (c, u))
both = set(nyd) & set(mine)
res = {"nydus_chunks": len(nyd), "walker_chunks": len(mine), "common": len(both),
       "only_nydus": len(set(nyd) - set(mine)), "only_walker": len(set(mine) - set(nyd)),
       "nydus_u": sum(v[1] for v in nyd.values()), "walker_u": sum(v[1] for v in mine.values()),
       "nydus_c": sum(v[0] for v in nyd.values()), "walker_c_zstd3": sum(v[0] for v in mine.values()),
       "common_nydus_c": sum(nyd[d][0] for d in both), "common_walker_c": sum(mine[d][0] for d in both)}
print(json.dumps(res))
