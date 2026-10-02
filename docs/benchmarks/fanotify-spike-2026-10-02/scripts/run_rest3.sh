#!/bin/bash
# S13: rerun of run_rest.sh's later steps after fixing fand.py's Backing (a property without a setter
# made every attach fail): gate 4, the owner-aware tree check, the shared-blob burst reruns, gate 5.
# The inode_share erofs.ko from build_erofs_ishare.sh is already loaded (and serves every later mount).
cd /root/s13
rm -f /data/results/burst-*-fix.json /data/results/gate4-copy.json /data/results/gate4-reflink.json \
      /data/results/gate4-multidev.json /data/results/gate4-hashes-copy.json
for m in copy reflink multidev; do python3 gate4.py run --mode $m --n 24; done > /data/logs/gate4.log 2>&1
for n in 20 100; do
  for p in fan:multidev fan:multidev+f; do python3 coldfan.py burst --n $n --path $p --tag "$n-${p/:/-}-fix"; done
done > /data/logs/bursts-md-fix.log 2>&1
python3 pcfan.py prep > /data/logs/pc-prep.log 2>&1
for v in image image-dio layer layer-dio multidev erofs ishare erofs-dio ishare-dio; do
  python3 pcfan.py run --variant $v
done > /data/logs/pagecache.log 2>&1
python3 coldfan.py tree --paths fan:demand --tag owners > /data/logs/tree-owners.log 2>&1
echo REST3_DONE >> /data/logs/pagecache.log
