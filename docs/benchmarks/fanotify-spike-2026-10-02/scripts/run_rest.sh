#!/bin/bash
# S13 after run_gate3.sh: the multi-device path through gate 3, gate 4 (disk sharing), the owner-aware
# full-tree rerun, then gate 5 (page cache) with the inode_share erofs.ko.
cd /root/s13
python3 mdsmoke.py > /data/logs/mdsmoke.log 2>&1
python3 coldfan.py seq --paths fan:multidev+f,fan:demand+f --reps 2 --tag fast > /data/logs/seq-md.log 2>&1
for n in 20 100; do
  for p in fan:multidev fan:multidev+f fan:demand+f; do python3 coldfan.py burst --n $n --path $p --tag "$n-${p/:/-}"; done
done > /data/logs/bursts-md.log 2>&1
for m in copy reflink multidev; do python3 gate4.py run --mode $m --n 24; done > /data/logs/gate4.log 2>&1
python3 coldfan.py tree --paths fan:demand --tag owners > /data/logs/tree-owners.log 2>&1
python3 nbddeath.py > /data/logs/nbd-death.log 2>&1
echo REST_A_DONE >> /data/logs/gate4.log
bash build_erofs_ishare.sh load > /data/logs/build-ishare.log 2>&1
python3 pcfan.py prep > /data/logs/pc-prep.log 2>&1
for v in image image-dio layer layer-dio multidev erofs ishare erofs-dio ishare-dio; do
  python3 pcfan.py run --variant $v
done > /data/logs/pagecache.log 2>&1
echo REST_DONE >> /data/logs/pagecache.log
