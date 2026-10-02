#!/bin/bash
# S13: multi-device bursts again after the shared-blob fix (fand.py merges every attached image's
# entries into a shared blob file; the first runs served zeros for chunks only a later image used).
cd /root/s13
for n in 20 100; do
  for p in fan:multidev fan:multidev+f; do python3 coldfan.py burst --n $n --path $p --tag "$n-${p/:/-}-fix"; done
done > /data/logs/bursts-md-fix.log 2>&1
echo REST2_DONE >> /data/logs/bursts-md-fix.log
