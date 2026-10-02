#!/bin/bash
# S13 gate 3: full-tree check, sequential cold commands, then 20- and 100-way cold bursts.
cd /root/s13
python3 coldfan.py tree > /data/logs/tree.log 2>&1
python3 coldfan.py seq --paths nbd:demand,fan:demand,nbd:readaround,fan:window --reps 2 --tag main > /data/logs/seq.log 2>&1
for n in 20 100; do
  for p in nbd:demand fan:demand nbd:readaround fan:window; do
    python3 coldfan.py burst --n $n --path $p --tag "$n-${p/:/-}"
  done
done > /data/logs/bursts.log 2>&1
for p in nbd:demand fan:demand; do python3 coldfan.py burst --n 20 --path $p --tag "20-${p/:/-}-r2"; done >> /data/logs/bursts.log 2>&1
echo GATE3_DONE >> /data/logs/bursts.log
