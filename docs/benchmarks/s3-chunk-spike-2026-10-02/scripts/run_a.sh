#!/bin/bash
# S12 (a): fetch S10's 181-image sample from our registry, convert per layer (256 KiB, no dict).
set -x
cd /root/s12
python3 fetch.py /root/s12/sample-s10.json > /data/logs/fetch.log 2>&1
python3 convert.py --mode nodict --chunk-size 0x40000 --out /data/work/nodict-256k --workers 40 --seed 13 > /data/logs/convert.log 2>&1
echo DONE $? > /data/logs/convert.done
