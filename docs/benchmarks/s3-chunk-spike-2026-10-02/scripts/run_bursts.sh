#!/bin/bash
# S12 (c) bursts: 20 and 100 distinct images on one cold backend. readaround runs record each image's
# trace; trace runs replay them. local:demand is the loopback baseline.
cd /root/s12
for n in 20 100; do
  python3 coldrun.py burst --n $n --mode local:demand --tag local-$n
  python3 coldrun.py burst --n $n --mode s3:readaround --tag s3-readaround-$n
  python3 coldrun.py burst --n $n --mode s3:trace --trace --tag s3-trace-$n
done > /data/logs/bursts.log 2>&1
echo BURSTS_DONE >> /data/logs/bursts.log
