#!/bin/bash
cd /root/s11
P=/root/s11/venv/bin/python
echo "=== eagain $(date -u +%T)"; $P -u repro_eagain.py
echo "=== sharing $(date -u +%T)"; $P -u verify_fscache.py sharing
for r in 1 2 3; do
  for path in fscache nbd; do
    echo "=== $path par r$r $(date -u +%T)"
    $P -u bench.py $path par && cp out/bench-$path-par.json out/bench-$path-par-r$r.json
  done
done
for path in fscache nbd; do
  echo "=== $path par x3 $(date -u +%T)"; $P -u bench.py $path par 3
done
echo "=== failover $(date -u +%T)"
$P -u verify_fscache.py failover
echo "RUN2_DONE $(date -u +%T)"
