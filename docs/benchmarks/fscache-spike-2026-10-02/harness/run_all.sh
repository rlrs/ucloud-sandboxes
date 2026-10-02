#!/bin/bash
cd /root/s11
P=/root/s11/venv/bin/python
for step in "fscache seq" "nbd seq" "fscache par" "nbd par"; do
  set -- $step
  echo "=== $step $(date -u +%T)"
  $P -u bench.py $1 $2 || echo "FAILED $step"
  [ "$2" = par ] && cp out/bench-$1-par.json out/bench-$1-par-r1.json
done
for r in 2; do
  for path in fscache nbd; do
    echo "=== $path par repeat $r $(date -u +%T)"
    $P -u bench.py $path par && cp out/bench-$path-par.json out/bench-$path-par-r$r.json
  done
done
echo "=== verify $(date -u +%T)"
$P -u verify_fscache.py contents dict validate
echo "ALL_DONE $(date -u +%T)"
