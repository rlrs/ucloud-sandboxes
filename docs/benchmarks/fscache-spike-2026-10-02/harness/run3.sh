#!/bin/bash
# Re-run with cache usage measured as du -x (no mounts crossed) and filesystem-used delta.
cd /root/s11
P=/root/s11/venv/bin/python
mkdir -p out/run1-cache-usage-invalid
cp out/bench-*-seq.json out/bench-*-par*.json out/run1-cache-usage-invalid/
for step in "fscache seq" "nbd seq" "fscache par" "nbd par"; do
  set -- $step
  echo "=== $step $(date -u +%T)"
  $P -u bench.py $1 $2 || echo "FAILED $step"
  [ "$2" = par ] && cp out/bench-$1-par.json out/bench-$1-par-r4.json
done
for path in fscache nbd; do echo "=== $path par x3 $(date -u +%T)"; $P -u bench.py $path par 3; done
echo "RUN3_DONE $(date -u +%T)"
