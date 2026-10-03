#!/bin/sh
# Today's path (0.8.4, EROFS components from the production registry) on the
# baseline worker (b1), same images and bench, 64 then 20.
set -u
cd /opt/m1-gate
O=/var/lib/nydusd-spike
mkdir -p $O
log() { echo "$(date -u +%FT%TZ) $*" >> $O/progress.log; }
for n in 64 20; do
  log "start base$n"
  /usr/bin/python3 chunk_store_gate_remote.py bench --kind burst --images bench-burst-base.json --run "sbase$n" \
      --n "$n" --out "$O/base$n.json" >> $O/progress.log 2>&1
  log "done base$n rc=$?"
done
log "all done"
