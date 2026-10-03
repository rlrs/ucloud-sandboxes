#!/bin/sh
# Shared nydusd cache (run 2) on the canary, every arm from a cold node: a
# warm-up that fills the store node, then per-image caches with attach 8 (run
# 1's best, as the same-run reference), then one shared cache with serial
# attach and attach 8 at 64 images and serial at 20, then kill -9 mid-read
# with the shared cache.
# Usage: STORE_URL=http://<store>:<port> TOKEN_FILE=<read token> shared_cache_run.sh
set -u
cd /opt/m1-gate
O=/var/lib/nydusd-spike
mkdir -p $O
W="/usr/bin/python3 nydusd_spike_worker.py"
log() { echo "$(date -u +%FT%TZ) $*" >> $O/progress.log; }
for spec in "python 1 64 warmup" "nydusd 8 64 n64a8" "nydusd-shared 1 64 s64" "nydusd-shared 8 64 s64a8" \
            "nydusd-shared 1 20 s20"; do
  set -- $spec
  log "start $4"
  $W arm --mode "$1" --attach "$2" --images bench-burst.json --n "$3" --run "s$4" --out "$O/$4.json" \
      --store-url "$STORE_URL" --token-file "$TOKEN_FILE" >> $O/progress.log 2>&1
  log "done $4 rc=$?"
done
log "start kill-shared"
$W kill --mode nydusd-shared --attach 8 --images bench-burst.json --n 64 --run kills --out $O/kill-shared.json \
    >> $O/progress.log 2>&1
log "done kill-shared rc=$?"
log "all done"
