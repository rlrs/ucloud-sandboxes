#!/bin/sh
# nydusd spike arms on the canary (w1), every one from a cold node: a warm-up
# that fills the store node, then nydusd and the Python RAFS export at 64 and
# 20 images, then kill -9 of nydusd daemons mid-read, then the fetch vs
# gVisor filesystem split (cold, warm in the sandbox, warm native).
# Usage: STORE_URL=http://<store>:<port> TOKEN_FILE=<read token> nydusd_spike_run.sh
set -u
cd /opt/m1-gate
O=/var/lib/nydusd-spike
mkdir -p $O
W="/usr/bin/python3 nydusd_spike_worker.py"
log() { echo "$(date -u +%FT%TZ) $*" >> $O/progress.log; }
for spec in "python 64 warmup" "nydusd 64 n64" "python 64 p64" "nydusd 20 n20" "python 20 p20"; do
  set -- $spec
  log "start $3"
  $W arm --mode "$1" --images bench-burst.json --n "$2" --run "s$3" --out "$O/$3.json" \
      --store-url "$STORE_URL" --token-file "$TOKEN_FILE" >> $O/progress.log 2>&1
  log "done $3 rc=$?"
done
log "start kill"
$W kill --images bench-burst.json --n 64 --run kill --out $O/kill.json >> $O/progress.log 2>&1
log "done kill rc=$?"
log "start split"
$W split --images bench-burst.json --n 8 --run split --out $O/split.json >> $O/progress.log 2>&1
log "done split rc=$?"
log "all done"
