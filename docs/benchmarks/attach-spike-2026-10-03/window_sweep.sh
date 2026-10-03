#!/bin/sh
# Spike only: demand window size sweep (patches the installed DEMAND_WINDOW_CHUNKS).
set -u
SP=$(ls -d /var/cache/ucloud-sandboxes/init-packages/*/agent-runtime/site-packages | head -1)
F=$SP/ucloud_sandboxes/environment_cache.py
cd /opt/attach-spike
D=/etc/systemd/system/ucloud-environment-io.service.d
BASE=$(systemctl show -p ExecStart --value ucloud-environment-io.service | sed -n 's/.*argv\[\]=\([^;]*\);.*/\1/p')
for spec in "1 2" "1 4" "8 4"; do
  set -- $spec; ac=$1; w=$2; name="a${ac}-w${w}-64"
  sed -i "s/^DEMAND_WINDOW_CHUNKS = [0-9]*/DEMAND_WINDOW_CHUNKS = $w/" $F
  rm -rf $SP/ucloud_sandboxes/__pycache__/environment_cache*
  grep "^DEMAND_WINDOW_CHUNKS" $F >> /var/lib/attach-spike/progress.log
  if [ "$ac" = 1 ]; then rm -f $D/attach-concurrency.conf
  else printf '[Service]\nExecStart=\nExecStart=%s --attach-concurrency %s\n' "$BASE" "$ac" > $D/attach-concurrency.conf; fi
  systemctl daemon-reload
  rm -f /var/lib/attach-spike/timing.jsonl
  echo "$(date -u +%FT%TZ) start $name" >> /var/lib/attach-spike/progress.log
  /usr/bin/python3 chunk_store_gate_remote.py bench --kind burst --images bench-64.json --run "spike$name" --n 64 \
      --out /var/lib/attach-spike/$name.json >> /var/lib/attach-spike/progress.log 2>&1
  echo "$(date -u +%FT%TZ) done $name rc=$?" >> /var/lib/attach-spike/progress.log
  mv /var/lib/attach-spike/timing.jsonl /var/lib/attach-spike/timing-$name.jsonl 2>/dev/null
done
sed -i "s/^DEMAND_WINDOW_CHUNKS = [0-9]*/DEMAND_WINDOW_CHUNKS = 16/" $F
rm -f $D/attach-concurrency.conf; systemctl daemon-reload
echo "$(date -u +%FT%TZ) sweep done" >> /var/lib/attach-spike/progress.log
