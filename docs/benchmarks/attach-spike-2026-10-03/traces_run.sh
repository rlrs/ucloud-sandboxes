#!/bin/sh
# Shared startup traces: round 1 records and shares, round 2 starts each mode
# with no local traces or cache (a fresh node), and finds the shared ones.
set -u
cd /opt/attach-spike
D=/etc/systemd/system/ucloud-environment-io.service.d
BASE=$(systemctl show -p ExecStart --value ucloud-environment-io.service | sed -n 's/.*argv\[\]=\([^;]*\);.*/\1/p')
printf '[Service]\nExecStart=\nExecStart=%s --shared-traces\n' "$BASE" > $D/shared-traces.conf
systemctl daemon-reload
for round in r1 r2; do
  name="shared-$round-64"
  rm -f /var/lib/attach-spike/timing.jsonl
  echo "$(date -u +%FT%TZ) start $name" >> /var/lib/attach-spike/progress.log
  /usr/bin/python3 chunk_store_gate_remote.py bench --kind burst --images bench-64.json --run "spike$name" --n 64 \
      --out /var/lib/attach-spike/$name.json >> /var/lib/attach-spike/progress.log 2>&1
  echo "$(date -u +%FT%TZ) done $name rc=$?" >> /var/lib/attach-spike/progress.log
  mv /var/lib/attach-spike/timing.jsonl /var/lib/attach-spike/timing-$name.jsonl 2>/dev/null
  sleep 20  # Let background pushes of this round's traces finish.
done
journalctl -u ucloud-environment-io --no-pager -o cat | grep -c "was not shared" >> /var/lib/attach-spike/progress.log
echo "$(date -u +%FT%TZ) traces done" >> /var/lib/attach-spike/progress.log
