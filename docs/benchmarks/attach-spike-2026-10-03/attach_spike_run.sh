#!/bin/sh
# Attach-cost spike: 20 and 64 distinct cold images, attach_concurrency 1 then 8.
set -u
cd /opt/attach-spike
D=/etc/systemd/system/ucloud-environment-io.service.d
BASE=$(systemctl show -p ExecStart --value ucloud-environment-io.service | sed -n 's/.*argv\[\]=\([^;]*\);.*/\1/p')
for spec in "1 20" "1 64" "8 20" "8 64"; do
  set -- $spec; ac=$1; n=$2; name="a${ac}-${n}"
  if [ "$ac" = 1 ]; then rm -f $D/attach-concurrency.conf
  else printf '[Service]\nExecStart=\nExecStart=%s --attach-concurrency %s\n' "$BASE" "$ac" > $D/attach-concurrency.conf; fi
  systemctl daemon-reload
  rm -f /var/lib/attach-spike/timing.jsonl
  echo "$(date -u +%FT%TZ) start $name" >> /var/lib/attach-spike/progress.log
  /usr/bin/python3 chunk_store_gate_remote.py bench --kind burst --images bench-$n.json --run "spike$name" --n "$n" \
      --out /var/lib/attach-spike/$name.json >> /var/lib/attach-spike/progress.log 2>&1
  echo "$(date -u +%FT%TZ) done $name rc=$?" >> /var/lib/attach-spike/progress.log
  mv /var/lib/attach-spike/timing.jsonl /var/lib/attach-spike/timing-$name.jsonl 2>/dev/null
done
rm -f $D/attach-concurrency.conf; systemctl daemon-reload
echo "$(date -u +%FT%TZ) all done" >> /var/lib/attach-spike/progress.log
