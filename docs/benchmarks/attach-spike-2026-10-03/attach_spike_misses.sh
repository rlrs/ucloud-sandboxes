#!/bin/sh
# Spike only: is the fetch rate capped by the cache's 8 concurrent misses?
set -u
SP=$(ls -d /var/cache/ucloud-sandboxes/init-packages/*/agent-runtime/site-packages | head -1)
F=$SP/ucloud_sandboxes/environment_backend.py
grep -q UCLOUD_ENVIRONMENT_CONCURRENT_MISSES $F || sed -i 's/    rafs, cache_options = None, None/    rafs, cache_options = None, ({"concurrent_misses": int(os.environ["UCLOUD_ENVIRONMENT_CONCURRENT_MISSES"])} if os.environ.get("UCLOUD_ENVIRONMENT_CONCURRENT_MISSES") else None)/' $F
grep -c UCLOUD_ENVIRONMENT_CONCURRENT_MISSES $F >> /var/lib/attach-spike/progress.log
rm -rf $SP/ucloud_sandboxes/__pycache__/environment_backend*
cd /opt/attach-spike
D=/etc/systemd/system/ucloud-environment-io.service.d
BASE=$(systemctl show -p ExecStart --value ucloud-environment-io.service | sed -n 's/.*argv\[\]=\([^;]*\);.*/\1/p')
for spec in "1 32 64" "1 64 64" "8 64 64"; do
  set -- $spec; ac=$1; misses=$2; n=$3; name="a${ac}-m${misses}-${n}"
  printf '[Service]\nEnvironment=UCLOUD_ENVIRONMENT_CONCURRENT_MISSES=%s\n' "$misses" > $D/misses.conf
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
rm -f $D/attach-concurrency.conf $D/misses.conf; systemctl daemon-reload
echo "$(date -u +%FT%TZ) misses done" >> /var/lib/attach-spike/progress.log
