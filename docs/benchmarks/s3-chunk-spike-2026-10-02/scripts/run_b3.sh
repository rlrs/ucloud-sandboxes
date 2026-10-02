#!/bin/bash
# S12 (b), 3 clients: the two cpx42 client VMs were refused (shared-core quota), so three independent
# s3bench processes (separate processes, connections and random streams) run in lockstep on the CCX63.
cd /root/s12
T=$(( $(date +%s) + 5 ))
for c in a b c; do
  python3 s3bench.py --urls /root/s12/presigned.json --out /data/results/s3bench-3client-$c.json --client $c \
    --seconds 10 --start-at $T > /data/logs/s3bench-3-$c.log 2>&1 &
done
wait
python3 - <<'PY'
import json
cells = {}
for c in "abc":
    for x in json.load(open(f"/data/results/s3bench-3client-{c}.json"))["cells"]:
        cells.setdefault((x["size"], x["conc"]), []).append(x)
json.dump([v for v in cells.values()], open("/data/results/s3bench-3client-raw.json", "w"))
PY
echo B3_DONE
