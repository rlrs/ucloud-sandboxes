#!/bin/bash
# End-to-end nydusify convert (registry -> VM-local registry) for a few sample images.
mkdir -p /data/nydusify-work /data/results
for idx in "$@"; do
  ref=$(python3 -c "import json;s=json.load(open('/data/manifests/sample-manifests.json'))[$idx];print(s['prepared_reference'])")
  tgt="127.0.0.1:5001/nydusify/img$idx:v6"
  /usr/bin/time -f "%e %U %S" -o /tmp/nydusify.time nydusify convert --source "$ref" --source-insecure --target "$tgt" --target-insecure \
     --fs-version 6 --compressor zstd --work-dir /data/nydusify-work/$idx --nydus-image /usr/local/bin/nydus-image > /data/logs/nydusify-$idx.log 2>&1
  rc=$?
  read wall u s < /tmp/nydusify.time
  man=$(curl -s -H "Accept: application/vnd.oci.image.manifest.v1+json" http://127.0.0.1:5001/v2/nydusify/img$idx/manifests/v6)
  echo "{\"idx\": $idx, \"rc\": $rc, \"wall\": $wall, \"cpu\": $(echo "$u + $s" | bc), \"manifest\": $man}" | tee -a /data/results/nydusify.jsonl | cut -c1-400
  tail -3 /data/logs/nydusify-$idx.log
done
