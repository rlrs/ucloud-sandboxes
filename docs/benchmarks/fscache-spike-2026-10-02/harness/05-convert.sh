#!/bin/bash
# Convert each selected prepared image with nydusify v2.4.5 (RAFS v6, zstd, 1 MiB chunks).
set -uo pipefail
W=/root/s11; mkdir -p $W/convert
cd $W
jq -c '.images[]' images.json > convert/list.jsonl
convert_one() {
  line="$1"; name=$(jq -r .name <<<"$line"); ref=$(jq -r .prepared_reference <<<"$line")
  src="${ref%%:latest@*}@${ref##*@}"
  dict="$2"
  start=$(date +%s.%N)
  extra=()
  [ -n "$dict" ] && extra=(--chunk-dict "bootstrap:registry:$dict" --chunk-dict-insecure)
  nydusify convert --source "$src" --source-insecure --target "127.0.0.1:5001/s11/$name:nydus" --target-insecure --plain-http \
    --fs-version 6 --compressor zstd --fs-chunk-size 0x100000 --work-dir "$W/convert/work-$name" \
    --output-json "$W/convert/$name.metrics.json" "${extra[@]}" > "$W/convert/$name.log" 2>&1
  rc=$?
  end=$(date +%s.%N)
  echo "{\"name\":\"$name\",\"rc\":$rc,\"seconds\":$(echo "$end - $start" | bc),\"chunk_dict\":\"$dict\"}" >> $W/convert/results.jsonl
  rm -rf "$W/convert/work-$name"
}
export -f convert_one; export W
rm -f convert/results.jsonl
# Pass 1: every image on its own (layer-wise blobs, no dictionary).
cat convert/list.jsonl | xargs -d '\n' -P 4 -I{} bash -c 'convert_one "$1" ""' _ {}
echo PASS1_DONE
