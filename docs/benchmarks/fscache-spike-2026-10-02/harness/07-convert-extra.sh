#!/bin/bash
# Pass 2: ScaleSWE responses-1..3 and oauthlib-1 against a chunk dictionary of a sibling image;
# tlego-3 uncompressed (for the digest-validation test).
set -uo pipefail
W=/root/s11; cd $W
one() { # name src target extra...
  name=$1; src=$2; target=$3; shift 3
  start=$(date +%s.%N)
  nydusify convert --source "$src" --source-insecure --target "$target" --target-insecure --plain-http \
    --fs-version 6 --fs-chunk-size 0x100000 --work-dir "$W/convert/work-x-$name" --output-json "$W/convert/$name.metrics.json" "$@" > "$W/convert/$name.log" 2>&1
  rc=$?
  echo "{\"name\":\"$name\",\"rc\":$rc,\"seconds\":$(echo "$(date +%s.%N) - $start" | bc),\"args\":\"$*\"}" >> $W/convert/results-extra.jsonl
  rm -rf "$W/convert/work-x-$name"
}
src() { r=$(jq -r ".images[] | select(.name==\"$1\") | .prepared_reference" images.json); echo "${r%%:latest@*}@${r##*@}"; }
rm -f convert/results-extra.jsonl
for n in scaleswe-responses-1 scaleswe-responses-2 scaleswe-responses-3; do
  one $n-dict "$(src $n)" 127.0.0.1:5001/s11/$n-dict:nydus --compressor zstd \
    --chunk-dict bootstrap:registry:127.0.0.1:5001/s11/scaleswe-responses-0:nydus --chunk-dict-insecure &
done
one scaleswe-oauthlib-1-dict "$(src scaleswe-oauthlib-1)" 127.0.0.1:5001/s11/scaleswe-oauthlib-1-dict:nydus --compressor zstd \
    --chunk-dict bootstrap:registry:127.0.0.1:5001/s11/scaleswe-oauthlib-0:nydus --chunk-dict-insecure &
one tlego-3-raw "$(src tlego-3)" 127.0.0.1:5001/s11/tlego-3-raw:nydus --compressor none &
wait
cat convert/results-extra.jsonl
echo EXTRA_DONE
