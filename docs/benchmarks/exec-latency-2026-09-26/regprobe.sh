#!/bin/bash
# Raw registry blob upload timing: POST, PATCH (data), PUT (commit), then GET.
set -eu
R=http://10.42.0.2:5000
f=/tmp/regprobe.bin
head -c 314572800 /dev/urandom > $f
d=sha256:$(sha256sum $f | cut -c1-64)
loc=$(curl -s -D - -o /dev/null -X POST $R/v2/bench/rawprobe/blobs/uploads/ | awk -F': ' 'tolower($1)=="location"{print $2}' | tr -d '\r')
case "$loc" in http*) ;; *) loc="$R$loc";; esac
s=$(date +%s.%N)
loc2=$(curl -s -D - -o /dev/null -X PATCH -H "Content-Type: application/octet-stream" --data-binary @$f "$loc" | awk -F': ' 'tolower($1)=="location"{print $2}' | tr -d '\r')
m=$(date +%s.%N)
case "$loc2" in http*) ;; *) loc2="$R$loc2";; esac
sep='?'; case "$loc2" in *\?*) sep='&';; esac
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$loc2${sep}digest=$d")
e=$(date +%s.%N)
echo "patch_s=$(echo "$m - $s" | bc) commit_s=$(echo "$e - $m" | bc) status=$code"
s=$(date +%s.%N); curl -s -o /dev/null -w "get %{size_download} B at %{speed_download} B/s\n" -L $R/v2/bench/rawprobe/blobs/$d; e=$(date +%s.%N)
rm -f $f
