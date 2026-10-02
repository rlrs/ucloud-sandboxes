#!/bin/bash
# Walker vs nydus-image v2.4.5 on the full tree of one image (dir-rafs, 256 KiB chunks, zstd, sha256).
set -eu
img=$1; n=$2; d=/data/w/calib/$n; rm -rf $d; mkdir -p $d/tree $d/blobs
docker rm -f calib-$n >/dev/null 2>&1 || true
docker create --name calib-$n $img true >/dev/null
docker export -o $d/img.tar calib-$n; docker rm -f calib-$n >/dev/null
tar -C $d/tree -xf $d/img.tar 2>/dev/null || true
python3 /data/w/scripts/walk.py --full --out $d/walk.jsonl.gz < $d/img.tar >/dev/null; rm -f $d/img.tar
/usr/bin/time -f "nydus_wall=%e nydus_cpu=%U+%S" nydus-image create -t dir-rafs --fs-version 6 --chunk-size 0x40000 --compressor zstd --digester sha256 -D $d/blobs -B $d/boot $d/tree >/dev/null 2>$d/nydus.err || { tail -5 $d/nydus.err; exit 1; }
tail -1 $d/nydus.err
python3 /data/w/scripts/calib.py $d/tree $d/walk.jsonl.gz $d/boot
du -sb $d/blobs | cut -f1 | sed 's/^/blob_bytes=/'
rm -rf $d/tree
