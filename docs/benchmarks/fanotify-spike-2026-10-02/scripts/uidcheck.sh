#!/bin/bash
# S13 side check: does nydus-image v2.4.5 (targz-rafs, fs-version 6) keep file owners? With and
# without --repeatable, a tiny layer with files owned by 1000:42 and 0:101.
set -u
D=/data/s13/uid; rm -rf $D; mkdir -p $D/src/home/u $D/out
echo hello > $D/src/home/u/f; echo x > $D/src/g
chown -R 1000:42 $D/src/home; chown 0:101 $D/src/g
tar -C $D/src --numeric-owner -czf $D/layer.tgz .
tar -tvzf $D/layer.tgz
for r in "" "--repeatable"; do
  tag=${r:-plain}; tag=${tag#--}
  mkdir -p $D/out/$tag/blobs
  nydus-image create -t targz-rafs --fs-version 6 --digester sha256 --compressor zstd --chunk-size 0x40000 $r \
    -D $D/out/$tag/blobs -B $D/out/$tag/boot $D/layer.tgz > /dev/null 2>&1
  python3 /root/s13/flatten.py $D/out/$tag/boot $D/out/$tag/blobs $D/out/$tag/full.img > /dev/null
  mkdir -p /mnt/s13uid; mount -t erofs -o ro $D/out/$tag/full.img /mnt/s13uid
  echo "== $tag"; stat -c '%n %u:%g %a' /mnt/s13uid/home/u /mnt/s13uid/home/u/f /mnt/s13uid/g
  umount /mnt/s13uid
done
# The same tree through mkfs.erofs 1.9 --tar (for comparison)
mkfs.erofs --quiet --tar=f --ungzip $D/out/mkfs.img $D/layer.tgz && mount -t erofs -o ro $D/out/mkfs.img /mnt/s13uid && \
  { echo "== mkfs.erofs --tar"; stat -c '%n %u:%g %a' /mnt/s13uid/home/u /mnt/s13uid/home/u/f /mnt/s13uid/g; umount /mnt/s13uid; }
