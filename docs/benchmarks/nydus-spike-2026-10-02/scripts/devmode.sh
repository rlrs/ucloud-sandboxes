#!/bin/bash
# Multi-device (-o device=) mount, file-backed mount and export --block checks for one image.
set -u
idx=$1; run=$2
boot=$run/images/$(printf %03d $idx).boot
python3 - "$boot" > /tmp/ndev <<'PY'
import sys; sys.path.insert(0, "/root"); import rafs
print(len(rafs.read(sys.argv[1])["devices"]))
PY
n=$(cat /tmp/ndev); echo "devices: $n"
pids=(); devs=()
start() { python3 /root/spike_nbd.py $1 part $boot $run/blobs $2 > /tmp/nbd.$2 & pids+=($!); until grep -q READY /tmp/nbd.$2; do sleep 0.05; done; }
start /dev/nbd40 -1
opts="ro"
for ((i=0;i<n;i++)); do start /dev/nbd$((41+i)) $i; opts="$opts,device=/dev/nbd$((41+i))"; done
mkdir -p /mnt/devmode
t=$(date +%s.%N); mount -t erofs -o $opts /dev/nbd40 /mnt/devmode && echo "multi-device mount OK in $(echo "$(date +%s.%N) - $t" | bc) s"; 
ls /mnt/devmode | head -5; find /mnt/devmode -type f 2>/dev/null | head -200 | xargs -d '\n' cat > /dev/null 2>&1 && echo "read 200 files OK"
umount /mnt/devmode; kill ${pids[@]}; wait
# Without device= the kernel uses the unified (flat) address space via mapped_blkaddr; covered by spike_nbd nydus mode.
# nydus-image export --block: one raw disk image with bootstrap + uncompressed blobs.
mkdir -p /data/export; rm -f /data/export/$idx.img
t=$(date +%s.%N)
nydus-image export --block --bootstrap $boot --localfs-dir $run/blobs --output /data/export/$idx.img 2>&1 | tail -2
echo "export --block took $(echo "$(date +%s.%N) - $t" | bc) s, size $(stat -c %s /data/export/$idx.img)"
mkdir -p /mnt/export; mount -t erofs -o ro,loop /data/export/$idx.img /mnt/export && echo "export --block loop mount OK"; ls /mnt/export | head -3; umount /mnt/export
# File-backed mount (CONFIG_EROFS_FS_BACKED_BY_FILE) of the same image without a loop device.
mount -t erofs -o ro /data/export/$idx.img /mnt/export && echo "file-backed mount OK"; umount /mnt/export
rm -f /data/export/$idx.img
