#!/bin/bash
# S13 gate 5: out-of-tree erofs.ko with CONFIG_EROFS_FS_PAGE_CACHE_SHARE=y against the running kernel's
# headers (S11/S12 recipe; Ubuntu's .config, vermagic and Module.symvers unchanged), then swap it in
# (no EROFS mounts may exist).
set -euxo pipefail
KREL=7.0.0-30-generic
S=/data/k/linux-source-7.0.0; H=/usr/src/linux-headers-$KREL
make -C $H M=$S/fs/erofs CC=x86_64-linux-gnu-gcc CONFIG_EROFS_FS=m CONFIG_EROFS_FS_PAGE_CACHE_SHARE=y \
     KCFLAGS=-DCONFIG_EROFS_FS_PAGE_CACHE_SHARE=1 -j"$(nproc)" modules 2>&1 | tail -n 3
modinfo -F vermagic $S/fs/erofs/erofs.ko
nm $S/fs/erofs/erofs.ko | grep -c ishare
sha256sum $S/fs/erofs/erofs.ko
if [ "${1:-}" = "load" ]; then
  ! grep -q " erofs " /proc/mounts
  modprobe -r erofs
  mkdir -p /lib/modules/$KREL/updates
  cp $S/fs/erofs/erofs.ko /lib/modules/$KREL/updates/erofs.ko
  depmod -a
  modprobe erofs
  modinfo -F filename erofs
  grep -c ishare /proc/kallsyms
  dmesg | tail -n 3
fi
echo BUILD_DONE
