#!/bin/bash
# S12 (e)(iii): out-of-tree erofs.ko with CONFIG_EROFS_FS_PAGE_CACHE_SHARE=y against the running
# kernel's headers tree (S11 recipe; Ubuntu's .config, vermagic and Module.symvers unchanged).
set -euxo pipefail
KREL=7.0.0-30-generic
S=/data/k/linux-source-7.0.0; H=/usr/src/linux-headers-$KREL
grep -n "ishare\|PAGE_CACHE_SHARE" $S/fs/erofs/Makefile
make -C $H M=$S/fs/erofs CC=x86_64-linux-gnu-gcc CONFIG_EROFS_FS=m CONFIG_EROFS_FS_PAGE_CACHE_SHARE=y \
     KCFLAGS=-DCONFIG_EROFS_FS_PAGE_CACHE_SHARE=1 -j"$(nproc)" modules 2>&1 | tail -5
modinfo -F vermagic $S/fs/erofs/erofs.ko
nm $S/fs/erofs/erofs.ko | grep -c ishare
sha256sum $S/fs/erofs/erofs.ko
echo BUILD_DONE
