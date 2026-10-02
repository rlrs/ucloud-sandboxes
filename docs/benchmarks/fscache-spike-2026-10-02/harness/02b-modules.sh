#!/bin/bash
# S11 step 1 (as shipped): out-of-tree M= build of fs/erofs and fs/cachefiles from Ubuntu's
# 7.0.0-30.30 source against the 7.0.0-30-generic headers tree, so every other config value,
# vermagic and Module.symvers come from the running kernel's build, unchanged.
set -euxo pipefail
KREL=7.0.0-30-generic
H=/usr/src/linux-headers-$KREL
S=/root/s11/kbuild/linux-source-7.0.0
OUT=/root/s11/out/modules; mkdir -p $OUT
grep -n -B1 -A8 'config EROFS_FS_PAGE_CACHE_SHARE' $S/fs/erofs/Kconfig | tee $OUT/page-cache-share-kconfig.txt
grep -hn '#include "\.\.' $S/fs/erofs/*.[ch] $S/fs/cachefiles/*.[ch] | tee $OUT/cross-dir-includes.txt || true
cat $H/include/config/kernel.release | tee $OUT/headers-kernel.release
EXT=/root/s11/kbuild/ext; rm -rf $EXT; mkdir -p $EXT
cp -r $S/fs/erofs $S/fs/cachefiles $EXT/
time make -s -C $H M=$EXT/cachefiles CONFIG_CACHEFILES=m CONFIG_CACHEFILES_ONDEMAND=y \
  KCFLAGS="-DCONFIG_CACHEFILES_ONDEMAND=1" -j$(nproc) modules
time make -s -C $H M=$EXT/erofs CONFIG_EROFS_FS=m CONFIG_EROFS_FS_ONDEMAND=y \
  KCFLAGS="-DCONFIG_EROFS_FS_ONDEMAND=1" -j$(nproc) modules
ls $EXT/erofs/*.o | tee $OUT/erofs-objects.txt
ls $EXT/cachefiles/*.o | tee $OUT/cachefiles-objects.txt
cp $EXT/erofs/erofs.ko $EXT/cachefiles/cachefiles.ko $OUT/
for m in erofs cachefiles; do
  modinfo $OUT/$m.ko | grep -E '^(vermagic|depends|srcversion|name|sig_id|signer)' | sed "s/^/rebuilt $m: /"
  modinfo /lib/modules/$KREL/kernel/fs/$m/$m.ko* | grep -E '^(vermagic|depends|srcversion|name|sig_id|signer)' | sed "s/^/stock   $m: /"
done | tee $OUT/modinfo.txt
nm $OUT/erofs.ko | grep -c -i fscache | sed 's/^/erofs fscache symbols: /' | tee -a $OUT/modinfo.txt
nm $OUT/cachefiles.ko | grep -c ondemand | sed 's/^/cachefiles ondemand symbols: /' | tee -a $OUT/modinfo.txt
sha256sum $OUT/*.ko | tee $OUT/sha256.txt
ls -la /lib/modules/$KREL/kernel/fs/netfs/ /lib/modules/$KREL/kernel/fs/erofs/ /lib/modules/$KREL/kernel/fs/cachefiles/ /lib/modules/$KREL/kernel/drivers/block/nbd.ko* | tee $OUT/stock-files.txt
echo MODULES_BUILT
