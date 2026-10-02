#!/bin/bash
# S11 step 1: rebuild erofs.ko and cachefiles.ko with the on-demand options for exactly 7.0.0-30-generic.
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
KREL=7.0.0-30-generic
UBUNTU_VER=7.0.0-30.30
OUT=/root/s11/out/modules; mkdir -p $OUT
apt-get install -y -q linux-source-7.0.0=$UBUNTU_VER linux-headers-$KREL >/dev/null
dpkg -l linux-source-7.0.0 linux-headers-$KREL | tail -2 | tee $OUT/packages.txt
B=/root/s11/kbuild; rm -rf $B; mkdir -p $B; cd $B
time tar -xjf /usr/src/linux-source-7.0.0/linux-source-7.0.0.tar.bz2
cd linux-source-7.0.0
head -6 Makefile | tee $OUT/makefile-head.txt
cp /boot/config-$KREL .config.stock
cp .config.stock .config
# Ubuntu's trusted/revocation key files are not in the source tarball; they only affect vmlinux.
scripts/config --set-str SYSTEM_TRUSTED_KEYS "" --set-str SYSTEM_REVOCATION_KEYS ""
scripts/config --enable EROFS_FS_ONDEMAND --enable CACHEFILES_ONDEMAND
make -s olddefconfig
diff <(grep -E '^(CONFIG_|# CONFIG_)' .config.stock | sort) <(grep -E '^(CONFIG_|# CONFIG_)' .config | sort) | tee $OUT/config.diff || true
grep -E 'CONFIG_(EROFS_FS_ONDEMAND|CACHEFILES_ONDEMAND|FSCACHE|NETFS_SUPPORT|CACHEFILES)=' .config | tee $OUT/config.relevant
# Which code outside fs/erofs and fs/cachefiles looks at the two options?
grep -rn --include='*.[ch]' --include='Kconfig*' --include='Makefile*' -E 'CONFIG_(EROFS_FS_ONDEMAND|CACHEFILES_ONDEMAND)|\b(EROFS_FS_ONDEMAND|CACHEFILES_ONDEMAND)\b' . \
  | grep -v -E '^\./(fs/erofs|fs/cachefiles)/' | tee $OUT/option-users-outside.txt || true
grep -rn -E 'ONDEMAND' include/ | tee $OUT/include-ondemand.txt || true
echo "-30-generic" > localversion-s11
make -s -j$(nproc) modules_prepare
cat include/config/kernel.release | tee $OUT/kernel.release
test "$(cat include/config/kernel.release)" = "$KREL"
cp /usr/src/linux-headers-$KREL/Module.symvers .
time make -s -j$(nproc) M=fs/cachefiles modules
time make -s -j$(nproc) M=fs/erofs modules
cp fs/erofs/erofs.ko fs/cachefiles/cachefiles.ko $OUT/
for m in erofs cachefiles; do
  modinfo $OUT/$m.ko | grep -E '^(vermagic|depends|srcversion|name|sig_id|signer)' | sed "s/^/rebuilt $m: /"
  modinfo /lib/modules/$KREL/kernel/fs/$m/$m.ko* | grep -E '^(vermagic|depends|srcversion|name|sig_id|signer)' | sed "s/^/stock   $m: /"
done | tee $OUT/modinfo.txt
sha256sum $OUT/*.ko | tee $OUT/sha256.txt
ls -la /lib/modules/$KREL/kernel/fs/netfs/ /lib/modules/$KREL/kernel/fs/erofs/ /lib/modules/$KREL/kernel/fs/cachefiles/ | tee $OUT/stock-files.txt
echo MODULES_BUILT
