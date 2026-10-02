#!/bin/bash
# S11 step 1b: check signature policy, unload stock modules, load the rebuilt ones.
set -uxo pipefail
KREL=7.0.0-30-generic
OUT=/root/s11/out/modules
{
echo "lockdown: $(cat /sys/kernel/security/lockdown)"
echo "modules_disabled: $(cat /proc/sys/kernel/modules_disabled)"
echo "sig_enforce: $(cat /sys/module/module/parameters/sig_enforce)"
echo "secure boot: $(mokutil --sb-state 2>&1)"
echo "taint before: $(cat /proc/sys/kernel/tainted)"
echo "loaded before:"; lsmod | grep -E '^(erofs|cachefiles|netfs) ' || true
grep -E 'erofs|cachefiles' /proc/mounts || true
modprobe -r erofs 2>&1; modprobe -r cachefiles 2>&1
echo "after unload:"; lsmod | grep -E '^(erofs|cachefiles|netfs) ' || true
mkdir -p /lib/modules/$KREL/updates/s11
install -m 0644 $OUT/erofs.ko $OUT/cachefiles.ko /lib/modules/$KREL/updates/s11/
depmod -a $KREL
modinfo -n erofs; modinfo -n cachefiles
modprobe cachefiles && echo "cachefiles load rc=0"
modprobe erofs && echo "erofs load rc=0"
lsmod | grep -E '^(erofs|cachefiles|netfs) '
echo "taint after: $(cat /proc/sys/kernel/tainted)"
dmesg | grep -iE 'erofs|cachefiles|netfs|verification|taint' | tail -10
ls -la /dev/cachefiles
cat /sys/module/erofs/srcversion /sys/module/cachefiles/srcversion
grep -E '^(srcversion)' <(modinfo $OUT/erofs.ko) <(modinfo $OUT/cachefiles.ko)
} 2>&1 | tee $OUT/load.txt
