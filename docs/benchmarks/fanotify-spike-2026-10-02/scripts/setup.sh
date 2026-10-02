#!/bin/bash
# S13 VM setup: Nydus v2.4.5 static release (GitHub), NBD, kernel source + headers (for fs/notify,
# fs/erofs and the inode_share erofs.ko), erofs-utils, xfsprogs. No Docker Hub. Records kernel facts.
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
KREL=7.0.0-30-generic; UBUNTU=7.0.0-30.30
mkdir -p /data/manifests /data/oci/blobs/sha256 /data/s12 /data/results /data/logs /data/mt /data/s13
cp /root/s12/rafs.py /root/rafs.py
cd /root/s12
[ -s nydus.tgz ] || curl -fsSL -o nydus.tgz https://github.com/dragonflyoss/nydus/releases/download/v2.4.5/nydus-static-v2.4.5-linux-amd64.tgz
sha256sum nydus.tgz | tee /data/results/nydus.sha256
tar -xzf nydus.tgz
install -m 0755 nydus-static/nydus-image /usr/local/bin/nydus-image
nydus-image --version | head -3
modprobe nbd nbds_max=1024 max_part=0
apt-get update -q
apt-get install -y -q build-essential libelf-dev libdw-dev dwarves zstd kmod "linux-source-7.0.0=$UBUNTU" \
  "linux-headers-$KREL=$UBUNTU" erofs-utils xfsprogs attr 2>&1 | tail -3
mkdir -p /data/k && cd /data/k
[ -d linux-source-7.0.0 ] || tar -xjf /usr/src/linux-source-7.0.0/linux-source-7.0.0.tar.bz2
{
  uname -a; cat /proc/version
  grep -E "CONFIG_FANOTIFY|CONFIG_FSNOTIFY|CONFIG_EROFS|CONFIG_FS_STACK|CONFIG_XFS_FS=|CONFIG_BLK_DEV_NBD" /boot/config-$KREL
  sysctl fs.fanotify 2>/dev/null || true
  grep -h SUBLEVEL /data/k/linux-source-7.0.0/Makefile | head -1
  mkfs.erofs --version 2>&1 | head -1; python3 --version; xfs_info -V
  grep -n "PRE_ACCESS\|INFO_TYPE_RANGE\|FAN_DENY_ERRNO\|FAN_ERRNO" /usr/include/linux/fanotify.h || true
  cat /proc/cmdline; cat /sys/kernel/security/lockdown 2>/dev/null || true
  lscpu | grep -E "Model name|^CPU\(s\)"; free -g; df -h / /data
} > /data/results/kernel-facts.txt 2>&1
cat /data/results/kernel-facts.txt
echo SETUP_DONE
