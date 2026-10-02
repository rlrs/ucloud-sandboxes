#!/bin/bash
# S12 (e)(iii): kernel source + headers for an out-of-tree erofs.ko with EROFS_FS_PAGE_CACHE_SHARE (S11 recipe).
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
KREL=7.0.0-30-generic; UBUNTU=7.0.0-30.30
grep -E "CONFIG_FS_STACK|CONFIG_EROFS|CONFIG_FS_ENCRYPTION=|CONFIG_NETFS" /boot/config-$KREL || true
apt-get update -q
apt-get install -y -q build-essential libelf-dev libdw-dev dwarves zstd kmod "linux-source-7.0.0=$UBUNTU" "linux-headers-$KREL=$UBUNTU" erofs-utils 2>&1 | tail -3
mkdir -p /data/k && cd /data/k
[ -d linux-source-7.0.0 ] || tar -xjf /usr/src/linux-source-7.0.0/linux-source-7.0.0.tar.bz2
mkfs.erofs --version || true
echo KSETUP_DONE
