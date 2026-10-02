#!/bin/bash
# S11: install build deps, fetch exact Ubuntu 7.0.0-30.30 kernel source and headers, Nydus v2.4.5, registry.
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
W=/root/s11; mkdir -p $W/dl; cd $W/dl
apt-get update -q || true
apt-get install -y -q build-essential flex bison bc libelf-dev libssl-dev libdw-dev dwarves zstd kmod cpio rsync python3-venv python3-cryptography jq curl git attr sysstat fio skopeo 2>&1 | tail -3 || true
NV=v2.4.5
[ -s nydus-static-$NV-linux-amd64.tgz ] || curl -fsSL -o nydus-static-$NV-linux-amd64.tgz https://github.com/dragonflyoss/nydus/releases/download/$NV/nydus-static-$NV-linux-amd64.tgz
tar -xzf nydus-static-$NV-linux-amd64.tgz
find nydus-static -maxdepth 1 -type f -exec install -m 0755 {} /usr/local/bin/ \;
nydusd --version > versions.txt; nydus-image --version >> versions.txt; nydusify --version >> versions.txt 2>&1 || true; cat versions.txt
[ -s registry.tgz ] || curl -fsSL -o registry.tgz https://github.com/distribution/distribution/releases/download/v3.0.0/registry_3.0.0_linux_amd64.tar.gz
tar -xzf registry.tgz registry && install -m 0755 registry /usr/local/bin/registry
echo SETUP_DONE
