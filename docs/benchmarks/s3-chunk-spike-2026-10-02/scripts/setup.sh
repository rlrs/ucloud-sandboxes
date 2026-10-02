#!/bin/bash
# S12 VM setup: Nydus v2.4.5 static release (GitHub), NBD module, work dirs. No Docker Hub.
set -euxo pipefail
mkdir -p /data/manifests /data/oci/blobs/sha256 /data/s12 /data/results /data/logs
cd /root/s12
cp rafs.py /root/rafs.py
[ -s nydus.tgz ] || curl -fsSL -o nydus.tgz https://github.com/dragonflyoss/nydus/releases/download/v2.4.5/nydus-static-v2.4.5-linux-amd64.tgz
sha256sum nydus.tgz | tee nydus.sha256
tar -xzf nydus.tgz
install -m 0755 nydus-static/nydus-image /usr/local/bin/nydus-image
nydus-image --version | head -3
modprobe nbd nbds_max=1024 max_part=0
cat /sys/module/nbd/parameters/nbds_max
echo SETUP_DONE
