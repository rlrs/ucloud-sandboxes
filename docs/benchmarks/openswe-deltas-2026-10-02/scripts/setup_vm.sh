#!/bin/bash
# One-time VM preparation (VM-local only): docker check, nydus-image for calibration.
# The venv had no pip on the snapshot; walk.py falls back to Python 3.14's stdlib compression.zstd.
set -u
mkdir -p /data/w/log
cd /data/w
command -v python3; python3 --version; docker --version; df -h /data | tail -1; nproc; free -g | head -2
grep -s insecure /etc/docker/daemon.json || echo "no insecure-registries in daemon.json"
docker info 2>/dev/null | grep -A3 -i 'insecure registries'
docker info 2>/dev/null | grep -i -E 'storage driver|driver-type'
python3 -m venv /data/w/venv 2>&1 | tail -1 || true
/data/w/venv/bin/pip -q install zstandard 2>&1 | tail -2
/data/w/venv/bin/python -c "import zstandard; print('zstandard', zstandard.__version__)"
curl -sSL -o /tmp/nydus.tgz https://github.com/dragonflyoss/nydus/releases/download/v2.4.5/nydus-static-v2.4.5-linux-amd64.tgz && sha256sum /tmp/nydus.tgz && tar -C /tmp -xzf /tmp/nydus.tgz && install -m755 /tmp/nydus-static/nydus-image /usr/local/bin/ && nydus-image --version | head -2
