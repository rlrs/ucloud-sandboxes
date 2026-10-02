#!/bin/bash
# S13 corpus prep (S10/S12 scripts unchanged): fetch S10's 181-image sample read-only from our
# registry, convert per layer (nydus-image v2.4.5, 256 KiB, sha256, zstd, no dict), pack into the
# local pack store (/data/s12/packs, standing in for the store node), then the same for S12's 64-image
# page-cache set (select_pc.py, deterministic seed), packed into the same index.
set -x
cd /root/s12
python3 fetch.py /root/s12/sample-s10.json > /data/logs/fetch.log 2>&1
python3 convert.py --mode nodict --chunk-size 0x40000 --out /data/work/nodict-256k --workers 44 --seed 13 > /data/logs/convert.log 2>&1
python3 packer.py --rundir /data/work/nodict-256k --sample /data/manifests/sample-manifests.json --tag main --maps all > /data/logs/packer-main.log 2>&1
echo MAIN_DONE > /data/logs/prep.state
python3 select_pc.py --candidates /root/s12/pc-candidates.json > /data/logs/select-pc.log 2>&1
SAMPLE_MANIFESTS=/data/manifests/pc-manifests.json python3 convert.py --mode nodict --chunk-size 0x40000 --out /data/work/pc-256k --workers 44 --seed 13 > /data/logs/convert-pc.log 2>&1
python3 packer.py --rundir /data/work/pc-256k --sample /data/manifests/pc-manifests.json --tag pc --prefix pc --maps all > /data/logs/packer-pc.log 2>&1
echo PC_DONE > /data/logs/prep.state
