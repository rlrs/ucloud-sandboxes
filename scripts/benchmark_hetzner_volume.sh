#!/usr/bin/env bash
set -euo pipefail

# Compare a mounted Hetzner Cloud Volume with a directory on the VM root disk.
# The test creates and removes one temporary fio file on each filesystem. It
# does not format the Volume, but it does issue fstrim against its mount.

usage() {
  cat >&2 <<'EOF'
usage: benchmark_hetzner_volume.sh \
  --volume-mount PATH --root-dir PATH --output-dir PATH \
  [--runtime-seconds N] [--size SIZE]
EOF
  exit 2
}

volume_mount=""
root_dir=""
output_dir=""
runtime_seconds=30
size=4G

while [[ $# -gt 0 ]]; do
  case "$1" in
    --volume-mount)
      [[ $# -ge 2 ]] || usage
      volume_mount="$2"
      shift 2
      ;;
    --root-dir)
      [[ $# -ge 2 ]] || usage
      root_dir="$2"
      shift 2
      ;;
    --output-dir)
      [[ $# -ge 2 ]] || usage
      output_dir="$2"
      shift 2
      ;;
    --runtime-seconds)
      [[ $# -ge 2 ]] || usage
      runtime_seconds="$2"
      shift 2
      ;;
    --size)
      [[ $# -ge 2 ]] || usage
      size="$2"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

[[ $EUID -eq 0 ]] || {
  echo "this benchmark must run as root" >&2
  exit 1
}
[[ -n "$volume_mount" && -n "$root_dir" && -n "$output_dir" ]] || usage
[[ "$runtime_seconds" =~ ^[1-9][0-9]*$ ]] || usage
command -v fio >/dev/null
command -v findmnt >/dev/null
command -v fstrim >/dev/null
command -v jq >/dev/null

volume_mount="$(realpath "$volume_mount")"
root_dir="$(realpath "$root_dir")"
mkdir -p "$output_dir"
output_dir="$(realpath "$output_dir")"
findmnt --mountpoint "$volume_mount" >/dev/null
[[ "$(findmnt -n -o SOURCE --target "$volume_mount")" != \
  "$(findmnt -n -o SOURCE --target "$root_dir")" ]] || {
  echo "volume and root benchmark directories are on the same filesystem" >&2
  exit 1
}

volume_work="$(mktemp -d "$volume_mount/ucloud-volume-fio.XXXXXX")"
root_work="$(mktemp -d "$root_dir/ucloud-root-fio.XXXXXX")"

cleanup() {
  rm -f "$volume_work/fio.data" "$root_work/fio.data"
  rmdir "$volume_work" "$root_work" 2>/dev/null || true
}
trap cleanup EXIT

fio_common=(
  --ioengine=libaio
  --direct=1
  --group_reporting=1
  --output-format=json
  --time_based=1
  "--runtime=$runtime_seconds"
  "--size=$size"
  --lat_percentiles=1
  --percentile_list=50:95:99:99.9
)

run_target() {
  local name="$1"
  local work="$2"
  local target="$work/fio.data"

  fio "${fio_common[@]}" \
    "--name=${name}-sequential-write" \
    "--filename=$target" \
    --rw=write --bs=1M --iodepth=32 --end_fsync=1 \
    "--output=$output_dir/${name}-sequential-write.json"
  sync -f "$work"
  echo 3 >/proc/sys/vm/drop_caches

  fio "${fio_common[@]}" \
    "--name=${name}-sequential-read" \
    "--filename=$target" \
    --rw=read --bs=1M --iodepth=32 \
    "--output=$output_dir/${name}-sequential-read.json"
  echo 3 >/proc/sys/vm/drop_caches

  fio "${fio_common[@]}" \
    "--name=${name}-random-mixed" \
    "--filename=$target" \
    --rw=randrw --rwmixread=70 --bs=4K --iodepth=32 \
    "--output=$output_dir/${name}-random-mixed.json"
  sync -f "$work"
}

findmnt --json --target "$volume_mount" >"$output_dir/volume-mount.json"
findmnt --json --target "$root_dir" >"$output_dir/root-mount.json"
run_target root "$root_work"
run_target volume "$volume_work"
fstrim -v "$volume_mount" >"$output_dir/volume-trim.txt"

jq -n \
  --arg size "$size" \
  --argjson runtime_seconds "$runtime_seconds" \
  --slurpfile root_seq_write "$output_dir/root-sequential-write.json" \
  --slurpfile root_seq_read "$output_dir/root-sequential-read.json" \
  --slurpfile root_random "$output_dir/root-random-mixed.json" \
  --slurpfile volume_seq_write "$output_dir/volume-sequential-write.json" \
  --slurpfile volume_seq_read "$output_dir/volume-sequential-read.json" \
  --slurpfile volume_random "$output_dir/volume-random-mixed.json" \
  '{
    schema: 1,
    size: $size,
    runtime_seconds: $runtime_seconds,
    root: {
      sequential_write: $root_seq_write[0].jobs[0].write,
      sequential_read: $root_seq_read[0].jobs[0].read,
      random_mixed: {
        read: $root_random[0].jobs[0].read,
        write: $root_random[0].jobs[0].write
      }
    },
    volume: {
      sequential_write: $volume_seq_write[0].jobs[0].write,
      sequential_read: $volume_seq_read[0].jobs[0].read,
      random_mixed: {
        read: $volume_random[0].jobs[0].read,
        write: $volume_random[0].jobs[0].write
      }
    }
  }' >"$output_dir/summary.json"

jq '{
  root: {
    sequential_write_bytes_per_second: .root.sequential_write.bw_bytes,
    sequential_read_bytes_per_second: .root.sequential_read.bw_bytes,
    random_mixed_iops: (
      .root.random_mixed.read.iops + .root.random_mixed.write.iops
    ),
    random_read_p99_latency_ns: (
      .root.random_mixed.read.clat_ns.percentile["99.000000"]
    ),
    random_write_p99_latency_ns: (
      .root.random_mixed.write.clat_ns.percentile["99.000000"]
    )
  },
  volume: {
    sequential_write_bytes_per_second: .volume.sequential_write.bw_bytes,
    sequential_read_bytes_per_second: .volume.sequential_read.bw_bytes,
    random_mixed_iops: (
      .volume.random_mixed.read.iops + .volume.random_mixed.write.iops
    ),
    random_read_p99_latency_ns: (
      .volume.random_mixed.read.clat_ns.percentile["99.000000"]
    ),
    random_write_p99_latency_ns: (
      .volume.random_mixed.write.clat_ns.percentile["99.000000"]
    )
  }
}' "$output_dir/summary.json"
