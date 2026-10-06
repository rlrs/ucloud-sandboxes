#!/usr/bin/env bash
# Build ucloud-noded, the node's front door, for the node bundle from committed
# source: a static x86_64-unknown-linux-musl binary from Cargo.lock (--locked),
# without build IDs and with local paths remapped, so one commit and Rust
# toolchain give one artifact.
#   build_pinned.sh OUTPUT_DIRECTORY
set -euo pipefail

[[ $# -eq 1 ]] || { echo "usage: $0 OUTPUT_DIRECTORY" >&2; exit 2; }
readonly SOURCE_DIR="$(cd "$(dirname "$0")" && pwd -P)"
mkdir -p "$1"
readonly OUTPUT_DIR="$(cd "$1" && pwd -P)"
readonly TARGET=x86_64-unknown-linux-musl

[[ -z "$(git -C "${SOURCE_DIR}" status --porcelain=v1 --untracked-files=all -- . ':!target')" ]] || {
  echo "runtime/noded must be committed" >&2
  exit 1
}
readonly COMMIT="$(git -C "${SOURCE_DIR}" rev-parse HEAD)"
readonly TREE="$(git -C "${SOURCE_DIR}" rev-parse HEAD:runtime/noded)"
readonly RUSTC_VERSION="$(rustc --version)"
readonly CARGO_HOME_DIR="${CARGO_HOME:-$HOME/.cargo}"

(
  cd "${SOURCE_DIR}"
  RUSTFLAGS="-C target-feature=+crt-static -C link-arg=-Wl,--build-id=none \
--remap-path-prefix=${SOURCE_DIR}=/noded --remap-path-prefix=${CARGO_HOME_DIR}=/cargo" \
    cargo build --release --locked --target "${TARGET}"
)
install -m 0755 "${SOURCE_DIR}/target/${TARGET}/release/ucloud-noded" "${OUTPUT_DIR}/ucloud-noded"
readonly BINARY_SHA256="$(sha256sum "${OUTPUT_DIR}/ucloud-noded" | awk '{print $1}')"

python3 - "${OUTPUT_DIR}/build-manifest.json" "${COMMIT}" "${TREE}" "${RUSTC_VERSION}" "${BINARY_SHA256}" <<'PYMANIFEST'
import json
from pathlib import Path
import sys

manifest, commit, tree, rustc, digest = sys.argv[1:]
Path(manifest).write_text(json.dumps({
    "schema": 1, "source_commit": commit, "source_tree": tree, "rustc_version": rustc,
    "artifact_sha256": digest, "target": "x86_64-unknown-linux-musl",
}, indent=2, sort_keys=True) + "\n")
PYMANIFEST
echo "ucloud-noded ${BINARY_SHA256}"
