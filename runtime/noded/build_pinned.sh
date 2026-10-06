#!/usr/bin/env bash
# Build ucloud-noded, the node daemon, for the node bundle from committed source:
# a static x86_64-unknown-linux-musl binary from Cargo.lock (--locked), built in
# the pinned Rust Alpine image (native musl C toolchain, for bundled SQLite),
# without build IDs and with paths remapped, so one commit gives one artifact.
#   build_pinned.sh OUTPUT_DIRECTORY
set -euo pipefail

[[ $# -eq 1 ]] || { echo "usage: $0 OUTPUT_DIRECTORY" >&2; exit 2; }
readonly SOURCE_DIR="$(cd "$(dirname "$0")" && pwd -P)"
mkdir -p "$1"
readonly OUTPUT_DIR="$(cd "$1" && pwd -P)"
readonly TARGET=x86_64-unknown-linux-musl
readonly IMAGE="rust@sha256:7cc1c22d77d9432f7fe012a70e6d3e555af54c2a6832700ed7d553f1769ae89f"  # rust:1.98.1-alpine

[[ -z "$(git -C "${SOURCE_DIR}" status --porcelain=v1 --untracked-files=all -- . ':!target')" ]] || {
  echo "runtime/noded must be committed" >&2
  exit 1
}
readonly COMMIT="$(git -C "${SOURCE_DIR}" rev-parse HEAD)"
readonly TREE="$(git -C "${SOURCE_DIR}" rev-parse HEAD:runtime/noded)"
readonly BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "${BUILD_DIR}"' EXIT
git -C "${SOURCE_DIR}" archive HEAD -- . | tar -x -C "${BUILD_DIR}"
docker run --rm --network host -v "${BUILD_DIR}:/noded" -w /noded "${IMAGE}" sh -euc '
  apk add --no-cache -q musl-dev >/dev/null
  RUSTFLAGS="-C target-feature=+crt-static -C link-arg=-Wl,--build-id=none --remap-path-prefix=/noded=/noded \
--remap-path-prefix=/usr/local/cargo=/cargo" cargo build --release --locked --target '"${TARGET}"'
  rustc --version > rustc-version
  chown -R '"$(id -u):$(id -g)"' target rustc-version'
readonly RUSTC_VERSION="$(cat "${BUILD_DIR}/rustc-version") (${IMAGE})"
install -m 0755 "${BUILD_DIR}/target/${TARGET}/release/ucloud-noded" "${OUTPUT_DIR}/ucloud-noded"
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
