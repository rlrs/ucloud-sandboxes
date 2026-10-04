#!/usr/bin/env bash
# Build ucloud-chunk-serve, the store node's read server, for the node bundle
# from committed source: a static linux/amd64 binary without build IDs, so one
# commit and Go version give one artifact.
#   build_pinned.sh OUTPUT_DIRECTORY
set -euo pipefail

[[ $# -eq 1 ]] || { echo "usage: $0 OUTPUT_DIRECTORY" >&2; exit 2; }
readonly SOURCE_DIR="$(cd "$(dirname "$0")" && pwd -P)"
mkdir -p "$1"
readonly OUTPUT_DIR="$(cd "$1" && pwd -P)"

[[ -z "$(git -C "${SOURCE_DIR}" status --porcelain=v1 --untracked-files=all -- .)" ]] || {
  echo "runtime/chunk_serve must be committed" >&2
  exit 1
}
readonly COMMIT="$(git -C "${SOURCE_DIR}" rev-parse HEAD)"
readonly TREE="$(git -C "${SOURCE_DIR}" rev-parse HEAD:runtime/chunk_serve)"
readonly GO_VERSION="$(cd "${SOURCE_DIR}" && GOTOOLCHAIN=local go env GOVERSION)"

(
  cd "${SOURCE_DIR}"
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOTOOLCHAIN=local GOFLAGS=-mod=readonly \
    go build -trimpath -buildvcs=false -ldflags='-s -w -buildid=' -o "${OUTPUT_DIR}/ucloud-chunk-serve" .
)
readonly BINARY_SHA256="$(sha256sum "${OUTPUT_DIR}/ucloud-chunk-serve" | awk '{print $1}')"

python3 - "${OUTPUT_DIR}/build-manifest.json" "${COMMIT}" "${TREE}" "${GO_VERSION}" "${BINARY_SHA256}" <<'PYMANIFEST'
import json
from pathlib import Path
import sys

manifest, commit, tree, go_version, digest = sys.argv[1:]
Path(manifest).write_text(json.dumps({
    "schema": 1, "source_commit": commit, "source_tree": tree, "go_version": go_version,
    "artifact_sha256": digest, "target": "linux/amd64",
}, indent=2, sort_keys=True) + "\n")
PYMANIFEST
echo "ucloud-chunk-serve ${BINARY_SHA256}"
