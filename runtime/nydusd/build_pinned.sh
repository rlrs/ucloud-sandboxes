#!/usr/bin/env bash
# Build nydusd for the node bundle: nydus v2.4.5 with the block-nbd export
# (C2.1, docs/benchmarks/nydusd-spike-2026-10-03). No release binary carries
# block-nbd, so this is the only source of the bundle's nydusd.
#   build_pinned.sh NYDUS_CHECKOUT OUTPUT_DIRECTORY   (PROTOC must name protoc)
set -euo pipefail

readonly EXPECTED_COMMIT="e3190057422fee17f594bb3a5c10741b45dac6ce"  # v2.4.5
readonly FEATURES="block-nbd"

usage() {
  echo "usage: $0 NYDUS_CHECKOUT OUTPUT_DIRECTORY" >&2
  exit 2
}

[[ $# -eq 2 ]] || usage
readonly SOURCE_DIR="$(cd "$1" && pwd -P)"
mkdir -p "$2"
readonly OUTPUT_DIR="$(cd "$2" && pwd -P)"

[[ "$(git -C "${SOURCE_DIR}" rev-parse HEAD)" == "${EXPECTED_COMMIT}" ]] || {
  echo "nydus checkout is not at ${EXPECTED_COMMIT} (v2.4.5)" >&2
  exit 1
}
[[ -z "$(git -C "${SOURCE_DIR}" status --porcelain=v1 --untracked-files=all)" ]] || {
  echo "nydus checkout must be clean" >&2
  exit 1
}
command -v cargo >/dev/null
[[ -x "${PROTOC:-}" ]] || { echo "PROTOC must name a protoc executable" >&2; exit 1; }

(
  cd "${SOURCE_DIR}"
  cargo build --locked --release --bin nydusd --features "${FEATURES}"
)

readonly BUILT_BINARY="${SOURCE_DIR}/target/release/nydusd"
[[ -x "${BUILT_BINARY}" ]]
"${BUILT_BINARY}" --version >/dev/null
readonly BINARY_SHA256="$(sha256sum "${BUILT_BINARY}" | awk '{print $1}')"
install -m 0755 "${BUILT_BINARY}" "${OUTPUT_DIR}/nydusd"
install -m 0644 "${SOURCE_DIR}/LICENSE-APACHE" "${OUTPUT_DIR}/LICENSE"

python3 - "${OUTPUT_DIR}/build-manifest.json" "${EXPECTED_COMMIT}" "${FEATURES}" "${BINARY_SHA256}" \
  "${SOURCE_DIR}/rust-toolchain.toml" <<'PYMANIFEST'
import json
import platform
from pathlib import Path
import sys

manifest, commit, features, digest, toolchain = sys.argv[1:]
channel = next((line.split("=", 1)[1].strip().strip('"') for line in Path(toolchain).read_text().splitlines()
                if line.strip().startswith("channel")), "")
Path(manifest).write_text(json.dumps({
    "schema": 1, "nydus_commit": commit, "features": features.split(","), "rust_toolchain": channel,
    "artifact_sha256": digest, "host_architecture": platform.machine(), "license": "Apache-2.0",
}, indent=2, sort_keys=True) + "\n")
PYMANIFEST
echo "nydusd ${BINARY_SHA256}"
