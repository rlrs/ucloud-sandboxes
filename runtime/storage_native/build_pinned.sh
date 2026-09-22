#!/usr/bin/env bash
set -euo pipefail

readonly EXPECTED_COMMIT="771ea55ca80abbfacc85e716ec91c40e82b3398b"
readonly PACKAGE="uvm-ublk-daemon"
readonly BINARY="uvm-ublk-daemon"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly PATCH_PATHS=(
  "${SCRIPT_DIR}/agentenv-streaming-dense-export.patch"
  "${SCRIPT_DIR}/agentenv-pooled-delete.patch"
  "${SCRIPT_DIR}/agentenv-owner-identity.patch"
  "${SCRIPT_DIR}/agentenv-owner-transitions.patch"
  "${SCRIPT_DIR}/agentenv-premerged-identity.patch"
  "${SCRIPT_DIR}/agentenv-device-reuse.patch"
  "${SCRIPT_DIR}/agentenv-jemalloc.patch"
  "${SCRIPT_DIR}/agentenv-storage-upgrade-compatibility.patch"
)

usage() {
  echo "usage: $0 AGENTENV_CHECKOUT OUTPUT_DIRECTORY" >&2
  exit 2
}

[[ $# -eq 2 ]] || usage
readonly SOURCE_DIR="$(cd "$1" && pwd -P)"
mkdir -p "$2"
readonly OUTPUT_DIR="$(cd "$2" && pwd -P)"

[[ "$(git -C "${SOURCE_DIR}" rev-parse HEAD)" == "${EXPECTED_COMMIT}" ]] || {
  echo "AgentEnv checkout is not at ${EXPECTED_COMMIT}" >&2
  exit 1
}
[[ -z "$(git -C "${SOURCE_DIR}" status --porcelain=v1 --untracked-files=all)" ]] || {
  echo "AgentEnv checkout must be clean" >&2
  exit 1
}
command -v cargo >/dev/null
PATCHES_APPLIED=0
cleanup() {
  if ((PATCHES_APPLIED > 0)); then
    for ((index=PATCHES_APPLIED - 1; index >= 0; index--)); do
      git -C "${SOURCE_DIR}" apply --reverse "${PATCH_PATHS[index]}"
    done
  fi
}
trap cleanup EXIT
for patch_path in "${PATCH_PATHS[@]}"; do
  [[ -f "${patch_path}" ]]
  git -C "${SOURCE_DIR}" apply --check "${patch_path}"
  git -C "${SOURCE_DIR}" apply "${patch_path}"
  ((PATCHES_APPLIED += 1))
done

(
  cd "${SOURCE_DIR}"
  cargo test --locked --release -p overlaybd --features io-uring --lib lsmt::file::tests
  cargo test --locked --release -p overlaybd --features io-uring --lib backend::cache::tests
  cargo test --locked --release -p "${PACKAGE}" --lib
  cargo build --locked --release -p "${PACKAGE}" --bin "${BINARY}"
)

readonly BUILT_BINARY="${SOURCE_DIR}/target/release/${BINARY}"
[[ -x "${BUILT_BINARY}" ]]
readonly BINARY_SHA256="$(sha256sum "${BUILT_BINARY}" | awk '{print $1}')"
readonly ARTIFACT_NAME="${BINARY}-${BINARY_SHA256}"
install -m 0755 "${BUILT_BINARY}" "${OUTPUT_DIR}/${ARTIFACT_NAME}"
install -m 0644 "${SOURCE_DIR}/LICENSE" "${OUTPUT_DIR}/${ARTIFACT_NAME}.LICENSE"

python3 - "${OUTPUT_DIR}/${ARTIFACT_NAME}.manifest.json" "${EXPECTED_COMMIT}" "${ARTIFACT_NAME}" "${BINARY_SHA256}" "${PATCH_PATHS[@]}" <<'PYMANIFEST'
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys

manifest, commit, artifact, digest, *patches = sys.argv[1:]
payload = {
    "agentenv_commit": commit,
    "artifact": artifact,
    "artifact_sha256": digest,
    "cargo_package": "uvm-ublk-daemon",
    "host_architecture": platform.machine(),
    "hybrid_upper_sub_version": 2,
    "license": "MIT",
    "patches": [
        {"name": Path(path).name, "sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest()}
        for path in patches
    ],
    "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
    "schema": 3,
}
Path(manifest).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PYMANIFEST

echo "${OUTPUT_DIR}/${ARTIFACT_NAME}"
