#!/usr/bin/env bash
set -euo pipefail

readonly EXPECTED_COMMIT="f41abb21324f6b0520abf34b7720aa260ddd10eb"
readonly PACKAGE="uvm-ublk-daemon"
readonly BINARY="uvm-ublk-daemon"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly PATCH_PATH="${SCRIPT_DIR}/agentenv-streaming-dense-export.patch"

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
[[ -f "${PATCH_PATH}" ]]
git -C "${SOURCE_DIR}" apply --check "${PATCH_PATH}"
git -C "${SOURCE_DIR}" apply "${PATCH_PATH}"
PATCH_APPLIED=true
cleanup() {
  if [[ "${PATCH_APPLIED}" == true ]]; then
    git -C "${SOURCE_DIR}" apply --reverse "${PATCH_PATH}"
  fi
}
trap cleanup EXIT

(
  cd "${SOURCE_DIR}"
  cargo test --locked --release -p "${PACKAGE}" --lib protocol::tests
  cargo build --locked --release -p "${PACKAGE}" --bin "${BINARY}"
)

readonly BUILT_BINARY="${SOURCE_DIR}/target/release/${BINARY}"
[[ -x "${BUILT_BINARY}" ]]
readonly BINARY_SHA256="$(sha256sum "${BUILT_BINARY}" | awk '{print $1}')"
readonly PATCH_SHA256="$(sha256sum "${PATCH_PATH}" | awk '{print $1}')"
readonly ARTIFACT_NAME="${BINARY}-${BINARY_SHA256}"
install -m 0755 "${BUILT_BINARY}" "${OUTPUT_DIR}/${ARTIFACT_NAME}"
install -m 0644 "${SOURCE_DIR}/LICENSE" "${OUTPUT_DIR}/${ARTIFACT_NAME}.LICENSE"

python3 - "${OUTPUT_DIR}/${ARTIFACT_NAME}.manifest.json" <<PY
import json
from pathlib import Path
import platform

payload = {
    "agentenv_commit": "${EXPECTED_COMMIT}",
    "artifact": "${ARTIFACT_NAME}",
    "artifact_sha256": "${BINARY_SHA256}",
    "cargo_package": "${PACKAGE}",
    "host_architecture": platform.machine(),
    "license": "MIT",
    "patch": "$(basename "${PATCH_PATH}")",
    "patch_sha256": "${PATCH_SHA256}",
    "schema": 1,
}
Path("${OUTPUT_DIR}/${ARTIFACT_NAME}.manifest.json").write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

echo "${OUTPUT_DIR}/${ARTIFACT_NAME}"
