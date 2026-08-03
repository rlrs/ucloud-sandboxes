#!/usr/bin/env bash
set -euo pipefail

readonly EXPECTED_COMMIT="f41abb21324f6b0520abf34b7720aa260ddd10eb"
readonly PACKAGE="uvm-ublk-daemon"
readonly BINARY="uvm-ublk-daemon"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly PATCH_PATHS=(
  "${SCRIPT_DIR}/agentenv-streaming-dense-export.patch"
  "${SCRIPT_DIR}/agentenv-pooled-delete.patch"
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
  cargo test --locked --release -p "${PACKAGE}" --lib protocol::tests
  cargo build --locked --release -p "${PACKAGE}" --bin "${BINARY}"
)

readonly BUILT_BINARY="${SOURCE_DIR}/target/release/${BINARY}"
[[ -x "${BUILT_BINARY}" ]]
readonly BINARY_SHA256="$(sha256sum "${BUILT_BINARY}" | awk '{print $1}')"
readonly DENSE_PATCH_SHA256="$(sha256sum "${PATCH_PATHS[0]}" | awk '{print $1}')"
readonly POOLED_DELETE_PATCH_SHA256="$(sha256sum "${PATCH_PATHS[1]}" | awk '{print $1}')"
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
    "patches": [
        {
            "name": "$(basename "${PATCH_PATHS[0]}")",
            "sha256": "${DENSE_PATCH_SHA256}",
        },
        {
            "name": "$(basename "${PATCH_PATHS[1]}")",
            "sha256": "${POOLED_DELETE_PATCH_SHA256}",
        },
    ],
    "schema": 2,
}
Path("${OUTPUT_DIR}/${ARTIFACT_NAME}.manifest.json").write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

echo "${OUTPUT_DIR}/${ARTIFACT_NAME}"
