#!/usr/bin/env bash
set -euo pipefail

readonly EXPECTED_COMMIT="9f653e577965df2ddd13875b5530cd2588661f1c"
readonly EXPECTED_PATCH_SERIES_SHA256="8fb36e2f8679db5d79d1ecd6c80ee236c32424c2a8ae2b93ffa9120efd436748"
readonly EXPECTED_PATCHED_FILES_SHA256="d6c13f78e38dbf2b9e5427340bc5bb39791499e5913c98506dca3257f851072f"
readonly BUILD_CONFIG="opt"
readonly -a PATCH_NAMES=(
  "0001-disk-backed-main-memory.patch"
  "0002-quota-owned-memory-directory.patch"
  "0003-two-phase-hibernation-capture.patch"
  "0004-restore-cpu-startup-burst.patch"
  "0005-restore-start-paused.patch"
)
readonly -a EXPECTED_PATCH_SHA256S=(
  "6bdf87cf565e96b0d65909a56f19a6a8790b10af0f973f22300d4b07bba9d554"
  "65acb8f572ab74e1ea6e3ebd4f29abd1a8e0b7bb354faafa9dbab47a2d75da5c"
  "495db595700dec88b770a868ed84b2d0b5fa1bf2bd2ab9f511e57816feffe3bd"
  "00e8dc2769edcf936ffec65647a87b43366ba0c6c36bbea33069e4e94cf9d264"
  "818a844362cfae61279e096bc27e9f34dd38187f606541c77bec57ffd057824a"
)

usage() {
  echo "usage: $0 GVISOR_CHECKOUT OUTPUT_DIRECTORY" >&2
  exit 2
}

[[ $# -eq 2 ]] || usage
readonly SOURCE_DIR="$(cd "$1" && pwd -P)"
mkdir -p "$2"
readonly OUTPUT_DIR="$(cd "$2" && pwd -P)"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

[[ "$(git -C "${SOURCE_DIR}" rev-parse HEAD)" == "${EXPECTED_COMMIT}" ]] || {
  echo "gVisor checkout is not at ${EXPECTED_COMMIT}" >&2
  exit 1
}
readonly CHECK_DIR="$(mktemp -d)"
trap 'rm -rf "${CHECK_DIR}"' EXIT
touch "${CHECK_DIR}/expected-paths-unsorted" "${CHECK_DIR}/patch-series"
for index in "${!PATCH_NAMES[@]}"; do
  patch_name="${PATCH_NAMES[$index]}"
  patch_path="${SCRIPT_DIR}/${patch_name}"
  patch_sha256="$(sha256sum "${patch_path}" | awk '{print $1}')"
  [[ "${patch_sha256}" == "${EXPECTED_PATCH_SHA256S[$index]}" ]] || {
    echo "${patch_name} digest does not match the pinned digest" >&2
    exit 1
  }
  printf '%s  %s\n' "${patch_sha256}" "${patch_name}" >> "${CHECK_DIR}/patch-series"
  git -C "${SOURCE_DIR}" apply --numstat "${patch_path}" |
    cut -f3 >> "${CHECK_DIR}/expected-paths-unsorted"
done
readonly PATCH_SERIES_SHA256="$(
  sha256sum "${CHECK_DIR}/patch-series" | awk '{print $1}'
)"
[[ "${PATCH_SERIES_SHA256}" == "${EXPECTED_PATCH_SERIES_SHA256}" ]] || {
  echo "hibernation patch series digest does not match the pinned digest" >&2
  exit 1
}
sort -u "${CHECK_DIR}/expected-paths-unsorted" > "${CHECK_DIR}/expected-paths"
git -C "${SOURCE_DIR}" status --porcelain=v1 --untracked-files=all |
  cut -c4- |
  sort > "${CHECK_DIR}/actual-paths"
if cmp -s "${CHECK_DIR}/expected-paths" "${CHECK_DIR}/actual-paths"; then
  echo "pinned hibernation patch series is already applied"
else
  [[ ! -s "${CHECK_DIR}/actual-paths" ]] || {
    echo "checkout is neither clean nor the exact patched tree" >&2
    exit 1
  }
  git -C "${SOURCE_DIR}" diff --cached --quiet
  for patch_name in "${PATCH_NAMES[@]}"; do
    patch_path="${SCRIPT_DIR}/${patch_name}"
    git -C "${SOURCE_DIR}" apply --check "${patch_path}"
    git -C "${SOURCE_DIR}" apply "${patch_path}"
  done
  git -C "${SOURCE_DIR}" status --porcelain=v1 --untracked-files=all |
    cut -c4- |
    sort > "${CHECK_DIR}/actual-paths"
fi
cmp "${CHECK_DIR}/expected-paths" "${CHECK_DIR}/actual-paths" || {
  echo "patched checkout contains paths outside the pinned patch" >&2
  exit 1
}
readonly PATCHED_FILES_SHA256="$(
  cd "${SOURCE_DIR}"
  while IFS= read -r path; do
    sha256sum "${path}"
  done < "${CHECK_DIR}/expected-paths" |
    sha256sum |
    awk '{print $1}'
)"
[[ "${PATCHED_FILES_SHA256}" == "${EXPECTED_PATCHED_FILES_SHA256}" ]] || {
  echo "patched checkout contains changes outside the pinned patch" >&2
  exit 1
}
command -v bazel >/dev/null
readonly BAZEL_VERSION="$(cd "${SOURCE_DIR}" && bazel --version)"

(
  cd "${SOURCE_DIR}"
  bazel test \
    "--test_filter=TestExternalBackingSaveRestore|TestExternalBackingRestoreRejectsWrongSize" \
    "//pkg/sentry/pgalloc:pgalloc_test"
  bazel test \
    "--test_filter=TestCreateMemoryFileWithDiskBacking" \
    "//runsc/boot:boot_test"
  bazel test \
    "--test_filter=TestRemoveCPUQuotaForStartup" \
    "//runsc/cmd:cmd_test"
  bazel test \
    "--test_filter=TestStartPausedStatusTransition" \
    "//runsc/container:container_test"
  bazel build "-c" "${BUILD_CONFIG}" "//runsc:runsc"
)

readonly BUILT_RUNSC="${SOURCE_DIR}/bazel-bin/runsc/runsc_/runsc"
[[ -x "${BUILT_RUNSC}" ]]
readonly RUNSC_SHA256="$(sha256sum "${BUILT_RUNSC}" | awk '{print $1}')"
readonly ARTIFACT_NAME="runsc-hibernate-${RUNSC_SHA256}"
install -m 0755 "${BUILT_RUNSC}" "${OUTPUT_DIR}/${ARTIFACT_NAME}"

python3 - "${OUTPUT_DIR}/${ARTIFACT_NAME}.manifest.json" <<PY
import json
from pathlib import Path
import platform

payload = {
    "artifact": "${ARTIFACT_NAME}",
    "bazel_version": "${BAZEL_VERSION}",
    "build_config": "${BUILD_CONFIG}",
    "gvisor_commit": "${EXPECTED_COMMIT}",
    "host_architecture": platform.machine(),
    "patch_series_sha256": "${PATCH_SERIES_SHA256}",
    "patches": [
        {"name": name, "sha256": digest}
        for digest, name in (
            line.split("  ", 1)
            for line in Path("${CHECK_DIR}/patch-series").read_text(
                encoding="ascii"
            ).splitlines()
        )
    ],
    "patched_files_sha256": "${PATCHED_FILES_SHA256}",
    "runsc_sha256": "${RUNSC_SHA256}",
    "schema": 1,
}
Path("${OUTPUT_DIR}/${ARTIFACT_NAME}.manifest.json").write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

echo "${OUTPUT_DIR}/${ARTIFACT_NAME}"
