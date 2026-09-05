#!/usr/bin/env bash
set -euo pipefail

readonly EXPECTED_COMMIT="50e1502a95d36ad2faf2c7ef33b8bf21fe975293"
readonly EXPECTED_PATCH_SERIES_SHA256="44beb70f08a1eca01dce6077cf8d4bb8e2cd4b093ef7055b806039125cd21573"
readonly EXPECTED_PATCHED_FILES_SHA256="ea7dcf91cd27683702b616a10a4124dd0e8e146afca51a3519d8b76ca6667144"
readonly BUILD_CONFIG="opt"
readonly -a PATCH_NAMES=("20260817/0001-ucloud-hibernation.patch")
readonly -a EXPECTED_PATCH_SHA256S=("bed13a2a1ef790a61a7a09d3a70511a15a7504d1ef2342edc672b4203950f5e6")

usage() {
  echo "usage: $0 GVISOR_CHECKOUT OUTPUT_DIRECTORY" >&2
  exit 2
}

[[ $# -eq 2 ]] || usage
SOURCE_DIR="$(cd "$1" && pwd -P)"
readonly SOURCE_DIR
mkdir -p "$2"
OUTPUT_DIR="$(cd "$2" && pwd -P)"
readonly OUTPUT_DIR
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly SCRIPT_DIR

[[ "$(git -C "${SOURCE_DIR}" rev-parse HEAD)" == "${EXPECTED_COMMIT}" ]] || {
  echo "gVisor checkout is not at ${EXPECTED_COMMIT}" >&2
  exit 1
}
CHECK_DIR="$(mktemp -d)"
readonly CHECK_DIR
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
PATCH_SERIES_SHA256="$(
  sha256sum "${CHECK_DIR}/patch-series" | awk '{print $1}'
)"
readonly PATCH_SERIES_SHA256
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
PATCHED_FILES_SHA256="$(
  cd "${SOURCE_DIR}"
  while IFS= read -r path; do
    sha256sum "${path}"
  done < "${CHECK_DIR}/expected-paths" |
    sha256sum |
    awk '{print $1}'
)"
readonly PATCHED_FILES_SHA256
[[ "${PATCHED_FILES_SHA256}" == "${EXPECTED_PATCHED_FILES_SHA256}" ]] || {
  echo "patched checkout contains changes outside the pinned patch" >&2
  exit 1
}
command -v bazel >/dev/null
BAZEL_VERSION="$(cd "${SOURCE_DIR}" && bazel version --gnu_format)"
readonly BAZEL_VERSION
[[ "${BAZEL_VERSION}" == "bazel $(cat "${SOURCE_DIR}/.bazelversion")" ]] || {
  echo "Bazel version does not match the upstream pin" >&2
  exit 1
}

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
  bazel build "-c" "${BUILD_CONFIG}" "//:release"
)

readonly BUILT_RELEASE="${SOURCE_DIR}/bazel-bin/release"
python3 - "${BUILT_RELEASE}" "${OUTPUT_DIR}" "${CHECK_DIR}/patch-series" \
  "${BAZEL_VERSION}" "${BUILD_CONFIG}" "${EXPECTED_COMMIT}" \
  "${PATCH_SERIES_SHA256}" "${PATCHED_FILES_SHA256}" <<'PY'
import hashlib
import json
from pathlib import Path
import platform
import shutil
import tempfile
import sys

release_path, output_path, series_path, bazel_version, build_config, commit, series_sha256, files_sha256 = sys.argv[1:]
release = Path(release_path)
names = ["runsc", "gvisor-bin/checkpointgofer", "gvisor-bin/gvisor-sentry-prewarmer",
         "gvisor-bin/gvisor_sentry", "gvisor-bin/runsc-metric-server"]
files = {}
for name in names:
    source = release / name
    value = hashlib.sha256()
    with source.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    digest = value.hexdigest()
    files[name] = {"sha256": digest, "size": source.stat().st_size}
identity = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
artifact = "gvisor-hibernate-" + identity
payload = {
    "artifact": artifact,
    "bazel_version": bazel_version,
    "build_config": build_config,
    "gvisor_commit": commit,
    "host_architecture": platform.machine(),
    "patch_series_sha256": series_sha256,
    "patches": [
        {"name": name, "sha256": digest}
        for digest, name in (
            line.split("  ", 1)
            for line in Path(series_path).read_text(encoding="ascii").splitlines()
        )
    ],
    "patched_files_sha256": files_sha256,
    "runsc_sha256": files["runsc"]["sha256"],
    "files": files,
    "schema": 2,
}
output = Path(output_path) / artifact
# Never combine companions from different builds in a shared gvisor-bin directory.
if output.exists():
    raise SystemExit(f"output distribution already exists: {output}")
with tempfile.TemporaryDirectory(prefix=".gvisor-build-", dir=output.parent) as temporary:
    staging = Path(temporary) / artifact
    for name in names:
        destination = staging / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(release / name, destination)
        destination.chmod(0o755)
    (staging / "build-manifest.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    staging.rename(output)
print(output / "runsc")
PY
