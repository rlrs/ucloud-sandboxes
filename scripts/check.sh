#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"
check_root=$(mktemp -d "${TMPDIR:-/tmp}/ucloud-sandboxes-check.XXXXXX")
trap 'rm -rf "$check_root"' EXIT
root_project_venv="$check_root/root-project-venv"
sdk_project_venv="$check_root/sdk-project-venv"

UV_PROJECT_ENVIRONMENT="$root_project_venv" uv sync --locked
UV_PROJECT_ENVIRONMENT="$root_project_venv" \
  uv run ruff check ucloud_sandboxes tests scripts

for script in scripts/*.sh; do
  bash -n "$script"
done
if command -v shellcheck >/dev/null 2>&1; then
  shellcheck --severity=warning scripts/*.sh
elif [ "${UCLOUD_CHECK_ALLOW_MISSING_SHELLCHECK:-0}" = 1 ]; then
  echo "shellcheck is unavailable; explicitly skipping shell lint" >&2
else
  echo "shellcheck is required (set UCLOUD_CHECK_ALLOW_MISSING_SHELLCHECK=1 to opt out)" >&2
  exit 1
fi

UV_PROJECT_ENVIRONMENT="$root_project_venv" uv run python -m unittest
uv build --out-dir "$check_root/root-dist"
root_wheels=("$check_root"/root-dist/*.whl)
if [ "${#root_wheels[@]}" -ne 1 ] || [ ! -f "${root_wheels[0]}" ]; then
  echo "expected exactly one root wheel" >&2
  exit 1
fi
uv venv "$check_root/root-venv"
uv pip install --python "$check_root/root-venv/bin/python" "${root_wheels[0]}"
(
  cd "$check_root"
  "$check_root/root-venv/bin/python" "$repo_root/scripts/verify_installed_wheel.py"
)

if command -v go >/dev/null 2>&1; then
  if [ "$(uname -s)" = Darwin ]; then
    (
      cd runtime/managed_process
      go test -ldflags=-linkmode=external ./...
    )
  else
    (
      cd runtime/managed_process
      go test ./...
    )
  fi
else
  echo "Go is required for the managed-process contract tests" >&2
  exit 1
fi

sdk_root="$repo_root/ucloud-sandboxes-sdk"
if [ -f "$sdk_root/pyproject.toml" ]; then
  (
    cd "$sdk_root"
    UV_PROJECT_ENVIRONMENT="$sdk_project_venv" uv sync --locked --no-dev
    UV_PROJECT_ENVIRONMENT="$sdk_project_venv" \
      uv run --no-dev python -c "import ucloud_sandboxes_sdk"
    uv build --out-dir "$check_root/sdk-dist"
    sdk_wheels=("$check_root"/sdk-dist/*.whl)
    if [ "${#sdk_wheels[@]}" -ne 1 ] || [ ! -f "${sdk_wheels[0]}" ]; then
      echo "expected exactly one SDK wheel" >&2
      exit 1
    fi
    uv venv "$check_root/sdk-venv"
    uv pip install --python "$check_root/sdk-venv/bin/python" "${sdk_wheels[0]}"
    (
      cd "$check_root"
      "$check_root/sdk-venv/bin/python" \
        "$repo_root/scripts/verify_installed_sdk_wheel.py"
    )
    UV_PROJECT_ENVIRONMENT="$sdk_project_venv" uv sync --locked --all-extras
    "$root_project_venv/bin/ruff" check \
      --config "$repo_root/pyproject.toml" src tests
    UV_PROJECT_ENVIRONMENT="$sdk_project_venv" \
      uv run --all-extras python -m unittest
  )
elif [ "${UCLOUD_CHECK_ALLOW_MISSING_SDK:-0}" = 1 ]; then
  echo "SDK peer checkout absent; explicitly skipping its suite" >&2
else
  echo "SDK peer checkout is required (set UCLOUD_CHECK_ALLOW_MISSING_SDK=1 to opt out)" >&2
  exit 1
fi
