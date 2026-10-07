#!/usr/bin/env bash
# Build the verifiers harness toolkit image (docs/toolkit-layers.md).
#   build.sh VERIFIERS_CHECKOUT IMAGE_REF [HARNESS...]
# HARNESS defaults to "bash null": each is verifiers/v1/harnesses/<name>/program.py,
# copied byte for byte (verifiers names its prepared script by that file's sha256).
# Publish it with publish-environment --environment-allow-path opt/ucloud/toolkits/vf-harness,
# then register it with toolkit-register.
set -euo pipefail
[[ $# -ge 2 ]] || { echo "usage: $0 VERIFIERS_CHECKOUT IMAGE_REF [HARNESS...]" >&2; exit 2; }
readonly CHECKOUT="$(cd "$1" && pwd -P)" IMAGE_REF="$2"
shift 2
[[ $# -gt 0 ]] || set -- bash null
readonly HARNESSES=("$@")
readonly UV_VERSION=0.12.23
readonly UV_SHA256=$(curl -LsSf "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-x86_64-unknown-linux-musl.tar.gz.sha256" | cut -d' ' -f1)
[[ "$UV_SHA256" =~ ^[0-9a-f]{64}$ ]] || { echo "could not read the uv release checksum" >&2; exit 1; }
commit=$(git -C "$CHECKOUT" rev-parse HEAD)
[[ -z "$(git -C "$CHECKOUT" status --porcelain -- verifiers/v1/harnesses)" ]] || {
    echo "the verifiers harnesses have uncommitted changes" >&2; exit 1; }
context=$(mktemp -d -t vf-harness-toolkit.XXXXXXXX)
cleanup() { [[ "$context" == "${TMPDIR:-/tmp}"/vf-harness-toolkit.* && -d "$context" ]] && rm -r -- "$context"; }
trap cleanup EXIT
mkdir "$context/programs"
for harness in "${HARNESSES[@]}"; do
    cp -- "$CHECKOUT/verifiers/v1/harnesses/$harness/program.py" "$context/programs/$harness.py"
done
cp -- "$(dirname "$0")/Dockerfile" "$context/Dockerfile"
docker build --build-arg UV_VERSION="$UV_VERSION" --build-arg UV_SHA256="$UV_SHA256" \
    --build-arg VERIFIERS_COMMIT="$commit" -t "$IMAGE_REF" "$context"
echo "built $IMAGE_REF from verifiers $commit (${HARNESSES[*]})"
