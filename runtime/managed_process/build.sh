#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
output=${1:-"$script_dir/ucloud-sandbox-init-linux-amd64"}

cd "$script_dir"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -trimpath \
  -ldflags='-s -w -buildid=' \
  -o "$output" \
  .
sha256sum "$output"
