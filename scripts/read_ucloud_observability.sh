#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: read_ucloud_observability.sh <gateway-job-id> \
  [--window <duration>] [--rate-window <duration>] \
  [--trace-limit <0-20>] [--compact]

Resolves the gateway's current SSH port with the active UCloud project and
prints the structured production diagnostics report from that gateway.
EOF
}

if (($# < 1)); then
  usage
  exit 2
fi
gateway_job_id="$1"
shift
if [[ ! "$gateway_job_id" =~ ^[0-9]+$ ]]; then
  echo "gateway job id must contain only digits" >&2
  exit 2
fi

remote_args=()
while (($#)); do
  case "$1" in
    --window|--rate-window)
      if [[ ! "${2:-}" =~ ^[1-9][0-9]*[smhd]$ ]]; then
        echo "$1 must be a positive duration such as 5m or 1h" >&2
        exit 2
      fi
      remote_args+=("$1" "$2")
      shift 2
      ;;
    --trace-limit)
      if [[ ! "${2:-}" =~ ^([0-9]|1[0-9]|20)$ ]]; then
        echo "--trace-limit must be between 0 and 20" >&2
        exit 2
      fi
      remote_args+=("$1" "$2")
      shift 2
      ;;
    --compact)
      remote_args+=("$1")
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

ssh_resolution="$(ucloud jobs ssh "$gateway_job_id" --print-only)"
ssh_line="$(printf '%s\n' "$ssh_resolution" | tail -n 1)"
if [[ ! "$ssh_line" =~ ^ssh\ ucloud@ssh\.cloud\.sdu\.dk\ -p\ ([0-9]+)$ ]]; then
  echo "UCloud returned an unexpected SSH command" >&2
  exit 1
fi
ssh_port="${BASH_REMATCH[1]}"
known_hosts="${TMPDIR:-/tmp}/ucloud-observability-known-hosts"

quoted_args=""
if ((${#remote_args[@]})); then
  printf -v quoted_args ' %q' "${remote_args[@]}"
fi
remote_command="sudo /usr/local/bin/ucloud-observability-report${quoted_args}"
exec ssh \
  -o BatchMode=yes \
  -o StrictHostKeyChecking=accept-new \
  -o "UserKnownHostsFile=$known_hosts" \
  -p "$ssh_port" \
  ucloud@ssh.cloud.sdu.dk \
  "$remote_command"
