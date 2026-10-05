#!/usr/bin/env bash
# Install the control plane's backups and health watch on a UCloud gateway.
#
#   install_ucloud_ops.sh --backups <dir on /work/data> [--public-gateway URL] [--public-relay URL]
#
# Backups (scripts/backup_gateway_state.py): the gateway hourly, keeping 48;
# the store node's chunk index every 6 hours, keeping 8. The watch
# (scripts/ops_watch.py) runs every minute; put ALERT_WEBHOOK_URL=<Slack-
# compatible webhook> in /etc/ucloud-sandboxes/alerts.env to receive alerts.
set -euo pipefail
backups=""
public_gateway=""
public_relay=""
while (($#)); do
  case "$1" in
    --backups) backups="${2:-}"; shift 2 ;;
    --public-gateway) public_gateway="${2:-}"; shift 2 ;;
    --public-relay) public_relay="${2:-}"; shift 2 ;;
    *) echo "usage: $0 --backups <dir> [--public-gateway URL] [--public-relay URL]" >&2; exit 2 ;;
  esac
done
[[ "$EUID" -eq 0 ]] || { echo "run as root" >&2; exit 2; }
case "$backups" in
  /work/*) ;;
  *) echo "--backups must be on durable UCloud storage (/work/...), not the gateway's own disk" >&2; exit 2 ;;
esac
source_dir="$(cd "$(dirname "$0")" && pwd)"
libexec=/usr/local/libexec/ucloud-sandboxes
python=/work/ucloud-sandboxes/gateway-venv/bin/python
install -d -m 0755 "$libexec"
install -m 0755 "$source_dir/backup_gateway_state.py" "$source_dir/ops_watch.py" "$libexec/"
install -d -m 0700 -o root -g root "$backups" "$backups/gateway" "$backups/chunk-index"
[[ -e /etc/ucloud-sandboxes/alerts.env ]] || install -m 0600 /dev/null /etc/ucloud-sandboxes/alerts.env

unit() {  # name, description, ExecStart, timer spec
  cat >"/etc/systemd/system/$1.service" <<EOF
[Unit]
Description=$2
RequiresMountsFor=/work/data
After=postgresql.service

[Service]
Type=oneshot
Nice=10
IOSchedulingClass=idle
ExecStart=$3
EOF
  cat >"/etc/systemd/system/$1.timer" <<EOF
[Unit]
Description=$2 (timer)

[Timer]
$4
Persistent=true

[Install]
WantedBy=timers.target
EOF
}
unit ucloud-sandbox-backup-gateway "Back up gateway PostgreSQL and state" \
  "$python $libexec/backup_gateway_state.py gateway --dest $backups/gateway --keep 48" \
  "OnCalendar=hourly
RandomizedDelaySec=300"
unit ucloud-sandbox-backup-chunk-index "Back up the store node's chunk index" \
  "$python $libexec/backup_gateway_state.py chunk-index --dest $backups/chunk-index --keep 8" \
  "OnCalendar=00/6:20"
unit ucloud-sandbox-ops-watch "Control plane health watch" \
  "$python $libexec/ops_watch.py --backups $backups --public-gateway '$public_gateway' --public-relay '$public_relay'" \
  "OnBootSec=2min
OnUnitActiveSec=1min
AccuracySec=5s"
systemctl daemon-reload
systemctl enable --now ucloud-sandbox-backup-gateway.timer ucloud-sandbox-backup-chunk-index.timer \
  ucloud-sandbox-ops-watch.timer
systemctl list-timers --no-pager 'ucloud-sandbox-backup-*' 'ucloud-sandbox-ops-watch*'
