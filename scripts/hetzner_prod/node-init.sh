#!/usr/bin/env bash
# Run VM init for one Hetzner server id from the gateway (sandbox role).
cd /work/ucloud-sandboxes
set -a; . /etc/ucloud-sandboxes/hetzner.env; set +a
exec /work/ucloud-sandboxes/gateway-venv/bin/ucloud-sandboxes init-vm "$1" \
  --config /etc/ucloud-sandboxes/deployment.json --role sandbox \
  --package-spec /work/ucloud-sandboxes/release/sandbox-node-package.tar.gz \
  --ssh-private-key-file /var/lib/ucloud-sandboxes/state/ssh/gateway-init --execute --output json
