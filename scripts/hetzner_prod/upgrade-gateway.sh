#!/bin/sh
# Upgrade the Hetzner gateway in place: upgrade-gateway.sh <version>
# Expects build/hetzner-prod/{ucloud_sandboxes-<version>-py3-none-any.whl,
# sandbox-node-package.tar.gz, builder-node-package.tar.gz, deployment.json}.
set -eu
v="$1"
s=$(cd "$(dirname "$0")" && pwd)
r=$(cd "$s/../.." && pwd)
d="$r/build/hetzner-prod"   # release inputs and deployment.json
H=root@77.42.92.27
S="$s/gscp"
"$S" "$d/ucloud_sandboxes-$v-py3-none-any.whl" "$H:/tmp/ucloud-sandboxes.whl"
"$S" "$d/deployment.json" "$H:/tmp/ucloud-sandboxes-deployment.json"
"$S" "$d/sandbox-node-package.tar.gz" "$H:/tmp/ucloud-sandboxes-sandbox-node-package.tar.gz"
"$S" "$d/builder-node-package.tar.gz" "$H:/tmp/ucloud-sandboxes-builder-node-package.tar.gz"
"$S" "$r/.hetzner/ssh/gateway-init" "$H:/tmp/ucloud-sandboxes-gateway-init"
"$S" "$r/.hetzner/ssh/gateway-init.pub" "$H:/tmp/ucloud-sandboxes-gateway-init.pub"
"$S" "$d/hetzner.env" "$H:/tmp/ucloud-sandboxes-hetzner.env"
"$S" "$r/scripts/configure_hetzner_sdk_ingress.sh" "$r/scripts/install_hetzner_gateway.sh" "$H:/tmp/"
"$S" "$s/gateway-prep.sh" "$H:/root/"
"$s/gw" 'chmod 600 /tmp/ucloud-sandboxes-hetzner.env /tmp/ucloud-sandboxes-gateway-init;
  [ -x /work/ucloud-sandboxes/gateway-venv/bin/python ] || bash /root/gateway-prep.sh > /root/gateway-prep.log 2>&1 || { echo prep-failed; tail -20 /root/gateway-prep.log; exit 1; };
  bash /tmp/install_hetzner_gateway.sh --public-ip 77.42.92.27 > /root/install-gateway.log 2>&1; echo installer_exit=$?;
  systemctl is-active ucloud-sandbox-gateway ucloud-sandbox-placement ucloud-sandbox-relay ucloud-sandbox-autoscaler | tr "\n" " "; echo;
  curl -s -m5 http://127.0.0.1:8090/healthz; echo'
