#!/usr/bin/env bash
# Install a gateway on a UCloud VM from a backup (or a moved deployment's state).
# Run as root after apt installs ca-certificates curl docker.io nftables openssl
# python3-venv postgresql-18. Inputs staged by the operator in $S (mode 0700):
#   ucloud_sandboxes-<version>-py3-none-any.whl  deployment.json  state.tar
#   postgres.dsn  ucloud.pgdump  s3.env  ucloud-session.json
#   release/ (the node bundles node_package_root names)
# A gateway backup (scripts/backup_gateway_state.py) holds all but the wheel and
# release: postgres.dump, state/, etc/. Registry data must already be at
# registry_store.data_root (it lives on /work/data). If the gateway's private
# address changed, run scripts/rehome_registry_host.py before starting services.
# Ends before gateway-reconcile, which starts the services. Never prints secrets.
set -euo pipefail
S=${STAGE:-/home/ucloud/stage}
wheel="$(ls "$S"/ucloud_sandboxes-*-py3-none-any.whl)"
[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 2; }
for f in deployment.json state.tar postgres.dsn \
         ucloud.pgdump s3.env ucloud-session.json release/sandbox-node-package.tar.gz \
         release/builder-node-package.tar.gz; do
  [[ -s "$S/$f" ]] || { echo "missing $S/$f" >&2; exit 2; }
done
mountpoint -q /work/data || { echo "/work/data is not mounted" >&2; exit 1; }

# OS packages are updated only through explicit maintenance.
cat > /etc/apt/apt.conf.d/99zz-ucloud-no-unattended-upgrades <<'EOF'
APT::Periodic::Enable "0";
APT::Periodic::Update-Package-Lists "0";
APT::Periodic::Unattended-Upgrade "0";
EOF
systemctl disable --now apt-daily.timer apt-daily-upgrade.timer || true
systemctl mask apt-daily.timer apt-daily-upgrade.timer apt-daily.service apt-daily-upgrade.service unattended-upgrades.service
systemctl enable --now docker.service

# PostgreSQL 18: same tuning as Hetzner, role and database from the DSN, then the dump.
cat > /etc/postgresql/18/main/conf.d/ucloud.conf <<'EOF'
max_connections = 200
shared_buffers = 1GB
effective_cache_size = 4GB
wal_compression = on
checkpoint_timeout = 15min
max_wal_size = 4GB
EOF
systemctl restart postgresql
# The DSN is the local socket with peer auth (host=/var/run/postgresql dbname=ucloud user=ucloud).
grep -qx 'host=/var/run/postgresql dbname=ucloud user=ucloud' "$S/postgres.dsn" || { echo "unexpected DSN shape" >&2; exit 1; }
db=ucloud
if runuser -u postgres -- psql -qAt -c "select 1 from pg_database where datname = 'ucloud'" | grep -q 1; then
  echo "database ucloud already exists; refusing to restore over it" >&2; exit 1
fi
runuser -u postgres -- psql -v ON_ERROR_STOP=1 -q -c 'CREATE ROLE ucloud LOGIN' -c 'CREATE DATABASE ucloud OWNER ucloud'
install -m 0644 "$S/ucloud.pgdump" /tmp/ucloud.pgdump
runuser -u postgres -- pg_restore --exit-on-error -d "$db" /tmp/ucloud.pgdump
rm -f /tmp/ucloud.pgdump

# Gateway state restored from Hetzner; services run as the VM's ucloud user.
data_root=/var/lib/ucloud-sandboxes/state
[[ ! -e "$data_root" ]] || { echo "$data_root already exists" >&2; exit 1; }
install -d -m 0750 -o ucloud -g ucloud /var/lib/ucloud-sandboxes
tar -C /var/lib/ucloud-sandboxes --no-same-owner -xf "$S/state.tar"
install -m 0600 "$S/ucloud-session.json" "$data_root/ucloud-session.json"
chown -R ucloud:ucloud /var/lib/ucloud-sandboxes

install -d -m 0755 /etc/ucloud-sandboxes
install -m 0644 "$S/deployment.json" /etc/ucloud-sandboxes/deployment.json
install -m 0600 -o ucloud -g ucloud "$S/postgres.dsn" /etc/ucloud-sandboxes/postgres.dsn
install -m 0600 "$S/s3.env" /etc/ucloud-sandboxes/s3.env

install_root=/work/ucloud-sandboxes
release_dir="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["node_package_root"])' "$S/deployment.json")"
venv_dir="$install_root/gateway-venv"
install -d -m 0755 "$install_root" "$release_dir"
cp -a "$S/release/." "$release_dir/"
chown -R root:root "$release_dir"
chmod -R u+rwX,go+rX,go-w "$release_dir"

registry_root="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["registry_store"]["data_root"])' "$S/deployment.json")"
install -d -m 0755 "$registry_root"

python3 -m venv "$venv_dir"
# bin/python -m pip, as upgrades install: scripts keep the same shebang across releases.
"$venv_dir/bin/python" -m pip install -q --disable-pip-version-check --force-reinstall "${wheel}[postgres]"

systemd_source="$("$venv_dir/bin/python" -c 'from importlib import resources; print(resources.files("ucloud_sandboxes").joinpath("systemd"))')"
for unit in \
  ucloud-sandbox-autoscaler.service ucloud-sandbox-gateway.service ucloud-sandbox-placement.service \
  ucloud-sandbox-registry-gc.service ucloud-sandbox-registry-gc.timer \
  ucloud-sandbox-registry-pressure.service ucloud-sandbox-registry-pressure.timer \
  ucloud-sandbox-snapshot-gc.service ucloud-sandbox-snapshot-gc.timer \
  ucloud-sandbox-chunk-index.service \
  ucloud-sandbox-registry-prune.service ucloud-sandbox-registry-prune.timer \
  ucloud-sandbox-registry.service ucloud-sandbox-relay.service \
  ucloud-sandbox-upstream-mirror@.service \
  ucloud-sandbox-upstream-mirror-trim.service ucloud-sandbox-upstream-mirror-trim.timer; do
  install -m 0644 "$systemd_source/$unit" "/etc/systemd/system/$unit"
done
# The autoscaler publishes environments to the chunk store (Hetzner S3).
install -d -m 0755 /etc/systemd/system/ucloud-sandbox-autoscaler.service.d
cat > /etc/systemd/system/ucloud-sandbox-autoscaler.service.d/s3.conf <<'EOF'
[Service]
EnvironmentFile=/etc/ucloud-sandboxes/s3.env
EOF
# The registry lives on the project drive.
for unit in ucloud-sandbox-registry.service ucloud-sandbox-registry-gc.service \
            ucloud-sandbox-registry-pressure.service ucloud-sandbox-registry-prune.service \
            ucloud-sandbox-gateway.service ucloud-sandbox-autoscaler.service; do
  install -d -m 0755 "/etc/systemd/system/$unit.d"
  cat > "/etc/systemd/system/$unit.d/project-drive.conf" <<'EOF'
[Unit]
RequiresMountsFor=/work/data
EOF
done
systemctl daemon-reload
echo "installed; registry data must be in $registry_root before gateway-reconcile"
