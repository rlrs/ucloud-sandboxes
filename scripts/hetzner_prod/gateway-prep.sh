#!/usr/bin/env bash
# PostgreSQL, service user, venv and database schemas before install_hetzner_gateway.sh.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get -o DPkg::Lock::Timeout=600 update -q >/dev/null
apt-get -o DPkg::Lock::Timeout=600 install -y -q postgresql python3-venv >/dev/null
id ucloud >/dev/null 2>&1 || useradd --system --create-home --home-dir /var/lib/ucloud-sandboxes --shell /usr/sbin/nologin ucloud
PGV=$(ls /etc/postgresql | sort -n | tail -1)
cat > /etc/postgresql/$PGV/main/conf.d/ucloud.conf <<CONF
max_connections = 200
shared_buffers = 1GB
effective_cache_size = 4GB
wal_compression = on
checkpoint_timeout = 15min
max_wal_size = 4GB
CONF
systemctl restart postgresql
sudo -u postgres psql -tAc "select 1 from pg_roles where rolname='ucloud'" | grep -q 1 || sudo -u postgres createuser ucloud
sudo -u postgres psql -tAc "select 1 from pg_database where datname='ucloud'" | grep -q 1 || sudo -u postgres createdb -O ucloud ucloud
install -d -m 0755 /etc/ucloud-sandboxes
printf 'host=/var/run/postgresql dbname=ucloud user=ucloud\n' > /etc/ucloud-sandboxes/postgres.dsn
chown ucloud:ucloud /etc/ucloud-sandboxes/postgres.dsn; chmod 0600 /etc/ucloud-sandboxes/postgres.dsn
V=/work/ucloud-sandboxes/gateway-venv
install -d -m 0755 /work/ucloud-sandboxes
[ -x $V/bin/python ] || python3 -m venv $V
mkdir -p /tmp/wheel
wheel_name=$(python3 - <<'PY'
from email.parser import BytesParser
import zipfile
with zipfile.ZipFile('/tmp/ucloud-sandboxes.whl') as wheel:
    name = next(n for n in wheel.namelist() if n.endswith('.dist-info/METADATA'))
    metadata = BytesParser().parsebytes(wheel.read(name))
    print(f"{metadata['Name'].replace('-', '_')}-{metadata['Version']}-py3-none-any.whl")
PY
)
cp /tmp/ucloud-sandboxes.whl "/tmp/wheel/$wheel_name"
$V/bin/pip install -q "/tmp/wheel/$wheel_name[postgres]"
install -d -m0700 -o ucloud -g ucloud /var/lib/ucloud-sandboxes/state
sudo -u ucloud $V/bin/python -m ucloud_sandboxes.shared_control migrate --dsn-file /etc/ucloud-sandboxes/postgres.dsn --deployment-id hetzner-sandboxes-prod --schema ucloud_shared_prod
sudo -u ucloud $V/bin/python -c 'from pathlib import Path; from ucloud_sandboxes.routing import RoutingStore; RoutingStore(Path("/var/lib/ucloud-sandboxes/state/routes.sqlite"))'
sudo -u ucloud $V/bin/python -m ucloud_sandboxes.shared_control.routing_cutover --routing-file /var/lib/ucloud-sandboxes/state/routes.sqlite --dsn-file /etc/ucloud-sandboxes/postgres.dsn --schema ucloud_routing_prod
sudo -u ucloud $V/bin/ucloud-sandboxes provision-environment-key --directory /var/lib/ucloud-sandboxes/state/environment-producer
echo "postgres=$PGV prep-done"
