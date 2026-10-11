# Production runbook (UCloud)

Production runs in the DFM Pretraining project. Its deployment ID is
`hetzner-sandboxes-prod`, kept from Hetzner because state and labels carry it.

| Part | Where | Durable state |
| --- | --- | --- |
| Gateway | job 12412561 (`cpu-amd-zen5-8-vcpu`), `sandbox-gateway-gateway-1` | Local disk: PostgreSQL 18 and `/var/lib/ucloud-sandboxes/state` |
| Store node | job 12414961, `sandboxes-store-1` (since 2026-10-07; was job 12412562) | Local disk: chunk index and replica |
| Workers and builders | autoscaled (`cpu-amd-zen5-64-vcpu` / `-16-vcpu`) | None |
| Registry | `/work/data/ucloud-sandboxes-prod/registry` | Project drive |
| Chunk store | Hetzner S3 `ucloud-sandboxes-prod-20260926`, `production/chunks` | Permanent |

**Addresses.** Everything names the gateway and store node by their DNS
names, which UCloud's resolver answers on every VM of the project
(`<hostname>.dfm-sandboxes.ucloud-apps.svc.cluster.local`, short names through
the search domain). Their private IPv4 addresses change when UCloud restarts
the VMs: on 2026-10-11 at 04:24 UTC both restarted, the gateway moved from
10.36.101.16 to 10.36.102.75 and the store node from 10.36.78.225 to
10.36.76.210, and every literal address in the configuration stopped working
(0.9.70 replaced them). Services bind `0.0.0.0`; the VMs' only other
interfaces are loopback and Docker's. Sandboxes reach the relay at its virtual
address 198.19.38.106 ([network-policy.md](network-policy.md)).

The gateway's and store node's local disks end with their jobs. Everything on
them is backed up to the project drive. The S3 bucket is the permanent copy of
every environment.

## Backups

`scripts/backup_gateway_state.py`, installed by `scripts/install_ucloud_ops.sh`.
Snapshots go to `/work/data/ucloud-sandboxes-prod/backups/`.

| Timer | Contents | Retention |
| --- | --- | --- |
| `ucloud-sandbox-backup-gateway` (hourly) | `postgres.dump` (pg_dump custom), `state/` (SQLite online copies, tokens, keys, provider session), `etc/`, chunk-index tokens, `manifest.json` with digests | 48 |
| `ucloud-sandbox-backup-chunk-index` (every 6 h) | The store node's index, copied online and `quick_check`ed there, gzip | 8 |

The snapshots contain credentials. The directories are mode 0700, but every
member of the project can read the drive, so treat project membership as
access to production.

Verify a gateway snapshot by unpacking it and checking `manifest.json`'s digests,
`PRAGMA quick_check` on each copied database, and `pg_restore --list postgres.dump`.

## Health watch and alerts

`ucloud-sandbox-ops-watch.timer` runs `scripts/ops_watch.py` every minute. It checks:
- the services and their health endpoints;
- the registry and PostgreSQL;
- gateway pressure: CPU some, memory full and IO full, as 60 s averages;
- the root disk, and whether the project drive is writable;
- store node and chunk index health, including the replica's verify failures;
- backup age;
- node init failures in the last 15 min;
- the two public links.

Current results are in `/var/lib/ucloud-sandboxes/ops-watch.json` (`failing`
lists the failing checks). A check alerts:
- after two consecutive failures;
- every 6 h while it stays failing;
- once on recovery.

Alerts go to the journal (`journalctl -u ucloud-sandbox-ops-watch`). Set
`ALERT_WEBHOOK_URL=` in `/etc/ucloud-sandboxes/alerts.env` (mode 0600) to also
POST them as `{"text": ...}` to a Slack-compatible webhook.

## Metrics and traces

`scripts/install_ucloud_observability_stack.sh --private-bind-ip 0.0.0.0 --endpoint-host sandbox-gateway-gateway-1` runs:
- an OTLP collector on the private network;
- Tempo for traces (72 h);
- VictoriaMetrics for metrics (14 d);
- Grafana.

Data lives on the gateway's disk. Telemetry is on: the deployment's `telemetry.endpoint` is
`http://sandbox-gateway-gateway-1:4318`, and trace sampling is 0.1. Workers export from
their next init.

To view the dashboards, open the tunnel, then go to
`http://127.0.0.1:3000/d/ucloud-platform-hot-paths`:

```bash
ssh -L 3000:127.0.0.1:3000 -p <gateway ssh port> ucloud@ssh.cloud.sdu.dk
```

## Releases and rollback

Release kits are in `/work/ucloud-sandboxes/release-<version>-<date>/`. The
live kit is also copied to `/work/data/ucloud-sandboxes-prod/releases/`.

To upgrade:
1. Run `gateway_upgrade_<version>.py check`. It needs an idle fleet and no queued builds.
2. Stop `ucloud-sandbox-registry-prune.timer`.
3. Run `apply`. A failed health check restores the previous venv and config automatically.
4. Start the timer again.

`rollback` restores the last apply's backup. Workers take the new bundle at
their next init. Drain or wait for scale-down to replace running ones.

Health checks use the UCloud controller copy
(`/work/ucloud-sandboxes/ucloud-controller-20261005/`). It checks the gateway
and relay at their private addresses until SDU routes the public links again
(reported 2026-10-05). Switch `PUBLIC_BASE` and `PUBLIC_RELAY_BASE` back to the
links then.

## When something is lost

**A worker or builder.** The autoscaler replaces it. Sandboxes parked to the
registry wake elsewhere. Running ones on a lost worker are lost (see
[node-failure-semantics.md](node-failure-semantics.md)).

**The store node.** Workers' reads fall back to S3 through the index only while
the index is up, and the index runs on the store node. To replace it:
1. Create a VM like the current one: `submit-vm --role node --hostname sandboxes-store-1 --no-public-link --product-id cpu-amd-zen5-8-vcpu --disk-gb 2000 --label ucloud-sandboxes/store=true`, then, in the payload, drop `timeAllocation` (1 h by default) and the `ucloud-sandboxes/node` label (the autoscaler must not own it) before submitting.
2. Put the newest chunk-index snapshot at `/var/lib/ucloud-chunk-index/index.sqlite` (owner `ucloud`, 0600). From the gateway, `gunzip -c` the snapshot into `ssh -i /var/lib/ucloud-sandboxes/state/ssh/gateway-init ucloud@sandboxes-store-1`, then `PRAGMA quick_check`.
3. Run `init-vm <job> --role store` from the gateway, with the current release's sandbox bundle and `/etc/ucloud-sandboxes/s3.env` loaded.

The replica refills from S3 by itself, in under 2 h for 331 GB. Keep the
hostname `sandboxes-store-1`: the configuration names it, so a new address
needs no change.

Done on 2026-10-07, when job 12412562 stopped at 12:00 UTC from outside the
gateway: the index restore took 33 s and the store init 22 s.

**The gateway.**
1. Create a VM like job 12412561, with the private network, the project drive mounted, and the two public links.
2. `apt-get install ca-certificates curl docker.io nftables openssl python3-venv postgresql-18`.
3. From the newest gateway snapshot, stage the installer's inputs:
   - `state.tar` from `state/` (`tar -cf state.tar state`);
   - `ucloud.pgdump` from `postgres.dump`;
   - `deployment.json`, `postgres.dsn` and `s3.env` from `etc/`;
   - `ucloud-session.json` from `state/`;
   - `release/` with the live kit's bundles;
   - the kit's wheel.
4. Run `scripts/install_ucloud_gateway.sh`. Put the chunk-index tokens in `/var/lib/ucloud-chunk-index/`.
5. Keep the hostname `sandbox-gateway-gateway-1`: then nothing else changes. With another hostname:
   - run `scripts/rehome_registry_host.py --from <old>:5000 --to <new>:5000 --execute`;
   - update `gateway_private_host`, `buildx_cache_ref`, `direct_network_allow_tcp`, `network_relays` and `telemetry.endpoint` in `deployment.json`;
   - the relay's virtual address follows its name (`default`), not its host, so clients keep theirs.
6. `python -m ucloud_sandboxes.systemd gateway-reconcile`.
7. Then `install_ucloud_ops.sh` and the observability stack.

Work lost: at most the last hour of gateway state. Running sandboxes'
routes are restored from PostgreSQL. Workers re-register by heartbeat.
