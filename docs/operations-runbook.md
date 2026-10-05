# Production runbook (UCloud)

Production runs in the DFM Pretraining project. Its deployment ID is
`hetzner-sandboxes-prod`, kept from Hetzner because state and labels carry it.

| Part | Where | Durable state |
| --- | --- | --- |
| Gateway | job 12412561 (`cpu-amd-zen5-8-vcpu`), 10.36.101.16 | Local disk: PostgreSQL 18 and `/var/lib/ucloud-sandboxes/state` |
| Store node | job 12412562, 10.36.103.152 | Local disk: chunk index and replica |
| Workers and builders | autoscaled (`cpu-amd-zen5-64-vcpu` / `-16-vcpu`) | None |
| Registry | `/work/data/ucloud-sandboxes-prod/registry` | Project drive |
| Chunk store | Hetzner S3 `ucloud-sandboxes-prod-20260926`, `production/chunks` | Permanent |

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

`scripts/install_ucloud_observability_stack.sh --private-bind-ip 10.36.101.16` runs:
- an OTLP collector on the private address;
- Tempo for traces (72 h);
- VictoriaMetrics for metrics (14 d);
- Grafana.

Data lives on the gateway's disk. Telemetry is on: the deployment's `telemetry.endpoint` is
`http://10.36.101.16:4318`, and trace sampling is 0.1. Workers export from
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
1. Create a VM like job 12412562: `ucloud-sandboxes/store` label, private network, 2,000 GB disk.
2. Put the newest chunk-index snapshot at `/var/lib/ucloud-chunk-index/index.sqlite` (owner `ucloud`, 0600).
3. Run `init-vm <job> --role store` from the gateway, with the current release's sandbox bundle and `/etc/ucloud-sandboxes/s3.env` loaded.

The replica refills from S3 by itself, in under 2 h for 331 GB. If the new
address differs, update `chunk_store.index_url`, `index_listen`, and
`store_node.listen` and `url`, then restart the gateway services.

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
5. If the private address changed:
   - run `scripts/rehome_registry_host.py --from <old>:5000 --to <new>:5000 --execute`;
   - update `gateway_private_host`, `buildx_cache_ref`, `direct_network_allow_tcp` and `network_relays` in `deployment.json`.
6. `python -m ucloud_sandboxes.systemd gateway-reconcile`.
7. Then `install_ucloud_ops.sh` and the observability stack.

Work lost: at most the last hour of gateway state. Running sandboxes'
routes are restored from PostgreSQL. Workers re-register by heartbeat.
