# Production relay PostgreSQL cutover — 2026-09-22

At 06:06:51 UTC, `live-ucloud-20260824a` switched its normal public model relay
(`https://app-sandboxes-relay-v2.cloud.sdu.dk`) to PostgreSQL. The gateway,
scheduler and worker journals retain their existing SQLite authorities. No SDK
or Verifiers update is required.

The relay was idle (`inflight=0`, `delivery_pending=0`), stopped, backed up and
imported before restart. All 2,156 active registrations and 11 retained responses
were imported. Registration tokens and metadata were compared with the source.
The database also contains one inactive registration retained to preserve an old
response's identity; this explains the 2,157 total rows in backup verification.
The SQLite source is durably fenced; do not remove its authority marker or start
an old SQLite backup after this cutover.

Public authenticated relay stats reported `backend: postgres`, 2,156 active
registrations, zero inflight requests and zero pending deliveries after restart.
Database crash recovery passed before cutover. A dump taken after cutover was
restored into a separate database and retained all 2,157 registration rows and
11 responses; the temporary restore database was then removed.

## Runtime and recovery

- Gateway job: `12379311`.
- Database service/container: `ucloud-relay-postgres`, PostgreSQL 17, bound only
  to `127.0.0.1:55440`; database/user `ucloud_relay`, schema `ucloud_shared`.
- Image ID: `sha256:f4c66b820c6f974249089d3d16d86a3698eae11e8746eb6644b2271031e91232`.
- Local ext4 data: `/var/lib/ucloud-sandboxes/live-ucloud-20260824a/postgres`.
- Private DSN: `/etc/ucloud-sandboxes/postgres-dsn`, readable by `ucloud` only.
- Configuration: `/etc/ucloud-sandboxes/deployment.json`, `relay_postgres`.
- Relay override: `/etc/systemd/system/ucloud-sandbox-relay.service.d/100-postgres-relay.conf`.
- Relay executable: `/work/ucloud-sandboxes/qualification/20260922/venv/bin/ucloud-sandboxes`.
- Backup service/timer: `ucloud-relay-postgres-backup`, every 15 minutes.
- Private persistent backups: `/work/ucloud-sandboxes/backups/live-ucloud-20260824a/postgres`.
- Pre-cutover configuration, SQLite snapshot and import receipt:
  `/work/ucloud-sandboxes/backups/live-ucloud-20260824a/cutover-20260922`.

`fsync`, `full_page_writes`, `synchronous_commit` and data checksums are enabled.
The backup helper is [`scripts/backup_relay_postgres.py`](../../../scripts/backup_relay_postgres.py).
It atomically publishes private custom-format dumps and keeps at most 48 within
10 GiB, always retaining at least two complete snapshots. Check the backup unit
status and latest successful dump time; the timer alone is not proof of success.

This is a single-host database with periodic backups, not automatic failover.
Loss of the gateway disk requires restoring the latest successful dump to a new
PostgreSQL instance; up to one backup interval plus dump runtime can be lost.
Stop relay processes before disaster recovery, preserve the schema/deployment
identity, and verify the import digest against the fenced source. Never resume
SQLite to work around a PostgreSQL outage.

The earlier 256-sandbox performance measurements used a qualification namespace.
This activation does not establish the requested 0.8-second p95 wake target;
the latest prior 256-sandbox result was approximately 1.4 seconds.

## Public endpoint smoke test

[Native Linux smoke result](prod-cutover-smoke.json): four sandboxes, three
model/park/wake/tool cycles each, all 12 cycles correct. After excluding the
first warmup cycle, commit-and-wake p95 was 0.630 seconds and verified tool
execution p95 was 1.256 seconds. The strict end-to-end 0.8-second SLO still
failed (the runner therefore exits 1); this small correctness smoke does not
establish performance at 256-way load. The former isolated PostgreSQL test
container was stopped after the public relay cutover; its data volume remains.
