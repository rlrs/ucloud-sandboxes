# Hetzner production deployment scripts

See the "Production deployment" section of [docs/hetzner.md](../../docs/hetzner.md).
The scripts read secrets from `.env` and `build/hetzner-prod/hetzner.env`, and
the SSH key from `.hetzner/ssh/gateway-init`. They write generated state
(resource ledger, deployment.json, known hosts, release inputs) to the
git-ignored `build/hetzner-prod/`.

- `hz.py` creates the gateway (on its Primary IP), creates servers, snapshots
  them and deletes them; every resource is recorded in the ledger.
- `make_config.py [snapshot-id]` renders and validates the deployment.
- `upgrade-gateway.sh <version>` brings up or upgrades the gateway.
- `gateway-prep.sh` sets up PostgreSQL, the venv and the schemas; it runs
  once on a fresh gateway.
- `node-init.sh <server-id>` runs VM init from the gateway, for a snapshot
  source or a canary.
- `gw`, `node <ip>` and `gscp` are SSH and scp to the gateway and to private
  nodes through it.
