#!/usr/bin/env python3
"""Rewrite the managed registry's host in gateway state after the gateway moves.

Image references are stored with the registry authority workers pull from
(`<gateway_private_host>:<registry_port>/ucloud-managed/...`): prepared
sources and foundations, image records, the image-roots journal, group specs
and parked sandboxes' storage snapshots. Registry content itself is addressed by
repository and digest, so a restored registry serves the same references under
the new authority once state names it.

Dry run by default: counts per database, table and column. `--execute` rewrites
`<from>/` to `<to>/` (the trailing slash keeps 10.42.0.2 from matching
10.42.0.20) in one transaction per database, and refuses while gateway services
run. Run as the services' user with the gateway venv's Python:

  rehome_registry_host.py --config /etc/ucloud-sandboxes/deployment.json \
      --from 10.42.0.2:5000 --to 10.36.101.16:5000 [--execute]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sqlite3
import subprocess

SERVICES = ("ucloud-sandbox-gateway", "ucloud-sandbox-placement", "ucloud-sandbox-autoscaler",
            "ucloud-sandbox-relay", "ucloud-sandbox-registry-prune")
TEXT_TYPES = {"text", "json", "jsonb", "character varying"}


def sqlite_rewrite(path: Path, old: str, new: str, execute: bool) -> dict[str, int]:
    counts: dict[str, int] = {}
    with sqlite3.connect(path, timeout=30) as db:
        tables = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
        for table in tables:
            for _, column, kind, *_ in db.execute(f'PRAGMA table_info("{table}")').fetchall():
                if kind.upper() not in ("", "TEXT", "JSON", "BLOB") and "CHAR" not in kind.upper():
                    continue
                where = f'typeof("{column}") = \'text\' AND instr("{column}", ?) > 0'
                count = db.execute(f'SELECT count(*) FROM "{table}" WHERE {where}', (old,)).fetchone()[0]
                if count:
                    counts[f"{table}.{column}"] = count
                    if execute:
                        db.execute(f'UPDATE "{table}" SET "{column}" = replace("{column}", ?, ?) WHERE {where}',
                                   (old, new, old))
    return counts


def postgres_rewrite(dsn: str, schemas: list[str], old: str, new: str, execute: bool) -> dict[str, int]:
    import psycopg
    from psycopg import sql
    counts: dict[str, int] = {}
    with psycopg.connect(dsn) as db, db.transaction():
        columns = db.execute(
            "SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ANY(%s) ORDER BY 1, 2, 3", (schemas,)).fetchall()
        for schema, table, column, kind in columns:
            if kind not in TEXT_TYPES:
                continue
            target, field = sql.Identifier(schema, table), sql.Identifier(column)
            where = sql.SQL("strpos({}::text, %s) > 0").format(field)
            count = db.execute(sql.SQL("SELECT count(*) FROM {} WHERE ").format(target) + where, (old,)).fetchone()[0]
            if count:
                counts[f"{schema}.{table}.{column}"] = count
                if execute:
                    cast = sql.SQL("::" + kind.replace("character varying", "text"))
                    db.execute(sql.SQL("UPDATE {} SET {} = replace({}::text, %s, %s)").format(target, field, field)
                               + cast + sql.SQL(" WHERE ") + where, (old, new, old))
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--from", dest="old", required=True, help="old registry authority, host:port")
    parser.add_argument("--to", dest="new", required=True, help="new registry authority, host:port")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    from ucloud_sandboxes.config import DeploymentConfig
    config = DeploymentConfig.from_file(args.config)
    expected = f"{config.registry_endpoint_host}:{config.registry_port}"
    if args.new != expected:
        raise SystemExit(f"--to must be this deployment's registry authority, {expected}")
    if args.old == args.new or "/" in args.old or ":" not in args.old:
        raise SystemExit("--from must be a different host:port")
    old, new = args.old + "/", args.new + "/"
    if args.execute:
        active = [unit for unit in SERVICES
                  if subprocess.run(["systemctl", "is-active", "--quiet", unit]).returncode == 0]
        if active:
            raise SystemExit("stop these services first: " + ", ".join(active))
    report: dict[str, object] = {"from": args.old, "to": args.new, "executed": args.execute}
    root = Path(config.data_root)
    for path in sorted([*root.glob("*.sqlite"), *root.glob("*.sqlite3")]):
        with path.open("rb") as header:
            database = header.read(16).startswith(b"SQLite format 3")
        if not database:
            report[path.name] = "not a database"  # e.g. routes.sqlite after routing moved to PostgreSQL
            continue
        counts = sqlite_rewrite(path, old, new, args.execute)
        if counts:
            report[path.name] = counts
    if config.relay_postgres is not None:
        dsn = Path(config.relay_postgres.dsn_file).read_text(encoding="utf-8").strip()
        import psycopg
        with psycopg.connect(dsn) as db:
            schemas = [row[0] for row in db.execute(
                "SELECT nspname FROM pg_namespace WHERE nspname LIKE 'ucloud\\_%'").fetchall()]
        report["postgres"] = postgres_rewrite(dsn, schemas, old, new, args.execute)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
