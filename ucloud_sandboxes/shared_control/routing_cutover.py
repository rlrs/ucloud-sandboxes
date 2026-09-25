"""Offline, idle-fleet transfer of every routing domain to PostgreSQL.

Stop gateway, relay dispatch and autoscaler first. This command never restarts
services or decides that a live worker is lost. It preserves generation history,
program outcomes, exec routing and storage liveness, not only active routes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from uuid import uuid4

from psycopg import sql

from ..routing import ROUTING_SCHEMA_VERSION
from .routing_repository import PostgresRoutingStore


def _digest(rows):
    digest = hashlib.sha256()
    for row in rows:
        digest.update(
            json.dumps(list(row), ensure_ascii=True, separators=(",", ":")).encode()
        )
        digest.update(b"\n")
    return digest.hexdigest()


def cutover(path: Path, *, dsn_file: Path, schema: str):
    path = path.resolve()
    dsn_file = dsn_file.resolve()
    with path.open("rb") as stream:
        header = stream.read(16)
    if header != b"SQLite format 3\x00":
        raise ValueError("cutover requires an existing SQLite routing database")
    backup = path.with_name(path.name + ".retired-" + uuid4().hex)
    store = PostgresRoutingStore(path, dsn=dsn_file.read_text().strip(), schema=schema)
    descriptor = None
    try:
        store.migrate()
        # A SQLite backup is a complete snapshot including committed WAL pages.
        # The stopped services are a required precondition, not inferred from a
        # heartbeat timeout. Never migrate an active deployment.
        with sqlite3.connect(path) as source, sqlite3.connect(backup) as saved:
            source.backup(saved)
        os.chmod(backup, 0o600)
        with sqlite3.connect(backup) as source:
            if (
                source.execute("PRAGMA user_version").fetchone()[0]
                != ROUTING_SCHEMA_VERSION
            ):
                raise ValueError("unsupported source routing schema")
            if source.execute("SELECT count(*) FROM sandboxes").fetchone()[0]:
                raise ValueError("routing cutover requires an idle fleet")
            if source.execute(
                "SELECT count(*) FROM sandbox_migrations WHERE phase!='complete'"
            ).fetchone()[0]:
                raise ValueError(
                    "routing cutover cannot discard an unfinished migration"
                )
            tables = [
                row[0]
                for row in source.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
            ]
            receipt = {}
            with store.pool.connection() as conn, conn.transaction():
                expected = {
                    r["tablename"]
                    for r in conn.execute(
                        "SELECT tablename FROM pg_tables WHERE schemaname=%s AND tablename!='routing_schema_version'",
                        (schema,),
                    )
                }
                if set(tables) != expected - {"gateway_commands", "worker_capacity_revisions"}:
                    raise ValueError(
                        "routing table set differs; refusing partial import"
                    )
                if conn.execute("SELECT count(*) FROM gateway_commands").fetchone()[0]:
                    raise ValueError("destination command queue is not empty")
                for table in tables:
                    if conn.execute(
                        sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table))
                    ).fetchone()[0]:
                        raise ValueError("destination routing authority is not empty")
                    columns = [
                        r[1]
                        for r in source.execute('PRAGMA table_info("' + table + '")')
                    ]
                    source_rows = source.execute(
                        'SELECT * FROM "' + table + '" ORDER BY "' + columns[0] + '"'
                    )
                    copy = sql.SQL("COPY {} ({}) FROM STDIN").format(
                        sql.Identifier(table),
                        sql.SQL(",").join(map(sql.Identifier, columns)),
                    )
                    source_digest = hashlib.sha256()
                    count = 0
                    with conn.cursor().copy(copy) as writer:
                        for row in source_rows:
                            writer.write_row(row)
                            source_digest.update(
                                json.dumps(
                                    list(row), ensure_ascii=True, separators=(",", ":")
                                ).encode()
                            )
                            source_digest.update(b"\n")
                            count += 1
                    expected_digest = source_digest.hexdigest()
                    with conn.cursor(name="verify_" + table) as cursor:
                        cursor.execute(
                            sql.SQL("SELECT {} FROM {} ORDER BY {}").format(
                                sql.SQL(",").join(map(sql.Identifier, columns)),
                                sql.Identifier(table),
                                sql.Identifier(columns[0]),
                            )
                        )
                        if _digest(row.values() for row in cursor) != expected_digest:
                            raise ValueError("routing import verification failed")
                    receipt[table] = {"rows": count, "sha256": expected_digest}
        # Publishing this descriptor is the only authority switch. Old binaries
        # reject its format; old open SQLite writers detect the inode change.
        descriptor = path.with_name(path.name + ".cutover-" + uuid4().hex)
        descriptor.write_text(
            json.dumps(
                {
                    "format": "ucloud-postgres-routing-v1",
                    "schema": schema,
                    "dsn_file": str(dsn_file),
                },
                sort_keys=True,
            )
            + "\n"
        )
        os.chmod(descriptor, 0o600)
        with descriptor.open("rb") as stream:
            os.fsync(stream.fileno())
        os.replace(descriptor, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return {"backup": str(backup), "schema": schema, "tables": receipt}
    finally:
        store.close()
        if descriptor is not None:
            descriptor.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--routing-file", required=True, type=Path)
    parser.add_argument("--dsn-file", required=True, type=Path)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            cutover(args.routing_file, dsn_file=args.dsn_file, schema=args.schema),
            indent=2,
        )
    )
