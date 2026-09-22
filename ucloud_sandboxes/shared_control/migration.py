"""Quiesced, fail-closed relay cutover; never run two relay authorities."""

from __future__ import annotations

import asyncio
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3

from psycopg import sql
from psycopg.types.json import Jsonb

from .. import model_relay as api
from .relay import PostgresRelayState

AUTHORITY_KEY = "postgres_authority"


def assert_relay_cutover(path: Path, deployment: str, schema: str):
    if not path.exists():
        return  # New deployment has no previous relay authority.
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
        row = conn.execute(
            "SELECT value FROM relay_meta WHERE key=?", (AUTHORITY_KEY,)
        ).fetchone()
        if row is None:
            raise ValueError(
                "existing SQLite relay must be explicitly imported before PostgreSQL startup"
            )
        identity = json.loads(row[0])
        if (
            identity.get("deployment_id") != deployment
            or identity.get("schema") != schema
        ):
            raise ValueError("SQLite relay cutover names another PostgreSQL authority")
        return identity


async def import_idle_relay(store, path: Path):
    """Stop relay processes first. Source lock excludes concurrent journal commits.

    PostgreSQL import stays inactive until the source fence is durable. A crash
    between commits fails closed; rerunning verifies the exact source digest and
    completes activation. Never remove the source fence to roll back after traffic.
    """
    if not path.is_absolute() or not path.is_file():
        raise ValueError("existing absolute SQLite relay path required")
    source = sqlite3.connect(
        path.as_uri() + "?mode=rw", uri=True, isolation_level=None, timeout=5
    )
    try:
        source.execute("PRAGMA synchronous=FULL")
        source.execute("BEGIN IMMEDIATE")
        version = source.execute(
            "SELECT value FROM relay_meta WHERE key='version'"
        ).fetchone()
        fenced = source.execute(
            "SELECT value FROM relay_meta WHERE key=?", (AUTHORITY_KEY,)
        ).fetchone()
        valid_version = (
            api.RelaySqliteStore.VERSION + 1 if fenced else api.RelaySqliteStore.VERSION
        )
        if version is None or int(version[0]) != valid_version:
            raise ValueError("unsupported SQLite relay version")
        rollouts = [
            json.loads(r[0])
            for r in source.execute(
                "SELECT payload FROM relay_rollouts ORDER BY rollout_id"
            )
        ]
        requests = [
            json.loads(r[0])
            for r in source.execute(
                "SELECT payload FROM relay_requests ORDER BY request_id"
            )
        ]
        digest = hashlib.sha256(
            json.dumps(
                [rollouts, requests], sort_keys=True, separators=(",", ":")
            ).encode()
        ).hexdigest()
        identity = dict(
            deployment_id=store.deployment_id, schema=store.schema, source_digest=digest
        )
        previous = source.execute(
            "SELECT value FROM relay_meta WHERE key=?", (AUTHORITY_KEY,)
        ).fetchone()
        if previous and json.loads(previous[0]) != identity:
            raise ValueError(
                "SQLite relay is already assigned to another authority or its contents changed"
            )
        decoded = [
            api._request_from_persisted_payload(r, loop=asyncio.get_running_loop())
            for r in requests
        ]
        if any(r.state != "completed" or r.delivery_pending for r in decoded):
            raise ValueError(
                "relay must be idle with no pending deliveries before cutover"
            )
        async with store.transaction("relay_import") as conn:
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (store.schema + ":relay-import:" + store.deployment_id,),
            )
            imported = await (
                await conn.execute(
                    "SELECT * FROM relay_imports WHERE deployment_id=%s FOR UPDATE",
                    (store.deployment_id,),
                )
            ).fetchone()
            if imported and imported["source_digest"] != digest:
                raise ValueError(
                    "source changed since provisional import; refuse ambiguous cutover"
                )
            if previous and imported is None:
                raise ValueError(
                    "fenced source has no matching target import; refuse to resurrect an old snapshot"
                )
            if not imported:
                occupied = await (
                    await conn.execute(
                        "SELECT 1 FROM relay_rollouts WHERE deployment_id=%s LIMIT 1",
                        (store.deployment_id,),
                    )
                ).fetchone()
                if occupied:
                    raise ValueError("target relay deployment is not empty")
                known = set()
                for reg in rollouts:
                    api.validate_rollout_id(reg["rollout_id"])
                    api.validate_registration_token(reg["registration_token"])
                    api._validate_registration_metadata(reg["metadata"])
                    await conn.execute(
                        "INSERT INTO relay_rollouts VALUES (%s,%s,%s,%s,%s,true)",
                        (
                            store.deployment_id,
                            reg["rollout_id"],
                            reg["registration_token"],
                            Jsonb(reg["metadata"]),
                            reg["registered_at"],
                        ),
                    )
                    known.add(reg["rollout_id"])
                total = 0
                for request in decoded:
                    if request.rollout_id not in known:
                        await conn.execute(
                            "INSERT INTO relay_rollouts VALUES (%s,%s,%s,%s,%s,false)",
                            (
                                store.deployment_id,
                                request.rollout_id,
                                request.registration_token,
                                Jsonb({}),
                                request.created_at,
                            ),
                        )
                        known.add(request.rollout_id)
                    values = api._persisted_request_payload(request)
                    for key in ("body", "headers", "completed_response"):
                        values.pop(key)
                    values["deployment_id"] = store.deployment_id
                    values["reserved_bytes"] = request.completed_bytes + 65536
                    # Very old journals can have an absent expiry; completed
                    # retention remains governed by completion, not this field.
                    values["expires_at"] = request.expires_at or request.created_at
                    total += values["reserved_bytes"]
                    await conn.execute(
                        sql.SQL("INSERT INTO relay_requests ({}) VALUES ({})").format(
                            sql.SQL(",").join(map(sql.Identifier, values)),
                            sql.SQL(",").join(sql.Placeholder() for _ in values),
                        ),
                        list(values.values()),
                    )
                    if request.completed_response is None:
                        raise ValueError("completed source request has no response")
                    raw, encoding, response_digest = PostgresRelayState._response_parts(
                        request.completed_response
                    )
                    await conn.execute(
                        "INSERT INTO relay_results VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (
                            store.deployment_id,
                            request.request_id,
                            raw,
                            encoding,
                            request.completed_response.status,
                            Jsonb(request.completed_response.headers),
                            response_digest,
                        ),
                    )
                await conn.execute(
                    "INSERT INTO relay_quota VALUES (%s,%s) ON CONFLICT(deployment_id) DO UPDATE SET reserved_bytes=excluded.reserved_bytes",
                    (store.deployment_id, total),
                )
                await conn.execute(
                    "INSERT INTO relay_imports(deployment_id,source_digest) VALUES (%s,%s)",
                    (store.deployment_id, digest),
                )
        source.execute(
            "INSERT INTO relay_meta VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (AUTHORITY_KEY, json.dumps(identity, sort_keys=True)),
        )
        # Previous releases don't know the authority marker, but reject this
        # unsupported journal version at startup. Quiescing running processes
        # before import is still mandatory; never rely on a startup-only fence.
        source.execute(
            "UPDATE relay_meta SET value=? WHERE key='version'",
            (str(api.RelaySqliteStore.VERSION + 1),),
        )
        source.commit()
        async with store.transaction("relay_activate_import") as conn:
            await conn.execute(
                "UPDATE relay_imports SET active=true WHERE deployment_id=%s AND source_digest=%s",
                (store.deployment_id, digest),
            )
        return {
            "rollouts": len(rollouts),
            "responses": len(decoded),
            "source_digest": digest,
            "active": True,
        }
    finally:
        source.close()
