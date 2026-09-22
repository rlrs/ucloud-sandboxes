"""Explicit qualification-only imports. Not a production ownership API.

These establish a parked inventory and already-leased model requests so the
result/wake slice can be tested independently of create, park and model inference.
Production migration and lease acquisition must implement their own contracts.
"""
from __future__ import annotations

import hashlib

from .model import StateConflict, positive_seconds
from .postgres import PostgresControlStore


async def node(store: PostgresControlStore, node_id: str, *, budget_mb: int = 4096, epoch: str = "boot-1"):
    if not node_id or not epoch or budget_mb <= 0:
        raise ValueError("invalid fixture node")
    async with store.transaction("fixture_node") as conn:
        await conn.execute(
            "INSERT INTO nodes (deployment_id,node_id,node_epoch,restore_budget_mb) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (store.deployment_id, node_id, epoch, budget_mb),
        )
        row = await (await conn.execute(
            "SELECT node_epoch,restore_budget_mb FROM nodes WHERE deployment_id=%s AND node_id=%s",
            (store.deployment_id, node_id),
        )).fetchone()
        if row != {"node_epoch": epoch, "restore_budget_mb": budget_mb}:
            raise StateConflict("fixture node already has another identity or capacity")


async def sandbox(
    store: PostgresControlStore, sandbox_id: str, node_id: str, *,
    generation: int = 1, restore_mb: int = 128, epoch: str = "boot-1",
):
    if not sandbox_id or generation < 1 or restore_mb < 1:
        raise ValueError("invalid fixture sandbox")
    async with store.transaction("fixture_sandbox") as conn:
        await conn.execute(
            "INSERT INTO sandbox_identities VALUES (%s,%s,%s)", (store.deployment_id, sandbox_id, generation),
        )
        await conn.execute(
            """INSERT INTO sandboxes (deployment_id,sandbox_id,generation,create_operation_id,
               spec_hash,node_id,node_epoch,state,restore_mb) VALUES (%s,%s,%s,%s,%s,%s,%s,'parked',%s)""",
            (store.deployment_id, sandbox_id, generation, "create-" + sandbox_id,
             hashlib.sha256(sandbox_id.encode()).hexdigest(), node_id, epoch, restore_mb),
        )


async def request(
    store: PostgresControlStore, request_id: str, sandbox_id: str, *,
    registration_id: str = "registration-1", lease_id: str = "lease-1", lease_seconds: float = 600,
):
    positive_seconds(lease_seconds)
    if not request_id or not registration_id or not lease_id:
        raise ValueError("invalid fixture request")
    async with store.transaction("fixture_request") as conn:
        cursor = await conn.execute(
            """INSERT INTO model_requests (deployment_id,request_id,sandbox_id,generation,registration_id,lease_id,lease_until)
               SELECT deployment_id,%s,sandbox_id,generation,%s,%s,clock_timestamp()+%s*interval '1 second'
               FROM sandboxes WHERE deployment_id=%s AND sandbox_id=%s""",
            (request_id, registration_id, lease_id, lease_seconds, store.deployment_id, sandbox_id),
        )
        if cursor.rowcount != 1:
            raise StateConflict("unknown fixture sandbox")
